package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxMessageSize = 32 << 20
	pingInterval   = 10 * time.Second
	clientTimeout  = 3 * time.Minute
)

var validCodecString = regexp.MustCompile(`^(avc1|avc3|hvc1|hev1)\.[A-Za-z0-9.]{1,48}$`)

var validUsername = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

type config struct {
	path          string
	usersFile     string
	logFile       string
	listen        string
	dashboardAddr string
	dashboardPass string
	certFile      string
	keyFile       string
	fps           int
	bitrate       int
}

type server struct {
	cfg      config
	users    map[string]string
	hub      *presenceHub
	seq      atomic.Uint64
	stopCh   chan struct{}
	wsMu     sync.Mutex
	sockets  map[*webSocket]struct{}
	active   map[string]*webSocket // latest authenticated socket per user
	wsWG     sync.WaitGroup
	stopping bool
}

type clientInfo struct {
	ID        uint64     `json:"id"`
	Username  string     `json:"username"`
	Filename  string     `json:"filename,omitempty"`
	Codec     string     `json:"codec,omitempty"`
	Thumb     uint64     `json:"thumb,omitempty"`
	ThumbAt   *time.Time `json:"thumbAt,omitempty"`
	Rate      float64    `json:"rate"` // bytes per second over rateWindow
	Connected time.Time  `json:"connected"`
	LastSeen  time.Time  `json:"lastSeen"`
}

type dashboardState struct {
	Offline   []string     `json:"offline"`
	Connected []clientInfo `json:"connected"`
}

type presenceHub struct {
	mu      sync.RWMutex
	users   []string
	clients map[uint64]clientInfo
	thumbs  map[uint64][]byte
	traffic map[uint64]*traffic
	watch   map[chan struct{}]struct{}
}

func newPresenceHub(users map[string]string) *presenceHub {
	names := make([]string, 0, len(users))
	for name := range users {
		names = append(names, name)
	}
	sortStrings(names)
	return &presenceHub{users: names, clients: make(map[uint64]clientInfo), thumbs: make(map[uint64][]byte), traffic: make(map[uint64]*traffic), watch: make(map[chan struct{}]struct{})}
}

func (h *presenceHub) set(c clientInfo) {
	h.mu.Lock()
	h.clients[c.ID] = c
	h.signalLocked()
	h.mu.Unlock()
}

func (h *presenceHub) remove(id uint64) {
	h.mu.Lock()
	delete(h.clients, id)
	delete(h.thumbs, id)
	delete(h.traffic, id)
	h.signalLocked()
	h.mu.Unlock()
}

func (h *presenceHub) touch(id uint64) {
	h.mu.Lock()
	if c, ok := h.clients[id]; ok {
		c.LastSeen = time.Now()
		h.clients[id] = c
	}
	h.mu.Unlock()
}

// rateWindow is the number of one-second buckets averaged for the rate.
const rateWindow = 3

type traffic struct {
	current int64
	buckets [rateWindow]int64
	next    int
}

func (h *presenceHub) received(id uint64, n int) {
	h.mu.Lock()
	if t := h.traffic[id]; t != nil {
		t.current += int64(n)
	} else if _, ok := h.clients[id]; ok {
		h.traffic[id] = &traffic{current: int64(n)}
	}
	h.mu.Unlock()
}

// sampleRates closes the current one-second bucket of every client and
// notifies watchers if any displayed rate changed.
func (h *presenceHub) sampleRates() {
	h.mu.Lock()
	defer h.mu.Unlock()
	changed := false
	for id, c := range h.clients {
		t := h.traffic[id]
		if t == nil {
			t = &traffic{}
			h.traffic[id] = t
		}
		t.buckets[t.next] = t.current
		t.next = (t.next + 1) % rateWindow
		t.current = 0
		var sum int64
		for _, b := range t.buckets {
			sum += b
		}
		rate := float64(sum) / rateWindow
		if rate != c.Rate {
			c.Rate = rate
			h.clients[id] = c
			changed = true
		}
	}
	if changed {
		h.signalLocked()
	}
}

// keyframe stores the latest keyframe of a client as its thumbnail source.
func (h *presenceHub) keyframe(id uint64, data []byte) {
	h.mu.Lock()
	if c, ok := h.clients[id]; ok {
		c.Thumb++
		now := time.Now()
		c.ThumbAt = &now
		h.clients[id] = c
		h.thumbs[id] = data
		h.signalLocked()
	}
	h.mu.Unlock()
}

func (h *presenceHub) thumbnail(id uint64) ([]byte, string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	data, ok := h.thumbs[id]
	return data, h.clients[id].Codec, ok
}

func (h *presenceHub) filename(id uint64, filename, codec string) {
	h.mu.Lock()
	if c, ok := h.clients[id]; ok {
		c.Filename = filename
		c.Codec = codec
		c.LastSeen = time.Now()
		h.clients[id] = c
		h.signalLocked()
	}
	h.mu.Unlock()
}

func (h *presenceHub) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	h.watch[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *presenceHub) unsubscribe(ch chan struct{}) { h.mu.Lock(); delete(h.watch, ch); h.mu.Unlock() }

func (h *presenceHub) signalLocked() {
	for ch := range h.watch {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (h *presenceHub) snapshot() dashboardState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	online := make(map[string]bool)
	connected := make([]clientInfo, 0, len(h.clients))
	for _, c := range h.clients {
		online[c.Username] = true
		connected = append(connected, c)
	}
	sortClients(connected)
	offline := make([]string, 0)
	for _, name := range h.users {
		if !online[name] {
			offline = append(offline, name)
		}
	}
	return dashboardState{Offline: offline, Connected: connected}
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func sortClients(v []clientInfo) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && (v[j].Username < v[j-1].Username || (v[j].Username == v[j-1].Username && v[j].Connected.Before(v[j-1].Connected))); j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func main() {
	var cfg config
	var bitrate string
	flag.StringVar(&cfg.path, "path", "recordings", "directory in which recordings are stored")
	flag.StringVar(&cfg.usersFile, "users", "users.csv", "username,password CSV file")
	flag.StringVar(&cfg.logFile, "log", "log.txt", "log file (opened in append mode)")
	flag.StringVar(&cfg.listen, "listen", ":8888", "recorder listen address")
	flag.StringVar(&cfg.dashboardAddr, "dashboard-listen", ":8889", "dashboard listen address")
	flag.StringVar(&cfg.dashboardPass, "p", "", "dashboard HTTP basic auth password (any username, required)")
	flag.StringVar(&cfg.certFile, "cert", "", "TLS certificate chain file (for example, Certbot fullchain.pem)")
	flag.StringVar(&cfg.keyFile, "key", "", "TLS private key file (for example, Certbot privkey.pem)")
	flag.IntVar(&cfg.fps, "fps", 2, "capture frames per second")
	flag.StringVar(&bitrate, "bitrate", "256K", "video bitrate in bits/sec (supports K and M suffixes)")
	flag.Parse()

	logFile, err := os.OpenFile(cfg.logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		log.Fatalf("open log file %q: %v", cfg.logFile, err)
	}
	defer logFile.Close()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(io.MultiWriter(os.Stderr, logFile))

	cfg.bitrate, err = parseBitrate(bitrate)
	if err != nil || cfg.fps < 1 {
		log.Fatalf("invalid settings: fps must be positive and bitrate must be a positive number with optional K/M suffix")
	}
	if (cfg.certFile == "") != (cfg.keyFile == "") {
		log.Fatal("invalid TLS settings: --cert and --key must be provided together")
	}
	if cfg.dashboardPass == "" {
		log.Fatal("invalid settings: -p dashboard password is required")
	}
	users, err := loadUsers(cfg.usersFile)
	if err != nil {
		log.Fatalf("load users: %v", err)
	}
	if err := os.MkdirAll(cfg.path, 0750); err != nil {
		log.Fatalf("create recording path: %v", err)
	}

	s := &server{
		cfg:     cfg,
		users:   users,
		hub:     newPresenceHub(users),
		stopCh:  make(chan struct{}),
		sockets: make(map[*webSocket]struct{}),
		active:  make(map[string]*webSocket),
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.hub.sampleRates()
			case <-s.stopCh:
				return
			}
		}
	}()
	recorderMux := http.NewServeMux()
	recorderMux.HandleFunc("/", s.serveRecorder)
	recorderMux.HandleFunc("/app.js", s.serveAppJS)
	recorderMux.HandleFunc("/ws", s.serveWebSocket)
	dashboardMux := http.NewServeMux()
	dashboardMux.HandleFunc("/", s.serveDashboard)
	dashboardMux.HandleFunc("/events", s.serveEvents)
	dashboardMux.HandleFunc("/thumb", s.serveThumb)

	recorderHTTP := &http.Server{Addr: cfg.listen, Handler: securityHeaders(recorderMux), ReadHeaderTimeout: 60 * time.Second}
	dashboardHTTP := &http.Server{Addr: cfg.dashboardAddr, Handler: securityHeaders(basicAuth(cfg.dashboardPass, dashboardMux)), ReadHeaderTimeout: 60 * time.Second}
	errCh := make(chan error, 2)
	serve := func(httpServer *http.Server) error {
		if cfg.certFile != "" {
			ln, err := net.Listen("tcp", httpServer.Addr)
			if err != nil {
				return err
			}
			return httpServer.ServeTLS(newRedirectListener(ln), cfg.certFile, cfg.keyFile)
		}
		return httpServer.ListenAndServe()
	}
	scheme := "http"
	if cfg.certFile != "" {
		scheme = "https"
	}
	go func() {
		log.Printf("recorder listening on %s://%s", scheme, cfg.listen)
		errCh <- serve(recorderHTTP)
	}()
	go func() {
		log.Printf("dashboard listening on %s://%s", scheme, cfg.dashboardAddr)
		errCh <- serve(dashboardHTTP)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		log.Printf("shutting down")
		s.beginShutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var shutdownWG sync.WaitGroup
		for _, httpServer := range []*http.Server{recorderHTTP, dashboardHTTP} {
			shutdownWG.Add(1)
			go func(httpServer *http.Server) {
				defer shutdownWG.Done()
				_ = httpServer.Shutdown(ctx)
			}(httpServer)
		}
		shutdownWG.Wait()
		webSocketsDone := make(chan struct{})
		go func() {
			s.wsWG.Wait()
			close(webSocketsDone)
		}()
		select {
		case <-webSocketsDone:
		case <-ctx.Done():
		}
		cancel()
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}

func (s *server) trackWebSocket(ws *webSocket) bool {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.stopping {
		return false
	}
	s.sockets[ws] = struct{}{}
	s.wsWG.Add(1)
	return true
}

// replaceActive makes ws the user's active socket and closes any older one.
func (s *server) replaceActive(username string, ws *webSocket, remote string) {
	s.wsMu.Lock()
	old := s.active[username]
	s.active[username] = ws
	s.wsMu.Unlock()
	if old != nil {
		log.Printf("closing older connection: user=%q replaced by ip=%q", username, remote)
		old.writeClose(4005, "replaced by newer connection")
		old.close()
	}
}

func (s *server) releaseActive(username string, ws *webSocket) {
	s.wsMu.Lock()
	if s.active[username] == ws {
		delete(s.active, username)
	}
	s.wsMu.Unlock()
}

func (s *server) untrackWebSocket(ws *webSocket) {
	s.wsMu.Lock()
	if _, ok := s.sockets[ws]; ok {
		delete(s.sockets, ws)
		s.wsWG.Done()
	}
	s.wsMu.Unlock()
}

func (s *server) beginShutdown() {
	s.wsMu.Lock()
	s.stopping = true
	close(s.stopCh)
	sockets := make([]*webSocket, 0, len(s.sockets))
	for ws := range s.sockets {
		sockets = append(sockets, ws)
	}
	s.wsMu.Unlock()
	for _, ws := range sockets {
		ws.writeClose(1001, "server shutting down")
		ws.close()
	}
}

func parseBitrate(s string) (int, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	if strings.HasSuffix(s, "K") {
		mult = 1000
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "M") {
		mult = 1000000
		s = strings.TrimSuffix(s, "M")
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil || n <= 0 || n*mult > int64(^uint(0)>>1) {
		return 0, errors.New("invalid bitrate")
	}
	return int(n * mult), nil
}

func loadUsers(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.TrimLeadingSpace = true
	users := make(map[string]string)
	for line := 1; ; line++ {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if len(rec) != 2 {
			return nil, fmt.Errorf("line %d: expected exactly two columns", line)
		}
		name, password := strings.TrimSpace(rec[0]), rec[1]
		if line == 1 && strings.EqualFold(name, "username") && strings.EqualFold(strings.TrimSpace(password), "password") {
			continue
		}
		if !validUsername.MatchString(name) {
			return nil, fmt.Errorf("line %d: invalid username %q", line, name)
		}
		if password == "" {
			return nil, fmt.Errorf("line %d: empty password", line)
		}
		if _, exists := users[name]; exists {
			return nil, fmt.Errorf("line %d: duplicate username %q", line, name)
		}
		users[name] = password
	}
	if len(users) == 0 {
		return nil, errors.New("no users found")
	}
	return users, nil
}

func basicAuth(password string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, got, ok := r.BasicAuth()
		gotSum := sha256.Sum256([]byte(got))
		if !ok || subtle.ConstantTimeCompare(gotSum[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="scrrec dashboard", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *server) serveRecorder(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, recorderHTML)
}

func (s *server) serveAppJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	js := strings.ReplaceAll(appJS, "__FPS__", strconv.Itoa(s.cfg.fps))
	js = strings.ReplaceAll(js, "__BITRATE__", strconv.Itoa(s.cfg.bitrate))
	_, _ = io.WriteString(w, js)
}

func (s *server) authenticate(username, password string) bool {
	want, ok := s.users[username]
	if !ok || len(want) != len(password) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(password)) == 1
}

type command struct{ Type, Username, Password, Codec, CodecString, Format string }

func (s *server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.trackWebSocket(ws) {
		ws.writeClose(1001, "server shutting down")
		ws.close()
		return
	}
	defer s.untrackWebSocket(ws)
	defer ws.close()
	_ = ws.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	op, data, err := ws.readMessage()
	if err != nil || op != opText {
		ws.writeClose(4000, "authentication required")
		return
	}
	var cmd command
	if json.Unmarshal(data, &cmd) != nil || cmd.Type != "auth" || !s.authenticate(cmd.Username, cmd.Password) {
		_ = ws.writeJSON(map[string]any{"type": "auth-error", "message": "Invalid username or password"})
		ws.writeClose(4001, "authentication failed")
		return
	}

	id := s.seq.Add(1)
	now := time.Now()
	remote := remoteIP(r.RemoteAddr)
	s.replaceActive(cmd.Username, ws, remote)
	defer s.releaseActive(cmd.Username, ws)
	s.hub.set(clientInfo{ID: id, Username: cmd.Username, Connected: now, LastSeen: now})
	defer s.hub.remove(id)
	recordingPath := ""
	log.Printf("client connected: user=%q ip=%q", cmd.Username, remote)
	defer func() {
		if recordingPath == "" {
			log.Printf("client disconnected: user=%q ip=%q", cmd.Username, remote)
			return
		}
		log.Printf("client disconnected: user=%q ip=%q path=%q", cmd.Username, remote, recordingPath)
	}()
	ws.onFrame = func() {
		_ = ws.conn.SetReadDeadline(time.Now().Add(clientTimeout))
		s.hub.touch(id)
	}
	if err := ws.writeJSON(map[string]any{"type": "auth-ok", "fps": s.cfg.fps, "bitrate": s.cfg.bitrate}); err != nil {
		return
	}
	_ = ws.conn.SetReadDeadline(time.Now().Add(clientTimeout))

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if ws.writeFrame(opPing, []byte(strconv.FormatInt(time.Now().Unix(), 10))) != nil {
					_ = ws.conn.Close()
					return
				}
			case <-done:
				return
			}
		}
	}()

	var file *os.File
	var codec string
	defer func() {
		if file != nil {
			if err := file.Close(); err != nil {
				log.Printf("close recording for %s: %v", cmd.Username, err)
			}
			if info, err := os.Stat(file.Name()); err == nil && info.Size() == 0 {
				if err := os.Remove(file.Name()); err != nil {
					log.Printf("remove empty recording for %s: %v", cmd.Username, err)
				} else {
					log.Printf("removed empty recording: user=%q path=%q", cmd.Username, recordingPath)
				}
			}
		}
	}()
	for {
		op, data, err = ws.readMessage()
		if err != nil {
			if !isExpectedClose(err) {
				log.Printf("websocket %s: %v", cmd.Username, err)
			}
			return
		}
		switch op {
		case opText:
			var next command
			if json.Unmarshal(data, &next) != nil || next.Type != "start" || file != nil {
				ws.writeClose(4002, "invalid command")
				return
			}
			ext, ok := codecExtension(next.Codec, next.Format)
			if !ok {
				ws.writeClose(4003, "unsupported codec")
				return
			}
			file, err = createRecording(s.cfg.path, cmd.Username, ext)
			if err != nil {
				log.Printf("create recording for %s: %v", cmd.Username, err)
				ws.writeClose(4500, "cannot create recording")
				return
			}
			codec = strings.ToLower(next.Codec)
			if !validCodecString.MatchString(next.CodecString) {
				next.CodecString = ""
			}
			recordingPath = file.Name()
			if absolutePath, absoluteErr := filepath.Abs(recordingPath); absoluteErr == nil {
				recordingPath = absolutePath
			}
			log.Printf("recording started: user=%q ip=%q path=%q", cmd.Username, remote, recordingPath)
			name := filepath.Base(file.Name())
			s.hub.filename(id, name, next.CodecString)
			if err := ws.writeJSON(map[string]any{"type": "start-ok", "filename": name}); err != nil {
				return
			}
		case opBinary:
			if file == nil {
				ws.writeClose(4004, "start required")
				return
			}
			if _, err := file.Write(data); err != nil {
				log.Printf("write recording for %s: %v", cmd.Username, err)
				ws.writeClose(4501, "write failed")
				return
			}
			s.hub.received(id, len(data))
			if isKeyframe(codec, data) {
				s.hub.keyframe(id, data)
			}
		}
	}
}

// isKeyframe reports whether an Annex B access unit contains an IDR (H.264)
// or IRAP (HEVC) NAL unit.
func isKeyframe(codec string, data []byte) bool {
	for i := 0; i+3 < len(data); i++ {
		if data[i] != 0 || data[i+1] != 0 || data[i+2] != 1 {
			continue
		}
		header := data[i+3]
		switch codec {
		case "h264":
			if header&0x1f == 5 {
				return true
			}
		case "hevc":
			if t := (header >> 1) & 0x3f; t >= 16 && t <= 21 {
				return true
			}
		}
		i += 2
	}
	return false
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return remoteAddr
}

func codecExtension(codec, format string) (string, bool) {
	if !strings.EqualFold(format, "annexb") {
		return "", false
	}
	switch strings.ToLower(codec) {
	case "hevc":
		return ".h265", true
	case "h264":
		return ".h264", true
	default:
		return "", false
	}
}

func createRecording(dir, username, ext string) (*os.File, error) {
	base := username + "-" + time.Now().Format("20060102_150405")
	for n := 1; ; n++ {
		name := base + ext
		if n > 1 {
			name = fmt.Sprintf("%s-%d%s", base, n, ext)
		}
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
}

func (s *server) serveDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, dashboardHTML)
}

func (s *server) serveThumb(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	data, codec, ok := s.hub.thumbnail(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Codec", codec)
	_, _ = w.Write(data)
}

func (s *server) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	updates := s.hub.subscribe()
	defer s.hub.unsubscribe(updates)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	send := func() bool {
		b, _ := json.Marshal(s.hub.snapshot())
		if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send() {
		return
	}
	for {
		select {
		case <-updates:
			if !send() {
				return
			}
		case <-ticker.C:
			if !send() {
				return
			}
		case <-r.Context().Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

// Minimal RFC 6455 server implementation. Browser-to-server frames must be masked.
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

type webSocket struct {
	conn    net.Conn
	reader  io.Reader
	writeMu sync.Mutex
	onFrame func()
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*webSocket, error) {
	if r.Method != http.MethodGet || !headerToken(r.Header, "Connection", "upgrade") || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, errors.New("websocket upgrade required")
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		return nil, errors.New("invalid websocket key")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("hijacking unsupported")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if _, err = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(h[:])); err != nil {
		conn.Close()
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &webSocket{conn: conn, reader: rw.Reader}, nil
}

func headerToken(h http.Header, name, token string) bool {
	for _, part := range strings.Split(h.Get(name), ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func (w *webSocket) readMessage() (byte, []byte, error) {
	var message []byte
	var messageOp byte
	for {
		var head [2]byte
		if _, err := io.ReadFull(w.reader, head[:]); err != nil {
			return 0, nil, err
		}
		fin, op, masked := head[0]&0x80 != 0, head[0]&0xf, head[1]&0x80 != 0
		if head[0]&0x70 != 0 || !masked {
			return 0, nil, errors.New("invalid websocket frame")
		}
		n := uint64(head[1] & 0x7f)
		if n == 126 {
			var b [2]byte
			if _, err := io.ReadFull(w.reader, b[:]); err != nil {
				return 0, nil, err
			}
			n = uint64(binary.BigEndian.Uint16(b[:]))
		} else if n == 127 {
			var b [8]byte
			if _, err := io.ReadFull(w.reader, b[:]); err != nil {
				return 0, nil, err
			}
			n = binary.BigEndian.Uint64(b[:])
		}
		if n > maxMessageSize || uint64(len(message))+n > maxMessageSize {
			return 0, nil, errors.New("websocket message too large")
		}
		var mask [4]byte
		if _, err := io.ReadFull(w.reader, mask[:]); err != nil {
			return 0, nil, err
		}
		payload := make([]byte, int(n))
		if _, err := io.ReadFull(w.reader, payload); err != nil {
			return 0, nil, err
		}
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
		if w.onFrame != nil {
			w.onFrame()
		}
		if op >= 8 {
			if !fin || n > 125 {
				return 0, nil, errors.New("invalid control frame")
			}
			switch op {
			case opPing:
				if err := w.writeFrame(opPong, payload); err != nil {
					return 0, nil, err
				}
				continue
			case opPong:
				continue
			case opClose:
				_ = w.writeFrame(opClose, payload)
				return 0, nil, io.EOF
			default:
				return 0, nil, errors.New("unknown control frame")
			}
		}
		if op == opContinuation {
			if messageOp == 0 {
				return 0, nil, errors.New("unexpected continuation")
			}
		} else {
			if messageOp != 0 || (op != opText && op != opBinary) {
				return 0, nil, errors.New("invalid data frame")
			}
			messageOp = op
		}
		message = append(message, payload...)
		if fin {
			return messageOp, message, nil
		}
	}
}

func (w *webSocket) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.writeFrame(opText, b)
}
func (w *webSocket) writeClose(code uint16, reason string) {
	b := make([]byte, 2, len(reason)+2)
	binary.BigEndian.PutUint16(b, code)
	b = append(b, reason...)
	_ = w.writeFrame(opClose, b)
}
func (w *webSocket) close() { _ = w.conn.Close() }
func (w *webSocket) writeFrame(op byte, payload []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(2 * time.Minute))
	h := []byte{0x80 | op}
	n := len(payload)
	if n < 126 {
		h = append(h, byte(n))
	} else if n <= 65535 {
		h = append(h, 126, byte(n>>8), byte(n))
	} else {
		h = append(h, 127, 0, 0, 0, 0, byte(uint64(n)>>24), byte(uint64(n)>>16), byte(uint64(n)>>8), byte(n))
	}
	if _, err := w.conn.Write(h); err != nil {
		return err
	}
	_, err := w.conn.Write(payload)
	return err
}

func isExpectedClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset")
}

// redirectListener hands TLS connections to the server and answers plain
// HTTP requests on the same port with a redirect to https.
type redirectListener struct {
	net.Listener
	conns chan net.Conn
	errCh chan error
	done  chan struct{}
	once  sync.Once
}

func newRedirectListener(ln net.Listener) *redirectListener {
	l := &redirectListener{Listener: ln, conns: make(chan net.Conn), errCh: make(chan error, 1), done: make(chan struct{})}
	go l.acceptLoop()
	return l
}

func (l *redirectListener) acceptLoop() {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			l.errCh <- err
			return
		}
		go l.sniff(conn)
	}
}

func (l *redirectListener) sniff(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	br := bufio.NewReader(conn)
	first, err := br.Peek(1)
	if err != nil {
		conn.Close()
		return
	}
	if first[0] == 0x16 {
		_ = conn.SetReadDeadline(time.Time{})
		select {
		case l.conns <- &peekedConn{Conn: conn, r: br}:
		case <-l.done:
			conn.Close()
		}
		return
	}
	defer conn.Close()
	req, err := http.ReadRequest(br)
	if err != nil || req.Host == "" {
		return
	}
	target := "https://" + req.Host + req.URL.RequestURI()
	resp := &http.Response{
		StatusCode: http.StatusPermanentRedirect,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Location": {target}, "Connection": {"close"}},
		Close:      true,
	}
	_ = conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
	_ = resp.Write(conn)
}

func (l *redirectListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case err := <-l.errCh:
		return nil, err
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *redirectListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}

type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) { return c.r.Read(p) }
