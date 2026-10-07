package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

type quicTestServer struct {
	s         *server
	recorder  *quicEndpoint
	dashboard *quicEndpoint
	roots     *x509.CertPool
}

func writeTestCertificate(t *testing.T, dir string) (certFile, keyFile string, roots *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	roots = x509.NewCertPool()
	roots.AddCert(cert)
	return certFile, keyFile, roots
}

func startQUICTestServer(t *testing.T) *quicTestServer {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile, roots := writeTestCertificate(t, dir)
	users := map[string]string{"alice": "secret", "bob": "hunter2"}
	s := &server{
		cfg: config{
			path:          filepath.Join(dir, "recordings"),
			listen:        "127.0.0.1:0",
			dashboardAddr: "127.0.0.1:0",
			dashboardPass: "dash",
			certFile:      certFile,
			keyFile:       keyFile,
			fps:           2,
			bitrate:       256000,
			quic:          true,
		},
		users:   users,
		hub:     newPresenceHub(users),
		stopCh:  make(chan struct{}),
		sockets: make(map[recorderConn]struct{}),
		active:  make(map[string]recorderConn),
	}
	if err := os.MkdirAll(s.cfg.path, 0750); err != nil {
		t.Fatal(err)
	}
	recorder, dashboard, err := s.listenQUIC()
	if err != nil {
		t.Fatal(err)
	}
	recorder.advertise(s.recorderHandler())
	dashboard.advertise(s.dashboardHandler())
	for _, endpoint := range []*quicEndpoint{recorder, dashboard} {
		go func() { _ = endpoint.serve() }()
	}
	ts := &quicTestServer{s: s, recorder: recorder, dashboard: dashboard, roots: roots}
	// Closing a webtransport.Server before Serve has started races inside the
	// library, so wait until both endpoints answer.
	tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer tr.Close()
	for _, endpoint := range []*quicEndpoint{recorder, dashboard} {
		req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("https://localhost:%d/", ts.port(endpoint)), nil)
		rsp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatalf("%s did not answer: %v", endpoint.name, err)
		}
		rsp.Body.Close()
	}
	t.Cleanup(func() {
		s.beginShutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = recorder.shutdown(ctx)
		_ = dashboard.shutdown(ctx)
	})
	return ts
}

func (ts *quicTestServer) port(e *quicEndpoint) int { return e.conn.LocalAddr().(*net.UDPAddr).Port }

// wtTestClient speaks the recorder's WebTransport framing.
type wtTestClient struct {
	t      *testing.T
	sess   *webtransport.Session
	stream *webtransport.Stream
	reader *bufio.Reader
}

func (ts *quicTestServer) dialWebTransport(t *testing.T) *wtTestClient {
	t.Helper()
	d := &webtransport.Transport{
		TLSClientConfig: &tls.Config{RootCAs: ts.roots, NextProtos: []string{http3.NextProtoH3}},
		QUICConfig:      &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
	}
	t.Cleanup(func() { _ = d.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rsp, sess, err := d.Dial(ctx, fmt.Sprintf("https://localhost:%d/wt", ts.port(ts.recorder)), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if rsp.StatusCode != http.StatusOK {
		t.Fatalf("dial status %d", rsp.StatusCode)
	}
	stream, err := sess.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &wtTestClient{t: t, sess: sess, stream: stream, reader: bufio.NewReader(stream)}
}

func (c *wtTestClient) send(op byte, payload []byte) {
	c.t.Helper()
	frame := make([]byte, wtFrameHeaderSize, wtFrameHeaderSize+len(payload))
	frame[0] = op
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))
	if _, err := c.stream.Write(append(frame, payload...)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *wtTestClient) sendJSON(v any) {
	c.t.Helper()
	b, _ := json.Marshal(v)
	c.send(opText, b)
}

func (c *wtTestClient) readJSON() map[string]any {
	c.t.Helper()
	_ = c.stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	var head [wtFrameHeaderSize]byte
	if _, err := io.ReadFull(c.reader, head[:]); err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	if head[0] != opText {
		c.t.Fatalf("frame type %d; want text", head[0])
	}
	payload := make([]byte, binary.BigEndian.Uint32(head[1:]))
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		c.t.Fatalf("read payload: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(payload, &msg); err != nil {
		c.t.Fatal(err)
	}
	return msg
}

// closeCode skips messages up to the server's close message, checks that the
// stream ends after it, and closes the session with its code as the browser
// does.
func (c *wtTestClient) closeCode() uint16 {
	c.t.Helper()
	for {
		msg := c.readJSON()
		if msg["type"] != "close" {
			continue
		}
		code, _ := msg["code"].(float64)
		if _, err := c.reader.ReadByte(); err != io.EOF {
			c.t.Fatalf("stream continued after the close message: %v", err)
		}
		_ = c.sess.CloseWithError(webtransport.SessionErrorCode(code), "")
		return uint16(code)
	}
}

func (c *wtTestClient) authenticate(username, password string) {
	c.t.Helper()
	c.sendJSON(map[string]string{"type": "auth", "username": username, "password": password})
	if msg := c.readJSON(); msg["type"] != "auth-ok" {
		c.t.Fatalf("auth reply %v", msg)
	}
}

func TestWebTransportRecording(t *testing.T) {
	ts := startQUICTestServer(t)
	c := ts.dialWebTransport(t)
	c.authenticate("alice", "secret")
	c.sendJSON(map[string]string{"type": "start", "codec": "h264", "codecString": "avc1.64001f", "format": "annexb"})
	msg := c.readJSON()
	filename, _ := msg["filename"].(string)
	if msg["type"] != "start-ok" || !strings.HasSuffix(filename, ".h264") {
		t.Fatalf("start reply %v", msg)
	}
	clients := ts.s.hub.snapshot().Connected
	if len(clients) != 1 || clients[0].Transport != "webtransport" {
		t.Fatalf("dashboard clients %+v", clients)
	}

	keyframe := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x65, 0x88}
	large := bytes.Repeat([]byte{0, 0, 1, 0x41, 0x9a, 0xff}, 100000)
	var want []byte
	for _, chunk := range [][]byte{keyframe, {0, 0, 0, 1, 0x41, 0x9a}, large} {
		c.send(opBinary, chunk)
		want = append(want, chunk...)
	}
	// FIN ends the recording; the server closes the session after reading it.
	if err := c.stream.Close(); err != nil {
		t.Fatal(err)
	}
	// The close message is sent after the server has written everything.
	if code := c.closeCode(); code != 1000 {
		t.Fatalf("close code %d; want 1000", code)
	}
	got, err := os.ReadFile(filepath.Join(ts.s.cfg.path, filename))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("recording has %d bytes; want %d", len(got), len(want))
	}
}

func TestWebTransportAuthFailure(t *testing.T) {
	ts := startQUICTestServer(t)
	c := ts.dialWebTransport(t)
	c.sendJSON(map[string]string{"type": "auth", "username": "alice", "password": "wrong"})
	if msg := c.readJSON(); msg["type"] != "auth-error" {
		t.Fatalf("auth reply %v", msg)
	}
	if code := c.closeCode(); code != 4001 {
		t.Fatalf("close code %d; want 4001", code)
	}
}

func TestWebTransportRejectsBadFrames(t *testing.T) {
	ts := startQUICTestServer(t)
	c := ts.dialWebTransport(t)
	c.authenticate("alice", "secret")
	c.send(opBinary, []byte{1, 2, 3})
	if code := c.closeCode(); code != 4004 {
		t.Fatalf("close code %d; want 4004 (start required)", code)
	}

	c = ts.dialWebTransport(t)
	c.authenticate("alice", "secret")
	c.send(9, nil)
	if code := c.closeCode(); code != 1000 {
		t.Fatalf("close code %d; want 1000", code)
	}
	if entries, _ := os.ReadDir(ts.s.cfg.path); len(entries) != 0 {
		t.Fatalf("unexpected recordings: %v", entries)
	}
}

func TestWebTransportReplacesOlderSession(t *testing.T) {
	ts := startQUICTestServer(t)
	first := ts.dialWebTransport(t)
	first.authenticate("bob", "hunter2")
	second := ts.dialWebTransport(t)
	second.authenticate("bob", "hunter2")
	if code := first.closeCode(); code != 4005 {
		t.Fatalf("close code %d; want 4005", code)
	}
	if clients := ts.s.hub.snapshot().Connected; len(clients) != 1 {
		t.Fatalf("dashboard clients %+v", clients)
	}
}

func TestHTTP3Pages(t *testing.T) {
	ts := startQUICTestServer(t)
	tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: ts.roots}}
	defer tr.Close()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	get := func(endpoint *quicEndpoint, path string, auth bool) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("https://localhost:%d%s", ts.port(endpoint), path), nil)
		if auth {
			req.SetBasicAuth("anyone", "dash")
		}
		rsp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if rsp.ProtoMajor != 3 {
			t.Fatalf("GET %s used %s", path, rsp.Proto)
		}
		return rsp
	}

	rsp := get(ts.recorder, "/app.js", false)
	body, _ := io.ReadAll(rsp.Body)
	rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`const TRANSPORT = "webtransport";`)) {
		t.Fatalf("app.js status %d without the WebTransport setting", rsp.StatusCode)
	}
	if want := fmt.Sprintf(`h3=":%d"; ma=86400`, ts.port(ts.recorder)); rsp.Header.Get("Alt-Svc") != want {
		t.Fatalf("Alt-Svc %q; want %q", rsp.Header.Get("Alt-Svc"), want)
	}

	rsp = get(ts.dashboard, "/", false)
	rsp.Body.Close()
	if rsp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated dashboard status %d", rsp.StatusCode)
	}
	rsp = get(ts.dashboard, "/events", true)
	defer rsp.Body.Close()
	line, err := bufio.NewReader(rsp.Body).ReadString('\n')
	if err != nil || line != "event: state\n" {
		t.Fatalf("event stream started with %q, %v", line, err)
	}
}

func TestAltSvcOnTCP(t *testing.T) {
	ts := startQUICTestServer(t)
	h := ts.recorder.advertise(ts.s.recorderHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if want := fmt.Sprintf(`h3=":%d"; ma=86400`, ts.port(ts.recorder)); rec.Header().Get("Alt-Svc") != want {
		t.Fatalf("Alt-Svc %q; want %q", rec.Header().Get("Alt-Svc"), want)
	}
}

func TestWebTransportServerClosesLingeringSession(t *testing.T) {
	old := wtCloseTimeout
	t.Cleanup(func() { wtCloseTimeout = old }) // after the server's cleanup
	wtCloseTimeout = 100 * time.Millisecond
	ts := startQUICTestServer(t)
	c := ts.dialWebTransport(t)
	c.sendJSON(map[string]string{"type": "auth", "username": "alice", "password": "wrong"})
	c.readJSON() // auth-error
	if msg := c.readJSON(); msg["type"] != "close" || msg["code"] != float64(4001) {
		t.Fatalf("close message %v", msg)
	}
	// This client ignores the close message, so the server closes the session.
	select {
	case <-c.sess.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("server did not close the session")
	}
}
