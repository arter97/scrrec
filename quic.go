package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

const (
	quicIdleTimeout = 30 * time.Second
	// http3IdleTimeout closes connections without active requests; QUIC
	// keep-alives alone would otherwise hold them open indefinitely.
	http3IdleTimeout = 2 * time.Minute
	// wtFrameHeaderSize is a one-byte message type and a big-endian uint32 length.
	wtFrameHeaderSize = 5
)

// wtCloseTimeout is how long the browser has to close the session after the
// server's close message before the server closes it.
var wtCloseTimeout = 5 * time.Second

// quicEndpoint is an HTTP/3 server on the UDP port that matches a TCP listener.
type quicEndpoint struct {
	name     string
	conn     net.PacketConn
	h3       *http3.Server
	serve    func() error
	shutdown func(context.Context) error
}

// listenQUIC binds the UDP sockets for the recorder and dashboard addresses.
// The recorder endpoint also accepts WebTransport sessions at /wt.
func (s *server) listenQUIC() (recorder, dashboard *quicEndpoint, err error) {
	cert, err := tls.LoadX509KeyPair(s.cfg.certFile, s.cfg.keyFile)
	if err != nil {
		return nil, nil, err
	}
	tlsConf := http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}})
	recorderConn, err := net.ListenPacket("udp", s.cfg.listen)
	if err != nil {
		return nil, nil, err
	}
	dashboardConn, err := net.ListenPacket("udp", s.cfg.dashboardAddr)
	if err != nil {
		recorderConn.Close()
		return nil, nil, err
	}

	recorderH3 := newHTTP3Server(tlsConf)
	s.wt = &webtransport.Server{
		H3: recorderH3,
		// Safari needs the WT_INITIAL_MAX_* settings, which webtransport-go
		// only sends when they are set; without them it can't open a stream
		// (quic-go/webtransport-go#355). A session uses one bidirectional
		// stream, and QUIC flow control already bounds the data in flight.
		Config: &webtransport.Config{
			MaxIncomingStreams:    16,
			MaxIncomingUniStreams: -1,
			MaxIncomingData:       math.MaxInt64,
		},
	}
	recorder = &quicEndpoint{
		name:  "recorder",
		conn:  recorderConn,
		h3:    recorderH3,
		serve: func() error { return s.wt.Serve(recorderConn) },
		shutdown: func(ctx context.Context) error {
			// The recorder's sessions were already closed by beginShutdown.
			done := make(chan error, 1)
			go func() { done <- s.wt.Close() }()
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	dashboardH3 := newHTTP3Server(tlsConf)
	dashboard = &quicEndpoint{
		name:     "dashboard",
		conn:     dashboardConn,
		h3:       dashboardH3,
		serve:    func() error { return dashboardH3.Serve(dashboardConn) },
		shutdown: dashboardH3.Shutdown,
	}
	return recorder, dashboard, nil
}

func newHTTP3Server(tlsConf *tls.Config) *http3.Server {
	return &http3.Server{
		TLSConfig:   tlsConf,
		IdleTimeout: http3IdleTimeout,
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  quicIdleTimeout,
			KeepAlivePeriod: pingInterval,
		},
	}
}

// advertise makes next the endpoint's HTTP/3 handler and returns a handler
// that announces the endpoint with Alt-Svc, for use on the TCP listener.
// Responses over HTTP/3 carry the header too, so browsers that arrived
// through an HTTPS DNS record or an earlier Alt-Svc keep the entry fresh.
func (e *quicEndpoint) advertise(next http.Handler) http.Handler {
	altSvc := fmt.Sprintf(`h3=":%d"; ma=86400`, e.conn.LocalAddr().(*net.UDPAddr).Port)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", altSvc)
		next.ServeHTTP(w, r)
	})
	e.h3.Handler = h
	return h
}

func (s *server) serveWebTransport(w http.ResponseWriter, r *http.Request) {
	sess, err := s.wt.Upgrade(w, r)
	if err != nil {
		log.Printf("webtransport upgrade from %s: %v", remoteIP(r.RemoteAddr), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(sess.Context(), 60*time.Second)
	stream, err := sess.AcceptStream(ctx)
	cancel()
	if err != nil {
		_ = sess.CloseWithError(4000, "authentication required")
		return
	}
	s.serveSession(&wtConn{sess: sess, stream: stream, reader: bufio.NewReader(stream)}, "webtransport", r.RemoteAddr)
}

// wtConn carries recorder messages on the one bidirectional stream the
// browser opens. Each message is framed as a type byte (opText or opBinary),
// a big-endian uint32 payload length, and the payload.
//
// The server never closes the session first unless the browser fails to:
// webtransport-go resets the session's streams when it closes one, which can
// discard messages still in flight, and Chrome then reports a lost connection
// without the close code. Instead the server sends a close message carrying
// the WebSocket close code, then FIN, and the browser closes the session.
type wtConn struct {
	sess      *webtransport.Session
	stream    *webtransport.Stream
	reader    *bufio.Reader
	writeMu   sync.Mutex
	onFrame   func()
	closeOnce sync.Once

	deadlineMu sync.Mutex
	closing    bool // set by writeClose; reads then fail with net.ErrClosed
}

func (c *wtConn) readMessage() (byte, []byte, error) {
	op, payload, err := c.readFrame()
	if err != nil && c.isClosing() {
		err = net.ErrClosed
	}
	return op, payload, err
}

func (c *wtConn) readFrame() (byte, []byte, error) {
	var head [wtFrameHeaderSize]byte
	if _, err := io.ReadFull(c.reader, head[:]); err != nil {
		return 0, nil, err
	}
	op, n := head[0], binary.BigEndian.Uint32(head[1:])
	if op != opText && op != opBinary {
		return 0, nil, errors.New("invalid webtransport frame")
	}
	if n > maxMessageSize {
		return 0, nil, errors.New("webtransport message too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	if c.onFrame != nil {
		c.onFrame()
	}
	return op, payload, nil
}

func (c *wtConn) writeJSON(v any) error { return c.writeText(v, 2*time.Minute) }

func (c *wtConn) writeText(v any, timeout time.Duration) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	frame := make([]byte, wtFrameHeaderSize, wtFrameHeaderSize+len(b))
	frame[0] = opText
	binary.BigEndian.PutUint32(frame[1:], uint32(len(b)))
	frame = append(frame, b...)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.stream.SetWriteDeadline(time.Now().Add(timeout))
	_, err = c.stream.Write(frame)
	return err
}

// writeClose asks the browser to close the session with code. It does not
// wait for the browser; the session is closed by the server after
// wtCloseTimeout if the browser has not closed it by then.
func (c *wtConn) writeClose(code uint16, reason string) {
	c.closeOnce.Do(func() {
		// Stop reading without telling the browser: STOP_SENDING could make
		// Chrome fail the session before it reads the close message.
		c.deadlineMu.Lock()
		c.closing = true
		_ = c.stream.SetReadDeadline(time.Now())
		c.deadlineMu.Unlock()
		_ = c.writeText(map[string]any{"type": "close", "code": code, "reason": reason}, wtCloseTimeout)
		_ = c.stream.Close()
		t := time.NewTimer(wtCloseTimeout)
		go func() {
			defer t.Stop()
			select {
			case <-c.sess.Context().Done():
			case <-t.C:
			}
			_ = c.sess.CloseWithError(webtransport.SessionErrorCode(code), reason)
		}()
	})
}

// close is a no-op if writeClose was already called. After a normal end of
// stream, its close message tells the browser the recording is complete.
func (c *wtConn) close() { c.writeClose(1000, "") }
func (c *wtConn) setReadDeadline(t time.Time) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if !c.closing {
		_ = c.stream.SetReadDeadline(t)
	}
}

func (c *wtConn) isClosing() bool {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	return c.closing
}

func (c *wtConn) setOnFrame(f func()) { c.onFrame = f }

// keepalive has nothing to do: QUIC keep-alives and the idle timeout detect
// dead peers.
func (c *wtConn) keepalive(<-chan struct{}) {}

// isWebTransportClose reports whether err is a session or stream closed by
// either side, or a connection closed by the browser.
func isWebTransportClose(err error) bool {
	if _, ok := errors.AsType[*webtransport.SessionError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*webtransport.StreamError](err); ok {
		return true
	}
	appErr, ok := errors.AsType[*quic.ApplicationError](err)
	return ok && appErr.Remote
}
