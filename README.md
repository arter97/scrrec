# scrrec

A small Go screen-recording server for Chrome. It authenticates users from a CSV file, requests monitor-only capture, encodes raw Annex-B video with WebCodecs, sends encoded chunks over WebSocket (or WebTransport over QUIC with `--quic`), stores those bytes unchanged, and shows presence on a separate dashboard. The only third-party dependencies are [quic-go](https://github.com/quic-go/quic-go) and [webtransport-go](https://github.com/quic-go/webtransport-go), which are used only with `--quic`. Building requires Go 1.26.

## Run

```sh
cp users.csv.example users.csv
go run . --path ./recordings --users ./users.csv -p secret
```

- Recorder: `http://localhost:8888`
- Dashboard: `http://localhost:8889`

Chrome permits screen capture on `localhost`. For access from other machines, put both ports behind HTTPS (the recorder's WebSocket will automatically use `wss://`). The dashboard is protected by HTTP basic auth: any username is accepted, and the password is the one given with `-p`.

## Options

```text
--path string               recording directory (default "recordings")
--users string              username,alias,password CSV (default "users.csv")
--log string                append-only log file (default "log.txt")
--listen string             recorder address (default ":8888")
--dashboard-listen string   dashboard address (default ":8889")
-p string                   dashboard basic auth password (required; any username)
--cert string               TLS certificate chain (for example, Certbot fullchain.pem)
--key string                TLS private key (for example, Certbot privkey.pem)
--fps int                   capture frame rate
--bitrate string            bits/sec; K and M suffixes accepted
--quic                      also serve HTTP/3 on UDP and record over WebTransport
```

For direct HTTPS using a Certbot certificate, provide both files:

```sh
go run . -p secret \
  --cert /etc/letsencrypt/live/recorder.example.com/fullchain.pem \
  --key /etc/letsencrypt/live/recorder.example.com/privkey.pem \
  --path ./recordings \
  --users ./users.csv
```

Both the recorder and dashboard use TLS when these options are set. They are then available at `https://recorder.example.com:8888` and `https://recorder.example.com:8889` with the default listen addresses. The process must have read permission for both Certbot files. Restart the process after certificate renewal so Go reloads the renewed certificate.

## QUIC and WebTransport

`--quic` is for networks where long-lived TCP connections stall or get reset. It requires `--cert` and `--key`, and the certificate must chain to a publicly trusted root, such as a Certbot certificate. Chrome refuses QUIC to certificates issued by locally installed CAs, even when it trusts them over TCP.

```sh
go run . -p secret --quic \
  --cert /etc/letsencrypt/live/recorder.example.com/fullchain.pem \
  --key /etc/letsencrypt/live/recorder.example.com/privkey.pem
```

With `--quic`, both listeners also serve HTTP/3 on the UDP port with the same number. Open those UDP ports in the firewall. TCP keeps working as before, and every TCP and HTTP/3 response carries `Alt-Svc: h3=":<port>"`. After a browser's first visit, it loads the pages, `app.js`, the dashboard event stream and thumbnails over QUIC.

To skip TCP even on the first visit, publish HTTPS DNS records. Browsers that use them, and other HTTP/3 clients that connect over UDP directly (for example `curl --http3-only`), never need TCP. For the default ports:

```text
_8888._https.recorder.example.com. 300 IN HTTPS 1 recorder.example.com. alpn="h3,h2"
_8889._https.recorder.example.com. 300 IN HTTPS 1 recorder.example.com. alpn="h3,h2"
```

For a server on port 443, the record goes on the host name itself rather than on an `_443._https` prefix.

The recorder always streams video over WebTransport to `/wt`. It never falls back to WebSocket: if UDP is blocked, it keeps retrying WebTransport with the usual backoff. Browsers without WebTransport get an error instead of a recorder. Each message on the session's single bidirectional stream is framed as a type byte (1 for JSON, 2 for video), a big-endian uint32 length, and the payload. To end a session, the server sends a `{"type":"close","code":…}` message carrying the same code as the WebSocket close, then FIN, and the browser closes the session with that code. If the browser has not closed the session within 5 seconds, the server closes it. The server never closes the session first, because the session close resets its streams and can discard the messages before it, and Chrome then reports only a lost connection. On a normal Stop, the browser sends FIN after the last chunk. The server's close message arrives after it has written everything up to that FIN, which confirms that the recording is complete. QUIC keep-alives run every 10 seconds, and a connection that receives no packets for 30 seconds is closed. The dashboard labels WebTransport clients with `QUIC`.

quic-go may log that it could not raise the UDP receive buffer size. On Linux, raise the limits with `sysctl -w net.core.rmem_max=7500000 net.core.wmem_max=7500000`.

Usernames may contain letters, digits, `.`, `_`, and `-`, must begin with a letter or digit, and are limited to 64 characters. The optional CSV header is `username,alias,password`. The alias is the display name shown on the dashboard. Quote fields using normal CSV syntax when a password contains a comma.

Each WebSocket or WebTransport session creates a new `username-yyyymmdd_hhmmss.h265` or `.h264` file. Collisions get `-2`, `-3`, and so on. Reconnects reuse an active screen-share track when Chrome allows it, but create a new file and encoder session.

Frames are sampled from a hidden `<video>` element playing the capture stream, at the configured FPS, rather than read through `MediaStreamTrackProcessor`, which Safari and Firefox do not expose on the main thread. The sampling clock runs in a dedicated worker because browsers throttle main-thread timers while the recorder tab is in the background. Static screens are therefore still encoded at a steady rate. If the encoder queue backs up, ticks are dropped instead of queued.

The browser probes power-efficient HEVC first and then H.264. If neither probe reports power efficiency, it falls back to H.264. VP9 is not used because a concatenated raw VP9 file would lose the WebSocket frame boundaries. H.264 and HEVC are configured as Annex-B, so parameter sets and NAL start codes are present in-band. The first frame of every encoder session is explicitly a keyframe, including after reconnect or encoder recovery, and another keyframe is requested every 10 seconds of capture time. These requests do not flush the encoder.

Codec profile levels are derived from the resized output dimensions instead of advertising a fixed maximum level. For example, 1280×704 at 2 FPS uses H.264 Level 3.1. The probe prefers the encoder's native variable-rate controller because some hardware reports constant-rate support but fails at very low requested bitrates.

Chrome's configuration probe is advisory: a driver can accept a configuration and then fail on the first frame. If that happens before any bytes are produced, the client reconnects automatically and tries the remaining hardware profiles, then software-preferred and no-preference configurations. A failed HEVC startup can ultimately fall back to H.264; the reconnect ensures the server creates a file with the correct extension.

On a normal Stop, the browser flushes the encoder and waits for the WebSocket send queue to drain before disconnecting. The raw files contain no container timestamps. For example, play a known 2 FPS recording with `ffplay -framerate 2 -f h264 recording.h264` or `ffplay -framerate 2 -f hevc recording.h265`.

The server sends WebSocket pings every 10 seconds and disconnects clients that have not answered within 30 seconds. The browser reconnects with exponential backoff capped at 10 seconds.

On `SIGINT` or `SIGTERM`, the server closes dashboard event streams and tracked WebSockets, closes recording files, and stops both listeners concurrently.

Server messages are appended to `log.txt` by default and also written to stderr. Use `--log PATH` to choose another file. Log lines include timestamps, authenticated client connects and disconnects, client IP addresses, and the absolute path of each recording when it starts. Passwords are never logged.

## Browser diagnostics

The frontend source is kept in `web/recorder.html`, `web/app.js`, and `web/dashboard.html`; `pages.go` only embeds those files into the Go binary. Open Chrome DevTools and filter the Console for `[scrrec]` to see capture dimensions, API availability, every WebCodecs configuration probe, Media Capabilities results, codec selection, WebSocket lifecycle, encoder queue pressure, and periodic byte/chunk totals. Passwords are never written to the console.
