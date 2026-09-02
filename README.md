# scrrec

A small, dependency-free Go screen-recording server for Chrome. It authenticates users from a CSV file, requests monitor-only capture, encodes raw Annex-B video with WebCodecs, sends encoded chunks over WebSocket, stores those bytes unchanged, and shows presence on a separate dashboard.

## Run

```sh
cp users.csv.example users.csv
go run . --path ./recordings --users ./users.csv
```

- Recorder: `http://localhost:8080`
- Dashboard: `http://localhost:8081`

Chrome permits screen capture on `localhost`. For access from other machines, put both ports behind HTTPS (the recorder's WebSocket will automatically use `wss://`). The dashboard is intentionally unauthenticated, so restrict it at the reverse proxy or firewall if needed.

## Options

```text
--path string               recording directory (default "recordings")
--users string              username,password CSV (default "users.csv")
--log string                append-only log file (default "log.txt")
--listen string             recorder address (default ":8080")
--dashboard-listen string   dashboard address (default ":8081")
--cert string               TLS certificate chain (for example, Certbot fullchain.pem)
--key string                TLS private key (for example, Certbot privkey.pem)
--fps int                   capture frame rate
--bitrate string            bits/sec; K and M suffixes accepted
```

For direct HTTPS using a Certbot certificate, provide both files:

```sh
go run . \
  --cert /etc/letsencrypt/live/recorder.example.com/fullchain.pem \
  --key /etc/letsencrypt/live/recorder.example.com/privkey.pem \
  --path ./recordings \
  --users ./users.csv
```

Both the recorder and dashboard use TLS when these options are set. They are then available at `https://recorder.example.com:8080` and `https://recorder.example.com:8081` with the default listen addresses. The process must have read permission for both Certbot files. Restart the process after certificate renewal so Go reloads the renewed certificate.

Usernames may contain letters, digits, `.`, `_`, and `-`, must begin with a letter or digit, and are limited to 64 characters. The optional CSV header is `username,password`. Quote fields using normal CSV syntax when a password contains a comma.

Each WebSocket session creates a new `username-yyyymmdd_hhmmss.h265` or `.h264` file. Collisions get `-2`, `-3`, and so on. Reconnects reuse an active screen-share track when Chrome allows it, but create a new file and encoder session.

The browser probes power-efficient HEVC first and then H.264. If neither probe reports power efficiency, it falls back to H.264. VP9 is not used because a concatenated raw VP9 file would lose the WebSocket frame boundaries. H.264 and HEVC are configured as Annex-B, so parameter sets and NAL start codes are present in-band. The first frame of every encoder session is explicitly a keyframe, including after reconnect or encoder recovery, and another keyframe is requested every 10 seconds. These requests do not flush the encoder.

Codec profile levels are derived from the resized output dimensions instead of advertising a fixed maximum level. For example, 1280×704 at 2 FPS uses H.264 Level 3.1. The probe prefers the encoder's native variable-rate controller because some hardware reports constant-rate support but fails at very low requested bitrates.

Chrome's configuration probe is advisory: a driver can accept a configuration and then fail on the first frame. If that happens before any bytes are produced, the client reconnects automatically and tries the remaining hardware profiles, then software-preferred and no-preference configurations. A failed HEVC startup can ultimately fall back to H.264; the reconnect ensures the server creates a file with the correct extension.

On a normal Stop, the browser flushes the encoder and waits for the WebSocket send queue to drain before disconnecting. The raw files contain no container timestamps. For example, play a known 2 FPS recording with `ffplay -framerate 2 -f h264 recording.h264` or `ffplay -framerate 2 -f hevc recording.h265`.

The server sends WebSocket pings every 10 seconds and disconnects clients that have not answered within 30 seconds. The browser reconnects with exponential backoff capped at 10 seconds.

On `SIGINT` or `SIGTERM`, the server closes dashboard event streams and tracked WebSockets, closes recording files, and stops both listeners concurrently.

Server messages are appended to `log.txt` by default and also written to stderr. Use `--log PATH` to choose another file. Log lines include timestamps, authenticated client connects and disconnects, client IP addresses, and the absolute path of each recording when it starts. Passwords are never logged.

## Browser diagnostics

The frontend source is kept in `web/recorder.html`, `web/app.js`, and `web/dashboard.html`; `pages.go` only embeds those files into the Go binary. Open Chrome DevTools and filter the Console for `[scrrec]` to see capture dimensions, API availability, every WebCodecs configuration probe, Media Capabilities results, codec selection, WebSocket lifecycle, encoder queue pressure, and periodic byte/chunk totals. Passwords are never written to the console.
