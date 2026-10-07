'use strict';

const FPS = __FPS__;
const BITRATE = __BITRATE__;
const TRANSPORT = __TRANSPORT__;
const TRANSPORT_LABEL = TRANSPORT === 'webtransport' ? 'WebTransport' : 'WebSocket';
const KEYFRAME_INTERVAL_SECONDS = 10;
const FRAME_DURATION_MICROSECONDS = Math.round(1e6 / FPS);
const LOG_PREFIX = '[scrrec]';

const captureVideo = document.querySelector('#capture-video');
const login = document.querySelector('#login');
const stopButton = document.querySelector('#stop');
const statusElement = document.querySelector('#status');
const statusDot = document.querySelector('#dot');
const captureAlert = document.querySelector('#capture-alert');
const captureAlertRetry = document.querySelector('#capture-alert-retry');

let connection = null;
let captureStream = null;
let encoderSession = null;
let credentials = null;
let selectedCodec = null;
let reconnectTimer = null;
let reconnectDelay = 1000;
let recordingTimer = null;
let recordingStartedAt = 0;
let intentionallyStopped = true;
let starting = false;
let stopping = false;

const codecCandidates = [
  {
    name: 'hevc',
    label: 'HEVC',
  },
  {
    name: 'h264',
    label: 'H.264',
  },
];

function log(message, details) {
  if (details === undefined) {
    console.info(LOG_PREFIX, message);
  } else {
    console.info(LOG_PREFIX, message, details);
  }
}

function logWarning(message, details) {
  console.warn(LOG_PREFIX, message, details ?? '');
}

function logError(message, error) {
  console.error(LOG_PREFIX, message, error);
}

function setStatus(text, kind = '') {
  statusElement.textContent = text;
  statusDot.className = `dot ${kind}`;
}

function formatElapsed(milliseconds) {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function updateRecordingTimer() {
  if (!recordingStartedAt || !statusDot.classList.contains('live')) return;
  setStatus(`Recording - ${formatElapsed(Date.now() - recordingStartedAt)}`, 'live');
}

function startRecordingTimer() {
  clearInterval(recordingTimer);
  recordingStartedAt = Date.now();
  updateRecordingTimer();
  recordingTimer = setInterval(updateRecordingTimer, 250);
}

function stopRecordingTimer() {
  clearInterval(recordingTimer);
  recordingTimer = null;
  recordingStartedAt = 0;
}

function showCaptureSelectionError() {
  document.documentElement.classList.add('capture-error');
  if (!captureAlert.open) captureAlert.showModal();
}

function clearCaptureSelectionError() {
  document.documentElement.classList.remove('capture-error');
  if (captureAlert.open) captureAlert.close();
}

captureAlert.addEventListener('cancel', (event) => event.preventDefault());
captureAlertRetry.addEventListener('click', () => {
  captureAlert.close();
  login.requestSubmit();
});

function connectionURL() {
  if (TRANSPORT === 'webtransport') return `https://${location.host}/wt`;
  const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${protocol}://${location.host}/ws`;
}

function sendCommand(command) {
  if (!connection?.isOpen()) {
    logWarning(`Skipped command because ${TRANSPORT_LABEL} is not open`, command.type);
    return;
  }
  // Never log the auth object because it contains the plaintext password.
  log(`Sending command: ${command.type}`);
  connection.sendText(JSON.stringify(command));
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

// Both transports share one interface: isOpen(), send() for encoded bytes,
// sendText(), bufferedAmount(), close(code, reason), finish() for a graceful
// stop, and onopen/onmessage/onclose callbacks. onclose receives
// { code, reason, clean } exactly once.
function openWebSocket(url) {
  const socket = new WebSocket(url);
  socket.binaryType = 'arraybuffer';
  const conn = {
    onopen: null,
    onmessage: null,
    onclose: null,
    isOpen: () => socket.readyState === WebSocket.OPEN,
    send: (bytes) => socket.send(bytes),
    sendText: (text) => socket.send(text),
    bufferedAmount: () => socket.bufferedAmount,
    close: (code, reason) => socket.close(code, reason),
    async finish(timeoutMilliseconds) {
      const drainDeadline = Date.now() + timeoutMilliseconds;
      while (socket.bufferedAmount > 0 && Date.now() < drainDeadline) {
        await delay(50);
      }
      log('Closing WebSocket after encoder flush', { bufferedBytes: socket.bufferedAmount });
      socket.close(1000, 'stopped');
    },
  };
  socket.onopen = () => conn.onopen?.();
  socket.onmessage = (event) => {
    if (typeof event.data === 'string') conn.onmessage?.(event.data);
  };
  socket.onclose = (event) => conn.onclose?.({
    code: event.code,
    reason: event.reason,
    clean: event.wasClean,
  });
  socket.onerror = (event) => logError('WebSocket error', event);
  return conn;
}

// WebTransport messages travel on one bidirectional stream, each framed as a
// type byte, a big-endian uint32 payload length, and the payload. To end a
// session, the server sends { type: 'close', code, reason } with a WebSocket
// close code and then FIN; the browser closes the session with that code.
// (A session closed by the server is reported by Chrome as a lost connection,
// without its code.) After a graceful stop, the close message confirms that
// the server has stored everything up to the browser's FIN.
const WT_FRAME_TEXT = 1;
const WT_FRAME_BINARY = 2;
const WT_FRAME_HEADER_BYTES = 5;
const WT_CONNECT_TIMEOUT_MILLISECONDS = 15000;

function openWebTransport(url) {
  let transport = null;
  let writer = null;
  let open = false;
  let closed = false;
  let pendingBytes = 0;
  let connectTimer = null;
  let serverClosed = false;
  let resolveClosed;
  const closedPromise = new Promise((resolve) => { resolveClosed = resolve; });

  const conn = {
    onopen: null,
    onmessage: null,
    onclose: null,
    isOpen: () => open,
    send: (bytes) => write(WT_FRAME_BINARY, bytes),
    sendText: (text) => write(WT_FRAME_TEXT, new TextEncoder().encode(text)),
    // Bytes handed to the stream whose writes have not completed yet.
    bufferedAmount: () => pendingBytes,
    close(code, reason) {
      try {
        transport?.close({ closeCode: code, reason });
      } catch (error) {
        logWarning('WebTransport close failed', error.message || String(error));
      }
      finishClose(code, reason, true);
    },
    async finish(timeoutMilliseconds) {
      if (!open) return;
      open = false;
      log('Closing WebTransport stream after encoder flush', { bufferedBytes: pendingBytes });
      // Closing the writer waits for queued writes and then sends FIN.
      writer.close().catch((error) => logWarning('WebTransport stream close failed', error));
      await Promise.race([closedPromise, delay(timeoutMilliseconds)]);
      if (!serverClosed) {
        logWarning('Server did not confirm the end of the recording; closing anyway', {
          bufferedBytes: pendingBytes,
        });
        conn.close(1000, 'stopped');
      }
    },
  };

  function finishClose(code, reason, clean) {
    if (closed) return;
    closed = true;
    open = false;
    resolveClosed();
    clearTimeout(connectTimer);
    // Callers may close from inside a callback; report it asynchronously, as
    // WebSocket does.
    setTimeout(() => conn.onclose?.({ code, reason, clean }));
  }

  function write(type, payload) {
    if (!open) return;
    const frame = new Uint8Array(WT_FRAME_HEADER_BYTES + payload.byteLength);
    frame[0] = type;
    new DataView(frame.buffer).setUint32(1, payload.byteLength);
    frame.set(payload, WT_FRAME_HEADER_BYTES);
    pendingBytes += frame.byteLength;
    writer.write(frame)
      .catch(() => {})
      .finally(() => { pendingBytes -= frame.byteLength; });
  }

  async function readMessages(reader) {
    const decoder = new TextDecoder();
    let buffered = new Uint8Array(0);
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          if (!closed) conn.close(1006, 'stream ended without a close message');
          return;
        }
        const merged = new Uint8Array(buffered.byteLength + value.byteLength);
        merged.set(buffered);
        merged.set(value, buffered.byteLength);
        buffered = merged;
        while (buffered.byteLength >= WT_FRAME_HEADER_BYTES) {
          const length = new DataView(buffered.buffer, buffered.byteOffset).getUint32(1);
          if (buffered.byteLength < WT_FRAME_HEADER_BYTES + length) break;
          const type = buffered[0];
          const payload = buffered.subarray(WT_FRAME_HEADER_BYTES, WT_FRAME_HEADER_BYTES + length);
          buffered = buffered.subarray(WT_FRAME_HEADER_BYTES + length);
          if (type !== WT_FRAME_TEXT) continue;
          const text = decoder.decode(payload);
          let message = null;
          try {
            message = JSON.parse(text);
          } catch (error) {
            // Passed on as is; the caller reports malformed messages.
          }
          if (message?.type === 'close') {
            serverClosed = true;
            log('Server closed the WebTransport session', message);
            conn.close(message.code, message.reason || '');
            return;
          }
          conn.onmessage?.(text);
        }
      }
    } catch (error) {
      if (!closed) logWarning('WebTransport stream read failed', error.message || String(error));
    }
  }

  try {
    transport = new WebTransport(url);
  } catch (error) {
    logError('WebTransport could not be created', error);
    finishClose(1006, error.message || String(error), false);
    return conn;
  }
  connectTimer = setTimeout(() => {
    logWarning('WebTransport connection timed out', {
      timeoutMilliseconds: WT_CONNECT_TIMEOUT_MILLISECONDS,
    });
    try {
      transport.close();
    } catch (error) {
      // The session failed on its own; finishClose below still reports it.
    }
    finishClose(1006, 'connection timed out', false);
  }, WT_CONNECT_TIMEOUT_MILLISECONDS);
  transport.closed.then(
    (info) => finishClose(info?.closeCode ?? 0, info?.reason || '', true),
    (error) => {
      if (!closed) logWarning('WebTransport closed with an error', error.message || String(error));
      finishClose(1006, error.message || String(error), false);
    },
  );
  transport.ready.then(async () => {
    clearTimeout(connectTimer);
    const stream = await transport.createBidirectionalStream();
    if (closed) return;
    writer = stream.writable.getWriter();
    readMessages(stream.readable.getReader());
    open = true;
    conn.onopen?.();
  }).catch((error) => {
    if (closed) return;
    logError('WebTransport connection failed', error);
    try {
      transport.close();
    } catch (closeError) {
      // Already closed.
    }
    finishClose(1006, error.message || String(error), false);
  });
  return conn;
}

// Browsers throttle main-thread timers on hidden pages to once per second or
// less, and the recorder tab is usually hidden while the screen is shared.
// Dedicated-worker timers are not throttled, so the frame clock runs there.
const frameClockURL = URL.createObjectURL(new Blob([`
  let timer = null;
  self.onmessage = ({ data }) => {
    clearInterval(timer);
    timer = setInterval(() => self.postMessage(null), data);
  };
`], { type: 'text/javascript' }));

function startFrameClock(intervalMilliseconds, onTick) {
  const worker = new Worker(frameClockURL);
  worker.onmessage = onTick;
  worker.postMessage(intervalMilliseconds);
  return { stop: () => worker.terminate() };
}

function roundToMultiple(value, multiple) {
  return Math.max(multiple, Math.round(value / multiple) * multiple);
}

function encodingDimensions(sourceWidth, sourceHeight) {
  const width = roundToMultiple(1920, 64);
  const aspectHeight = sourceHeight * (width / sourceWidth);
  const height = roundToMultiple(aspectHeight, 64);
  return { width, height };
}

login.addEventListener('submit', async (event) => {
  event.preventDefault();
  credentials = {
    username: document.querySelector('#username').value,
    password: document.querySelector('#password').value,
  };
  localStorage.setItem('scrrec-user', credentials.username);
  intentionallyStopped = false;
  reconnectDelay = 1000;
  login.style.display = 'none';
  stopButton.style.display = 'block';
  log('Start requested', { username: credentials.username, fps: FPS, bitrate: BITRATE });

  try {
    await prepareCapture();
    connect();
  } catch (error) {
    logError('Capture setup failed', error);
    intentionallyStopped = true;
    login.style.display = 'block';
    stopButton.style.display = 'none';
    login.querySelector('button').textContent = 'Choose entire screen again';
    setStatus(error.message || String(error), 'warn');
  }
});

document.querySelector('#username').value = localStorage.getItem('scrrec-user') || '';
stopButton.addEventListener('click', () => stopEverything('Stopped'));

async function stopEverything(message) {
  if (stopping) return;
  stopping = true;
  intentionallyStopped = true;
  clearTimeout(reconnectTimer);
  const activeSocket = connection;
  log('Graceful stop started');

  await stopEncoder(true);

  if (activeSocket?.isOpen()) await activeSocket.finish(30000);

  if (connection === activeSocket) connection = null;
  captureStream?.getTracks().forEach((track) => track.stop());
  captureStream = null;
  captureVideo.srcObject = null;
  selectedCodec = null;
  starting = false;
  login.style.display = 'block';
  login.querySelector('button').textContent = 'Start recording';
  stopButton.style.display = 'none';
  setStatus(message || 'Stopped');
  stopping = false;
  log('Graceful stop complete');
}

function connect() {
  if (intentionallyStopped) return;
  clearTimeout(reconnectTimer);
  setStatus('Connecting…', 'warn');
  const url = connectionURL();
  const socket = TRANSPORT === 'webtransport' ? openWebTransport(url) : openWebSocket(url);
  connection = socket;
  log(`${TRANSPORT_LABEL} connecting`, { url, reconnectDelay });

  socket.onopen = () => {
    if (socket !== connection) return;
    log(`${TRANSPORT_LABEL} open; authenticating`, { username: credentials.username });
    sendCommand({ type: 'auth', ...credentials });
  };

  socket.onmessage = async (data) => {
    if (socket !== connection) return;
    let message;
    try {
      message = JSON.parse(data);
    } catch (error) {
      logWarning('Ignored malformed server message', data);
      return;
    }
    log(`Received command: ${message.type}`, message.filename || '');

    if (message.type === 'auth-error') {
      stopEverything(message.message || 'Authentication failed');
    } else if (message.type === 'auth-ok') {
      reconnectDelay = 1000;
      beginServerRecording(socket);
    } else if (message.type === 'start-ok') {
      const attemptedChoice = selectedCodec;
      try {
        await startEncoder(socket, message.filename);
      } catch (error) {
        starting = false;
        logError('Encoder startup failed', error);
        if (advanceEncoderChoice(attemptedChoice, error)) {
          setStatus('Encoder configuration failed; retrying another one…', 'warn');
          socket.close(4100, 'retrying encoder');
        } else {
          setStatus(`Encoder could not start: ${error.message || String(error)}`, 'warn');
          socket.close(4100, 'encoder failed');
        }
      }
    }
  };

  socket.onclose = async (event) => {
    if (socket !== connection) return;
    logWarning(`${TRANSPORT_LABEL} closed`, event);
    connection = null;
    await stopEncoder(false);
    starting = false;
    if (event.code === 4005) {
      stopEverything('Another session for this user connected; this one was closed');
    } else if (event.code === 4001) {
      // A WebTransport close can discard the auth-error message, so the
      // close code alone also means the credentials were rejected.
      stopEverything('Invalid username or password');
    } else if (!intentionallyStopped) {
      setStatus('Connection lost; reconnecting…', 'warn');
      log('Scheduling reconnect', { delayMilliseconds: reconnectDelay });
      reconnectTimer = setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 10000);
    }
  };
}

async function beginServerRecording(socket) {
  if (intentionallyStopped || starting || socket !== connection) return;
  starting = true;
  try {
    await prepareCapture();
    if (socket === connection && socket.isOpen()) {
      log('Requesting raw recording file', { codec: selectedCodec.name, format: 'annexb' });
      sendCommand({
        type: 'start',
        codec: selectedCodec.name,
        codecString: selectedCodec.config.codec,
        format: 'annexb',
      });
    }
  } catch (error) {
    starting = false;
    logError('Could not begin server recording', error);
    setStatus(error.message || String(error), 'warn');
    if (!intentionallyStopped && socket === connection) {
      setTimeout(() => beginServerRecording(socket), 1000);
    }
  }
}

async function prepareCapture() {
  const existingTrack = captureStream?.getVideoTracks()[0];
  if (existingTrack?.readyState === 'live') {
    if (!selectedCodec) selectedCodec = await chooseCodec(captureStream);
    return;
  }

  captureStream = null;
  selectedCodec = null;
  setStatus('Choose an entire screen', 'warn', 'Window and tab captures are rejected.');
  const candidate = await navigator.mediaDevices.getDisplayMedia({
    video: { displaySurface: 'monitor', frameRate: { ideal: FPS, max: FPS } },
    audio: false,
  });
  const track = candidate.getVideoTracks()[0];
  const settings = track.getSettings();
  log('Display capture selected', settings);

  if (settings.displaySurface !== 'monitor') {
    logWarning('Rejected non-monitor capture', settings.displaySurface);
    candidate.getTracks().forEach((candidateTrack) => candidateTrack.stop());
    showCaptureSelectionError();
    throw new Error('A window or tab was selected. Click below and choose an entire screen.');
  }

  clearCaptureSelectionError();

  await requestCaptureResize(track, settings);

  try {
    selectedCodec = await chooseCodec(candidate);
    await attachCaptureVideo(candidate);
  } catch (error) {
    candidate.getTracks().forEach((candidateTrack) => candidateTrack.stop());
    throw error;
  }
  captureStream = candidate;
  track.addEventListener('ended', () => {
    if (captureStream === candidate) {
      logWarning('Display capture track ended');
      stopEverything('Screen sharing ended');
    }
  }, { once: true });
}

// Frames are read from a playing <video> element rather than a
// MediaStreamTrackProcessor, which Safari and Firefox do not expose on window.
async function attachCaptureVideo(stream) {
  captureVideo.srcObject = stream;
  await captureVideo.play();
  log('Capture video playing', {
    videoWidth: captureVideo.videoWidth,
    videoHeight: captureVideo.videoHeight,
    readyState: captureVideo.readyState,
  });
}

async function requestCaptureResize(track, sourceSettings) {
  const sourceWidth = sourceSettings.width || 1920;
  const sourceHeight = sourceSettings.height || 1080;
  const target = encodingDimensions(sourceWidth, sourceHeight);

  try {
    await track.applyConstraints({
      width: { exact: target.width },
      height: { exact: target.height },
      frameRate: { ideal: FPS, max: FPS },
      resizeMode: 'crop-and-scale',
    });

    const resizedSettings = track.getSettings();
    log('Capture-track resize result', {
      sourceWidth,
      sourceHeight,
      requestedWidth: target.width,
      requestedHeight: target.height,
      actualWidth: resizedSettings.width,
      actualHeight: resizedSettings.height,
    });
  } catch (error) {
    logWarning('Capture track could not provide the exact encoded size; canvas fallback will be used', {
      sourceWidth,
      sourceHeight,
      requestedWidth: target.width,
      requestedHeight: target.height,
      error: error.message || String(error),
    });
  }
}

function makeEncoderConfig(candidate, codec, width, height, hardwareAcceleration) {
  const config = {
    codec,
    width,
    height,
    framerate: FPS,
    bitrate: BITRATE,
    bitrateMode: 'constant',
    latencyMode: 'realtime',
    hardwareAcceleration,
  };
  if (candidate.name === 'hevc') {
    config.hevc = { format: 'annexb' };
  } else {
    config.avc = { format: 'annexb' };
  }
  return config;
}

function h264LevelHex(width, height, framerate) {
  const frameMacroblocks = Math.ceil(width / 16) * Math.ceil(height / 16);
  const macroblocksPerSecond = frameMacroblocks * framerate;
  const levels = [
    { hex: '1e', maxFrame: 1620, maxRate: 40500 },
    { hex: '1f', maxFrame: 3600, maxRate: 108000 },
    { hex: '20', maxFrame: 5120, maxRate: 216000 },
    { hex: '28', maxFrame: 8192, maxRate: 245760 },
    { hex: '2a', maxFrame: 8704, maxRate: 522240 },
    { hex: '32', maxFrame: 22080, maxRate: 589824 },
    { hex: '33', maxFrame: 36864, maxRate: 983040 },
    { hex: '34', maxFrame: 36864, maxRate: 2073600 },
  ];
  return levels.find((level) =>
    frameMacroblocks <= level.maxFrame && macroblocksPerSecond <= level.maxRate)?.hex || '34';
}

function hevcLevelIDC(width, height) {
  const pixels = width * height;
  const levels = [
    { idc: 30, maxPicture: 36864 },
    { idc: 60, maxPicture: 122880 },
    { idc: 63, maxPicture: 245760 },
    { idc: 90, maxPicture: 552960 },
    { idc: 93, maxPicture: 983040 },
    { idc: 120, maxPicture: 2228224 },
    { idc: 150, maxPicture: 8912896 },
    { idc: 153, maxPicture: 8912896 },
  ];
  return levels.find((level) => pixels <= level.maxPicture)?.idc || 153;
}

function codecStrings(candidate, width, height) {
  if (candidate.name === 'hevc') {
    const level = hevcLevelIDC(width, height);
    return [`hvc1.1.6.L${level}.B0`, `hev1.1.6.L${level}.B0`];
  }

  const level = h264LevelHex(width, height, FPS);
  return [`avc1.6400${level}`, `avc1.4d00${level}`, `avc1.4200${level}`];
}

async function findSupportedConfig(candidate, width, height, hardwareAcceleration) {
  const candidates = codecStrings(candidate, width, height);
  log(`Derived ${candidate.label} codec strings`, { width, height, candidates });

  for (const codec of candidates) {
    // Let the encoder use its native rate controller first. Some hardware
    // advertises constant mode but fails when the requested rate is very low.
    for (const constantRate of [false, true]) {
      const config = makeEncoderConfig(candidate, codec, width, height, hardwareAcceleration);
      if (!constantRate) delete config.bitrateMode;
      try {
        const support = await VideoEncoder.isConfigSupported(config);
        log('VideoEncoder configuration probe', {
          candidate: candidate.name,
          requestedConfig: config,
          supported: support.supported,
          returnedConfig: support.config,
        });
        if (support.supported) return config;
      } catch (error) {
        logWarning('VideoEncoder configuration probe threw', {
          candidate: candidate.name,
          config,
          error: error.message || String(error),
        });
      }
    }
  }
  return null;
}

async function probePowerEfficiency(config, width, height) {
  if (!navigator.mediaCapabilities?.encodingInfo) {
    logWarning('Media Capabilities encodingInfo is unavailable');
    return false;
  }
  const mediaConfig = {
    type: 'record',
    video: {
      contentType: `video/mp4;codecs="${config.codec}"`,
      width,
      height,
      bitrate: BITRATE,
      framerate: FPS,
    },
  };
  try {
    const result = await navigator.mediaCapabilities.encodingInfo(mediaConfig);
    log('Media Capabilities result', { mediaConfig, result });
    return result.powerEfficient;
  } catch (error) {
    logWarning('Media Capabilities probe threw', {
      mediaConfig,
      error: error.message || String(error),
    });
    return false;
  }
}

async function chooseCodec(media) {
  const settings = media.getVideoTracks()[0].getSettings();
  const sourceWidth = settings.width || 1920;
  const sourceHeight = settings.height || 1080;
  const { width, height } = encodingDimensions(sourceWidth, sourceHeight);
  let h264Fallback = null;
  log('Beginning codec selection', {
    sourceWidth,
    sourceHeight,
    encodedWidth: width,
    encodedHeight: height,
    widthAlignment: width % 64,
    heightAlignment: height % 64,
    fps: FPS,
    bitrate: BITRATE,
  });

  for (const candidate of codecCandidates) {
    const hardwareConfig = await findSupportedConfig(candidate, width, height, 'prefer-hardware');
    if (candidate.name === 'h264' && !h264Fallback) {
      h264Fallback = hardwareConfig ||
        await findSupportedConfig(candidate, width, height, 'no-preference');
    }
    if (!hardwareConfig) {
      logWarning(`No supported hardware configuration for ${candidate.label}`);
      continue;
    }
    if (await probePowerEfficiency(hardwareConfig, width, height)) {
      log(`Selected power-efficient ${candidate.label}`, hardwareConfig);
      return makeCodecChoice(candidate, hardwareConfig, width, height);
    }
    logWarning(`${candidate.label} was supported but not reported power-efficient`);
  }

  if (!h264Fallback) {
    const h264 = codecCandidates.find((candidate) => candidate.name === 'h264');
    h264Fallback = await findSupportedConfig(h264, width, height, 'no-preference');
  }
  if (!h264Fallback) {
    const error = new Error(
      `No raw Annex-B H.264 configuration was accepted for the resized ` +
      `${width}x${height} output (${sourceWidth}x${sourceHeight} source), ` +
      `${FPS} fps, ${BITRATE} bit/s. See [scrrec] probe logs above.`,
    );
    logError('Codec selection failed', error);
    throw error;
  }
  logWarning('No power-efficient codec found; falling back to H.264', h264Fallback);
  const h264 = codecCandidates.find((candidate) => candidate.name === 'h264');
  return makeCodecChoice(h264, h264Fallback, width, height);
}

async function makeCodecChoice(candidate, config, width, height) {
  const choices = [];
  const candidates = [candidate];
  if (candidate.name === 'hevc') {
    candidates.push(codecCandidates.find((item) => item.name === 'h264'));
  }

  for (const fallbackCandidate of candidates) {
    for (const hardwareAcceleration of ['prefer-hardware', 'prefer-software', 'no-preference']) {
      for (const codec of codecStrings(fallbackCandidate, width, height)) {
        const fallbackConfig = makeEncoderConfig(
          fallbackCandidate,
          codec,
          width,
          height,
          hardwareAcceleration,
        );
        // Native rate control is more broadly operational at very low bitrates.
        delete fallbackConfig.bitrateMode;
        if (fallbackCandidate.name === candidate.name &&
            JSON.stringify(fallbackConfig) === JSON.stringify(config)) continue;
        try {
          const support = await VideoEncoder.isConfigSupported(fallbackConfig);
          if (support.supported) {
            choices.push({ ...fallbackCandidate, config: fallbackConfig });
          }
        } catch (error) {
          logWarning('Operational fallback probe threw', {
            codec: fallbackConfig.codec,
            hardwareAcceleration,
            error: error.message || String(error),
          });
        }
      }
    }
  }

  log('Prepared encoder fallback choices', choices.map((choice) => ({
    codec: choice.config.codec,
    hardwareAcceleration: choice.config.hardwareAcceleration,
  })));
  return { ...candidate, config, alternatives: choices };
}

function advanceEncoderChoice(failedChoice, error) {
  const alternatives = failedChoice?.alternatives || [];
  if (!alternatives.length) return false;
  const [next, ...remaining] = alternatives;
  selectedCodec = { ...next, alternatives: remaining };
  logWarning('Encoder rejected its first frame; retrying with another backend/configuration', {
    error: error?.message || String(error),
    failedCodec: failedChoice.config.codec,
    failedHardwareAcceleration: failedChoice.config.hardwareAcceleration,
    nextCodec: next.config.codec,
    nextHardwareAcceleration: next.config.hardwareAcceleration,
    remainingChoices: remaining.length,
  });
  return true;
}

async function startEncoder(socket, filename) {
  if (socket !== connection || !captureStream || intentionallyStopped) return;
  await stopEncoder(false);
  const session = {
    socket,
    filename,
    codecChoice: selectedCodec,
    encoder: null,
    clock: null,
    stopped: false,
    startedAt: performance.now(),
    lastKeyFrameTimestamp: null,
    framesSubmitted: 0,
    framesDropped: 0,
    waitingLogged: false,
    chunksSent: 0,
    bytesSent: 0,
    resizeCanvas: null,
    resizeContext: null,
    resizeLogged: false,
  };
  session.encoder = new VideoEncoder({
    output: (chunk, metadata) => handleEncodedChunk(session, chunk, metadata),
    error: (error) => {
      if (!session.stopped) {
        session.clock?.stop();
        logError('VideoEncoder reported an error', error);
        if (session.chunksSent === 0 && advanceEncoderChoice(session.codecChoice, error)) {
        setStatus('Encoder backend failed; retrying another configuration…', 'warn');
          socket.close(4101, 'retrying encoder');
        } else {
          setStatus(`Encoding error: ${error.message}`, 'warn');
          socket.close(4101, 'encoder error');
        }
      }
    },
  });
  log('Configuring VideoEncoder', session.codecChoice.config);
  session.encoder.configure(session.codecChoice.config);

  encoderSession = session;
  session.clock = startFrameClock(1000 / FPS, () => encodeCurrentFrame(session));
  encodeCurrentFrame(session);
  starting = false;
  setStatus('Recording', 'live');
  startRecordingTimer();
  log('Raw encoder started', { filename, config: session.codecChoice.config });
}

function handleEncodedChunk(session, chunk, metadata) {
  if (!chunk.byteLength || session.socket !== connection || !session.socket.isOpen()) return;
  const bytes = new Uint8Array(chunk.byteLength);
  chunk.copyTo(bytes);
  session.socket.send(bytes);
  session.chunksSent += 1;
  session.bytesSent += bytes.byteLength;

  if (session.chunksSent === 1 || session.chunksSent % 10 === 0) {
    log('Encoded chunk sent', {
      filename: session.filename,
      chunkNumber: session.chunksSent,
      chunkType: chunk.type,
      chunkBytes: bytes.byteLength,
      totalBytes: session.bytesSent,
      bufferedBytes: session.socket.bufferedAmount(),
      decoderConfigPresent: Boolean(metadata?.decoderConfig),
    });
  }
  if (session.socket.bufferedAmount() >= 4 * 1024 * 1024) {
    logWarning(`${TRANSPORT_LABEL} backlog is growing`, {
      filename: session.filename,
      bufferedBytes: session.socket.bufferedAmount(),
    });
    setStatus('Recording; network backlog is growing', 'warn');
  }
}

function encodeCurrentFrame(session) {
  if (session.stopped) return;
  if (captureVideo.readyState < HTMLMediaElement.HAVE_CURRENT_DATA ||
      !captureVideo.videoWidth || !captureVideo.videoHeight) {
    if (!session.waitingLogged) {
      session.waitingLogged = true;
      logWarning('Capture video has no frame yet; skipping tick', {
        readyState: captureVideo.readyState,
      });
    }
    return;
  }
  if (session.encoder.encodeQueueSize > 2) {
    session.framesDropped += 1;
    if (session.framesDropped === 1 || session.framesDropped % 10 === 0) {
      logWarning('Encoder queue is backed up; dropping frame', {
        queueSize: session.encoder.encodeQueueSize,
        framesSubmitted: session.framesSubmitted,
        framesDropped: session.framesDropped,
      });
    }
    return;
  }

  const timestamp = Math.round((performance.now() - session.startedAt) * 1000);
  let frame = null;
  try {
    frame = captureFrame(session, timestamp);
    const keyFrame = session.lastKeyFrameTimestamp === null ||
      timestamp - session.lastKeyFrameTimestamp >= KEYFRAME_INTERVAL_SECONDS * 1e6;
    session.encoder.encode(frame, { keyFrame });
    session.framesSubmitted += 1;
    if (keyFrame) {
      session.lastKeyFrameTimestamp = timestamp;
      log(session.framesSubmitted === 1
        ? 'Submitted initial keyframe'
        : 'Submitted periodic keyframe', {
        timestamp,
        frameNumber: session.framesSubmitted,
        intervalSeconds: KEYFRAME_INTERVAL_SECONDS,
        sourceWidth: captureVideo.videoWidth,
        sourceHeight: captureVideo.videoHeight,
        encodedCodedWidth: frame.codedWidth,
        encodedCodedHeight: frame.codedHeight,
      });
    }
  } catch (error) {
    if (!session.stopped) {
      session.clock.stop();
      logError('Capture-to-encoder pipeline failed', error);
      setStatus(`Capture pipeline error: ${error.message}`, 'warn');
      session.socket.close(4102, 'capture pipeline error');
    }
  } finally {
    frame?.close();
  }
}

function captureFrame(session, timestamp) {
  const targetWidth = session.codecChoice.config.width;
  const targetHeight = session.codecChoice.config.height;
  const sourceWidth = captureVideo.videoWidth;
  const sourceHeight = captureVideo.videoHeight;
  const frameInit = { timestamp, duration: FRAME_DURATION_MICROSECONDS };

  if (sourceWidth === targetWidth && sourceHeight === targetHeight) {
    return new VideoFrame(captureVideo, frameInit);
  }

  if (!session.resizeCanvas) {
    session.resizeCanvas = window.OffscreenCanvas
      ? new OffscreenCanvas(targetWidth, targetHeight)
      : document.createElement('canvas');
    session.resizeCanvas.width = targetWidth;
    session.resizeCanvas.height = targetHeight;
    session.resizeContext = session.resizeCanvas.getContext('2d', {
      alpha: false,
      desynchronized: true,
    });
    if (!session.resizeContext) {
      throw new Error('Could not create a 2D context for frame resizing.');
    }
  }

  const scale = Math.min(targetWidth / sourceWidth, targetHeight / sourceHeight);
  const drawWidth = Math.round(sourceWidth * scale);
  const drawHeight = Math.round(sourceHeight * scale);
  const drawX = Math.floor((targetWidth - drawWidth) / 2);
  const drawY = Math.floor((targetHeight - drawHeight) / 2);

  session.resizeContext.fillStyle = 'black';
  session.resizeContext.fillRect(0, 0, targetWidth, targetHeight);
  session.resizeContext.drawImage(captureVideo, drawX, drawY, drawWidth, drawHeight);

  if (!session.resizeLogged) {
    session.resizeLogged = true;
    log('Using canvas frame resize fallback', {
      sourceWidth,
      sourceHeight,
      targetWidth,
      targetHeight,
      contentRectangle: { x: drawX, y: drawY, width: drawWidth, height: drawHeight },
    });
  }

  return new VideoFrame(session.resizeCanvas, frameInit);
}

async function stopEncoder(graceful) {
  const session = encoderSession;
  stopRecordingTimer();
  if (!session) return;
  encoderSession = null;
  session.stopped = true;
  log('Stopping encoder', {
    graceful,
    framesSubmitted: session.framesSubmitted,
    chunksSent: session.chunksSent,
    bytesSent: session.bytesSent,
  });
  session.clock?.stop();
  if (graceful && session.encoder.state === 'configured') {
    const flushed = await Promise.race([
      session.encoder.flush().then(() => true),
      delay(3000).then(() => false),
    ]).catch((error) => {
      logWarning('Encoder flush failed', error);
      return false;
    });
    log('Encoder flush finished', { flushed });
  }
  if (session.encoder.state !== 'closed') {
    try {
      session.encoder.close();
    } catch (error) {
      logWarning('Encoder close failed', error);
    }
  }
}

const requiredAPIs = {
  getDisplayMedia: Boolean(navigator.mediaDevices?.getDisplayMedia),
  VideoEncoder: Boolean(window.VideoEncoder),
  VideoFrame: Boolean(window.VideoFrame),
  Worker: Boolean(window.Worker),
  WebTransport: Boolean(window.WebTransport),
  mediaCapabilitiesEncoding: Boolean(navigator.mediaCapabilities?.encodingInfo),
};
log('Client initialized', { fps: FPS, bitrate: BITRATE, transport: TRANSPORT, requiredAPIs });

if (!requiredAPIs.getDisplayMedia || !requiredAPIs.VideoEncoder ||
    !requiredAPIs.VideoFrame || !requiredAPIs.Worker) {
  setStatus('This browser does not support the required screen-capture WebCodecs APIs.', 'warn');
  login.querySelector('button').disabled = true;
  logError('Required browser APIs are missing', requiredAPIs);
} else if (TRANSPORT === 'webtransport' && !requiredAPIs.WebTransport) {
  setStatus('This browser does not support WebTransport, which this server requires.', 'warn');
  login.querySelector('button').disabled = true;
  logError('Required browser APIs are missing', requiredAPIs);
}
