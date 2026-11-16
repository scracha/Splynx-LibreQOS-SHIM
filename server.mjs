import http from 'node:http';
import crypto from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';
import { Encoder, decode } from 'cbor-x';

const LISTEN_HOST = process.env.LISTEN_HOST || '127.0.0.1';
const LISTEN_PORT = Number(process.env.LISTEN_PORT || 9999);
const LQOS_WS_URL = process.env.LQOS_WS_URL || 'ws://127.0.0.1:9123/websocket/ws';
const SHIM_API_KEY = (process.env.SHIM_API_KEY || 'change-me-to-a-random-secret').trim();
const TOKEN_TTL_SEC = Number(process.env.TOKEN_TTL_SEC || 300);

const ACK_TEXT = 'I accept that this is an unstable, internal API and is unsupported';
const encoder = new Encoder({ useRecords: false, variableMapSize: true });

// Caches & active state
const ipToCircuitCache = new Map(); // ip -> { circuitId, exp }
const tokens = new Map();           // token -> { ip, circuitId, isTest, exp }
const circuitHubs = new Map();      // circuitId -> CircuitHub

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function isAuthorized(providedKey) {
  const a = Buffer.from(String(providedKey || ''), 'utf8');
  const b = Buffer.from(SHIM_API_KEY, 'utf8');
  if (!b.length || a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function pickExactCircuitId(results, ip) {
  if (!Array.isArray(results)) return null;
  const exactParen = `(${ip})`;
  const boundaryRe = new RegExp(`(?:^|[^0-9.])${escapeRegex(ip)}(?:[^0-9.]|$)`);

  for (const r of results) {
    const dev = r?.Device;
    if (dev?.circuit_id && typeof dev.name === 'string') {
      if (dev.name.includes(exactParen) || boundaryRe.test(dev.name)) {
        return dev.circuit_id;
      }
    }
  }
  for (const r of results) {
    const c = r?.Circuit;
    if (c?.id && typeof c.name === 'string' && boundaryRe.test(c.name)) {
      return c.id;
    }
  }
  return null;
}

// One persistent WS connection dedicated to Search queries (serialized queue)
class SearchClient {
  constructor() {
    this.ws = null;
    this.ready = false;
    this.connecting = null;
    this.queue = Promise.resolve();
  }

  ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN && this.ready) {
      return Promise.resolve();
    }
    if (this.connecting) return this.connecting;

    this.connecting = new Promise((resolve, reject) => {
      const ws = new WebSocket(LQOS_WS_URL);
      ws.binaryType = 'arraybuffer';
      this.ws = ws;
      this.ready = false;

      const timer = setTimeout(() => {
        ws.terminate();
        this.connecting = null;
        reject(new Error(`LibreQoS search WS connect timeout (${LQOS_WS_URL})`));
      }, 6000);

      ws.on('message', (raw) => {
        let msg;
        try {
          msg = decode(new Uint8Array(raw));
        } catch {
          return;
        }
        if (msg?.event === 'Hello') {
          ws.send(encoder.encode({
            HelloReply: { ack: ACK_TEXT, token: '' },
          }));
          this.ready = true;
          clearTimeout(timer);
          this.connecting = null;
          resolve();
        }
      });

      ws.on('close', () => {
        this.ready = false;
        this.ws = null;
      });

      ws.on('error', (err) => {
        clearTimeout(timer);
        this.ready = false;
        this.connecting = null;
        console.error(`[SearchClient] WS error: ${err.message}`);
        reject(err);
      });
    });

    return this.connecting;
  }

  searchIp(ip) {
    const run = async () => {
      await this.ensureConnected();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          this.ws?.off('message', onMsg);
          reject(new Error(`Search timeout for ${ip}`));
        }, 5000);

        const onMsg = (raw) => {
          let msg;
          try {
            msg = decode(new Uint8Array(raw));
          } catch {
            return;
          }
          if (msg?.event === 'SearchResults' && msg.term === ip) {
            clearTimeout(timer);
            this.ws?.off('message', onMsg);
            resolve(pickExactCircuitId(msg.results, ip));
          }
        };

        this.ws.on('message', onMsg);
        this.ws.send(encoder.encode({ Search: { term: ip } }));
      });
    };

    const next = this.queue.then(run, run);
    this.queue = next.catch(() => {});
    return next;
  }

  close() {
    try {
      this.ws?.close();
    } catch {}
  }
}

const searchClient = new SearchClient();

async function resolveIpToCircuit(ip) {
  const cached = ipToCircuitCache.get(ip);
  const now = Date.now();
  if (cached && cached.exp > now) {
    return cached.circuitId;
  }
  const circuitId = await searchClient.searchIp(ip);
  ipToCircuitCache.set(ip, {
    circuitId,
    exp: now + (circuitId ? 300_000 : 15_000),
  });
  return circuitId;
}

// One upstream CircuitWatcher WS per active circuit_id
class CircuitHub {
  constructor(circuitId) {
    this.circuitId = circuitId;
    this.ws = null;
    this.reconnectTimer = null;
    this.idleTimer = null;
    this.closed = false;
    this.wsSubscribers = new Map(); // subKey -> { clientWs, ip, externalCircuitId }
    this.lastHttpViewAt = 0;
    this.meta = {
      circuit_name: circuitId,
      parent_node: '',
      download_max_mbps: 0,
      upload_max_mbps: 0,
    };
    this.latest = null;
    this.history = [];
    this.connect();
  }

  connect() {
    if (this.closed) return;
    const ws = new WebSocket(LQOS_WS_URL);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = decode(new Uint8Array(raw));
      } catch {
        return;
      }

      if (msg?.event === 'Hello') {
        ws.send(encoder.encode({
          HelloReply: { ack: ACK_TEXT, token: '' },
        }));
        ws.send(encoder.encode({ CircuitById: { id: this.circuitId } }));
        ws.send(encoder.encode({ Private: { CircuitWatcher: { circuit: this.circuitId } } }));
        return;
      }

      if (msg?.event === 'CircuitByIdResult' && msg.ok && msg.data?.devices?.length) {
        const d = msg.data.devices[0];
        this.meta = {
          circuit_name: d.circuit_name || this.circuitId,
          parent_node: d.parent_node || '',
          download_max_mbps: Number(d.download_max_mbps || 0),
          upload_max_mbps: Number(d.upload_max_mbps || 0),
        };
        return;
      }

      if (msg?.event === 'CircuitWatcher' && msg.data) {
        const d = msg.data;
        const downBps = Number(d.bytes_per_second?.down || 0) * 8;
        const upBps = Number(d.bytes_per_second?.up || 0) * 8;
        const rttDownMs = d.rtt_current_p50_nanos?.down ? Number(d.rtt_current_p50_nanos.down) / 1e6 : null;
        const rttUpMs = d.rtt_current_p50_nanos?.up ? Number(d.rtt_current_p50_nanos.up) / 1e6 : null;
        const rxmDown = d.tcp_retransmit_sample?.down?.packets
          ? (d.tcp_retransmit_sample.down.retransmits / d.tcp_retransmit_sample.down.packets) * 100
          : 0;
        const rxmUp = d.tcp_retransmit_sample?.up?.packets
          ? (d.tcp_retransmit_sample.up.retransmits / d.tcp_retransmit_sample.up.packets) * 100
          : 0;

        this.latest = {
          ts: Date.now(),
          down_bps: downBps,
          up_bps: upBps,
          rtt_down_ms: rttDownMs,
          rtt_up_ms: rttUpMs,
          rxmit_down_pct: rxmDown,
          rxmit_up_pct: rxmUp,
          qoo_score: d.qoo_score ?? null,
          active_flows: d.active_flow_count ?? 0,
        };
        this.history.push(this.latest);
        if (this.history.length > 180) this.history.shift();

        // Push to all Splynx splynx_node WS subscribers for this circuit
        for (const sub of this.wsSubscribers.values()) {
          if (sub.clientWs.readyState === WebSocket.OPEN) {
            sub.clientWs.send(JSON.stringify({
              type: 'event',
              channel: 'circuit_live',
              circuit_id: sub.externalCircuitId,
              data: {
                devices: [{
                  addresses_str: [[sub.ip, 32]],
                  bits_per_second: [downBps, upBps],
                }],
              },
            }));
          }
        }
      }
    });

    ws.on('close', () => {
      if (!this.closed) {
        this.reconnectTimer = setTimeout(() => this.connect(), 2000);
      }
    });

    ws.on('error', (err) => {
      console.error(`[CircuitHub ${this.circuitId}] WS error: ${err.message}`);
    });
  }

  addWsSubscriber(subKey, clientWs, ip, externalCircuitId) {
    this.wsSubscribers.set(subKey, { clientWs, ip, externalCircuitId });
    this.cancelIdle();
  }

  removeWsSubscriber(subKey) {
    this.wsSubscribers.delete(subKey);
    this.checkIdle();
  }

  touchHttpViewer() {
    this.lastHttpViewAt = Date.now();
    this.cancelIdle();
  }

  cancelIdle() {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
  }

  checkIdle() {
    if (this.wsSubscribers.size > 0) return;
    if (Date.now() - this.lastHttpViewAt < 20_000) return;
    if (this.idleTimer) return;
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.wsSubscribers.size === 0 && Date.now() - this.lastHttpViewAt >= 20_000) {
        this.destroy();
      }
    }, 20_000);
  }

  destroy() {
    this.closed = true;
    this.cancelIdle();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    try {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(encoder.encode({ Private: { StopCircuitWatcher: null } }));
      }
      this.ws?.close();
    } catch {}
    circuitHubs.delete(this.circuitId);
  }
}

function getOrCreateHub(circuitId) {
  let hub = circuitHubs.get(circuitId);
  if (!hub) {
    hub = new CircuitHub(circuitId);
    circuitHubs.set(circuitId, hub);
  }
  return hub;
}

// Periodic cleanup of expired tokens & idle hubs
const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [tok, info] of tokens) {
    if (info.exp <= now) tokens.delete(tok);
  }
  for (const hub of circuitHubs.values()) {
    hub.checkIdle();
  }
}, 15_000);

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 65536) {
        reject(new Error('Body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function renderEmbedHtml(token, info) {
  if (info.isTest) {
    return `<!doctype html><html><body style="font-family:system-ui,sans-serif;padding:24px;color:#334155;">
      <h3>LibreQoS Local Shim Connected</h3><p>Test IP ${escapeHtml(info.ip)} validated OK.</p></body></html>`;
  }
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LibreQoS Live Stats</title>
<style>
  :root { color-scheme: light dark; }
  body { margin:0; padding:16px; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background:#0f172a; color:#e2e8f0; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; margin-bottom:14px; }
  .card { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:12px 14px; }
  .label { font-size:12px; color:#94a3b8; margin-bottom:4px; }
  .val { font-size:20px; font-weight:700; }
  .down { color:#38bdf8; }
  .up { color:#4ade80; }
  .meta { font-size:13px; color:#94a3b8; margin-bottom:12px; display:flex; flex-wrap:wrap; gap:14px; }
  .meta strong { color:#e2e8f0; }
  .chart-box { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:12px; }
  canvas { width:100%; height:320px; display:block; }
</style>
</head>
<body>
  <div class="meta">
    <span>Circuit: <strong id="cName">Loading...</strong></span>
    <span>Tower/Parent: <strong id="cParent">-</strong></span>
    <span>IP: <strong>${escapeHtml(info.ip)}</strong></span>
    <span>Plan: <strong id="cPlan">-</strong></span>
  </div>
  <div class="cards">
    <div class="card"><div class="label">Download</div><div class="val down" id="vDown">-</div></div>
    <div class="card"><div class="label">Upload</div><div class="val up" id="vUp">-</div></div>
    <div class="card"><div class="label">RTT P50 (D / U)</div><div class="val" id="vRtt">-</div></div>
    <div class="card"><div class="label">QoO Score</div><div class="val" id="vQoo">-</div></div>
    <div class="card"><div class="label">Active Flows</div><div class="val" id="vFlows">-</div></div>
  </div>
  <div class="chart-box">
    <canvas id="chart"></canvas>
  </div>
<script>
const TOKEN = ${JSON.stringify(token)};
const canvas = document.getElementById('chart');
const ctx = canvas.getContext('2d');

function fmtBps(bps) {
  if (bps >= 1e9) return (bps / 1e9).toFixed(2) + ' Gbps';
  if (bps >= 1e6) return (bps / 1e6).toFixed(2) + ' Mbps';
  if (bps >= 1e3) return (bps / 1e3).toFixed(1) + ' Kbps';
  return Math.round(bps) + ' bps';
}

function drawChart(history) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
    canvas.width = w * dpr;
    canvas.height = h * dpr;
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const pad = { top: 14, right: 16, bottom: 24, left: 66 };
  const pw = w - pad.left - pad.right;
  const ph = h - pad.top - pad.bottom;

  // Dynamic Y-axis scaling with a 10 Mbps baseline and 20% headroom
  let peak = 10 * 1e6;
  for (const s of history) {
    if (s.down_bps > peak) peak = s.down_bps;
    if (s.up_bps > peak) peak = s.up_bps;
  }
  const yMax = peak * 1.2;

  ctx.strokeStyle = '#334155';
  ctx.fillStyle = '#94a3b8';
  ctx.font = '11px system-ui, sans-serif';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (ph * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
    ctx.fillText(fmtBps(yMax * (1 - i / 4)), 4, y + 4);
  }

  if (history.length < 2) return;
  const n = history.length;

  function plot(key, color, fillRgba) {
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
      const x = pad.left + (pw * i / Math.max(1, n - 1));
      const y = pad.top + ph - (Math.min(history[i][key], yMax) / yMax) * ph;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();

    ctx.lineTo(pad.left + pw, pad.top + ph);
    ctx.lineTo(pad.left, pad.top + ph);
    ctx.closePath();
    ctx.fillStyle = fillRgba;
    ctx.fill();
  }

  plot('down_bps', '#38bdf8', 'rgba(56, 189, 248, 0.12)');
  plot('up_bps', '#4ade80', 'rgba(74, 222, 128, 0.10)');
}

async function poll() {
  try {
    const r = await fetch('/splynx/live-data?token=' + encodeURIComponent(TOKEN), { cache: 'no-store' });
    if (!r.ok) return;
    const d = await r.json();
    document.getElementById('cName').textContent = d.meta?.circuit_name || '-';
    document.getElementById('cParent').textContent = d.meta?.parent_node || '-';
    document.getElementById('cPlan').textContent =
      (d.meta?.download_max_mbps || 0) + ' / ' + (d.meta?.upload_max_mbps || 0) + ' Mbps';
    if (d.latest) {
      document.getElementById('vDown').textContent = fmtBps(d.latest.down_bps);
      document.getElementById('vUp').textContent = fmtBps(d.latest.up_bps);
      const rD = d.latest.rtt_down_ms != null ? d.latest.rtt_down_ms.toFixed(1) + 'ms' : '-';
      const rU = d.latest.rtt_up_ms != null ? d.latest.rtt_up_ms.toFixed(1) + 'ms' : '-';
      document.getElementById('vRtt').textContent = rD + ' / ' + rU;
      document.getElementById('vQoo').textContent = d.latest.qoo_score != null ? Math.round(d.latest.qoo_score) : '-';
      document.getElementById('vFlows').textContent = d.latest.active_flows ?? 0;
    }
    drawChart(d.history || []);
  } catch {}
}
poll();
setInterval(poll, 1000);
</script>
</body>
</html>`;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'POST' && url.pathname === '/splynx/token/v1') {
    try {
      const body = await readJsonBody(req);
      if (!isAuthorized(body.api_key)) {
        console.warn('[token/v1] Rejected request: invalid api_key');
        return sendJson(res, 401, { error: 'Invalid API key' });
      }
      const ip = String(body.ip || '').trim();
      if (!ip) {
        return sendJson(res, 400, { error: 'Missing ip' });
      }

      let circuitId = null;
      const isTest = (ip === '192.0.2.1');
      if (!isTest) {
        circuitId = await resolveIpToCircuit(ip);
        if (!circuitId) {
          return sendJson(res, 404, { error: `No LibreQoS circuit found for IP ${ip}` });
        }
        getOrCreateHub(circuitId).touchHttpViewer();
      }

      const token = crypto.randomBytes(24).toString('hex');
      tokens.set(token, {
        ip,
        circuitId,
        isTest,
        exp: Date.now() + TOKEN_TTL_SEC * 1000,
      });

      return sendJson(res, 200, { token, ttl_seconds: TOKEN_TTL_SEC });
    } catch (err) {
      console.error(`[token/v1] Error: ${err.message}`);
      return sendJson(res, 500, { error: err.message });
    }
  }

  if (req.method === 'GET' && url.pathname === '/splynx/webv1.html') {
    const token = url.searchParams.get('token') || '';
    const info = tokens.get(token);
    if (!info || info.exp <= Date.now()) {
      res.writeHead(403, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      return res.end('Token expired or invalid. Please reload the page.');
    }
    if (info.circuitId) {
      getOrCreateHub(info.circuitId).touchHttpViewer();
    }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    });
    return res.end(renderEmbedHtml(token, info));
  }

  if (req.method === 'GET' && url.pathname === '/splynx/live-data') {
    const token = url.searchParams.get('token') || '';
    const info = tokens.get(token);
    if (!info || info.exp <= Date.now() || !info.circuitId) {
      return sendJson(res, 403, { error: 'Invalid or expired token' });
    }
    const hub = getOrCreateHub(info.circuitId);
    hub.touchHttpViewer();
    return sendJson(res, 200, {
      meta: hub.meta,
      latest: hub.latest,
      history: hub.history,
    });
  }

  res.writeHead(404, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/splynx/ws/v1') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (clientWs) => {
  const clientId = crypto.randomBytes(8).toString('hex');
  const activeSubs = new Map(); // subKey -> { hub, externalCircuitId }

  clientWs.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'hello') {
      clientWs.send(JSON.stringify({ type: 'proceed' }));
      return;
    }

    if (msg.type === 'subscribe' && msg.channel === 'circuit_live') {
      if (!isAuthorized(msg.api_key)) {
        console.warn('[ws/v1] Rejected subscribe: invalid api_key');
        clientWs.send(JSON.stringify({ type: 'error', code: 'UNAUTHORIZED', message: 'Invalid API key' }));
        return;
      }
      const ip = String(msg.ip || '').trim();
      const requestId = msg.request_id;
      if (!ip) return;

      try {
        const circuitId = await resolveIpToCircuit(ip);
        if (!circuitId) {
          clientWs.send(JSON.stringify({
            type: 'error',
            code: 'NOT_FOUND',
            message: `No LibreQoS circuit found for ${ip}`,
          }));
          return;
        }

        const externalCircuitId = `${circuitId}|${ip}`;
        const subKey = `${clientId}:${externalCircuitId}`;
        const hub = getOrCreateHub(circuitId);
        hub.addWsSubscriber(subKey, clientWs, ip, externalCircuitId);
        activeSubs.set(subKey, { hub, externalCircuitId });

        clientWs.send(JSON.stringify({
          type: 'subscribed',
          circuit_id: externalCircuitId,
          request_id: requestId,
        }));
      } catch (err) {
        console.error(`[ws/v1] Subscribe error for ${ip}: ${err.message}`);
        clientWs.send(JSON.stringify({ type: 'error', code: 'INTERNAL', message: err.message }));
      }
      return;
    }

    if (msg.type === 'unsubscribe' && msg.channel === 'circuit_live') {
      for (const [subKey, entry] of activeSubs.entries()) {
        if (
          (msg.circuit_id && entry.externalCircuitId === msg.circuit_id) ||
          (msg.ip && subKey.endsWith(`|${msg.ip}`))
        ) {
          entry.hub.removeWsSubscriber(subKey);
          activeSubs.delete(subKey);
        }
      }
    }
  });

  clientWs.on('close', () => {
    for (const [subKey, entry] of activeSubs.entries()) {
      entry.hub.removeWsSubscriber(subKey);
    }
    activeSubs.clear();
  });
});

function shutdown() {
  clearInterval(cleanupTimer);
  searchClient.close();
  for (const hub of circuitHubs.values()) {
    hub.destroy();
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

if (!SHIM_API_KEY || SHIM_API_KEY === 'change-me-to-a-random-secret') {
  console.warn('[WARN] SHIM_API_KEY is using the placeholder default; set a random secret in systemd.');
}

server.listen(LISTEN_PORT, LISTEN_HOST, () => {
  console.log(`LibreQoS local Splynx shim listening on http://${LISTEN_HOST}:${LISTEN_PORT} -> ${LQOS_WS_URL}`);
});
