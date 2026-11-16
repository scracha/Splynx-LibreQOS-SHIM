# Splynx LibreQoS Local Shim (v2.x)

A lightweight Node.js API shim that bridges **Splynx 6.x** and **LibreQoS 2.x**, allowing you to view real-time bandwidth graphs in the Splynx Admin UI **without** requiring a paid LibreQoS Insight license.

## The Problem
Splynx's official `splynx-libreqos` addon expects a specific REST/WebSocket API provided by LibreQoS Insight. LibreQoS 2.x includes a CBOR WebSocket API on `lqosd` (`:9123`), but the `lqos_api` (`:9122`) is locked without an Insight key.

## The Solution
This project provides a local Node.js shim that sits behind Nginx. It intercepts the Splynx addon's API calls, translates them into LibreQoS CBOR WebSocket messages, and renders a custom HTML5 Canvas dashboard with dynamic auto-scaling.

### Architecture
1. **Splynx (PHP/JS)** -> Requests a token and loads the iframe from `https://<your-splynx-domain>/splynx/webv1.html`.
2. **Nginx** -> Proxies `/splynx/` to `127.0.0.1:9999` (the Node.js shim).
3. **Node.js Shim** -> Opens a CBOR WebSocket to LibreQoS `lqosd` (`:9123`), polls the live data, and serves a custom HTML5 dashboard.

---

## Features
- **No Insight License Required:** Bypasses the locked `lqos_api` and talks directly to `lqosd`.
- **Dynamic Y-Axis Scaling:** Automatically scales the graph to actual bandwidth usage (with a 10Mbps baseline), rather than the customer's maximum plan speed. This prevents graphs from looking flat for idle connections.
- **Smart Caching Control:** Forces `Cache-Control: no-store` on expired tokens to prevent browsers from caching 403 error pages when the shim restarts.
- **Protocol Translation:** Seamlessly translates Splynx's JSON WebSockets into LibreQoS's CBOR WebSockets.

---

## Installation Guide

### 1. Prerequisites
- Node.js (v18+)
- LibreQoS 2.x running with `lqosd` accessible on `127.0.0.1:9123`
- Splynx 6.1+ with the `splynx-libreqos` addon installed

### 2. Setup the Shim
```bash
sudo mkdir -p /opt/libreqos-shim
cd /opt/libreqos-shim
npm init -y
npm install ws cbor-x
```
Clone this repository or create `/opt/libreqos-shim/server.mjs` using the code provided in this repo.

### 3. Systemd Service
Create `/etc/systemd/system/libreqos-shim.service`:

```ini
[Unit]
Description=LibreQoS Local Shim for Splynx
After=network.target

[Service]
Type=simple
User=root
Environment=SHIM_API_KEY=your_secure_api_key_here
ExecStart=/usr/bin/node /opt/libreqos-shim/server.mjs
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now libreqos-shim
```

### 4. Nginx Configuration
In your Splynx Nginx config (e.g., `/etc/nginx/sites-available/splynx-ssl`), add the following location block:

```nginx
location ^~ /splynx/ {
    proxy_pass http://127.0.0.1:9999; # MUST NOT have a trailing slash
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```
Reload Nginx:
```bash
sudo nginx -t && sudo systemctl reload nginx
```

### 5. Splynx Addon Configuration
1. Go to **Administration -> API keys** in Splynx and create a key.
2. Go to **Config -> Integrations -> Modules list -> Splynx LibreQoS**.
3. Set the **LibreQoS URL** to `https://your-splynx-domain.com` (Do **not** include `/splynx/` or a trailing slash).
4. Input the API Key you generated.
5. **Important:** If you previously set the URL to `127.0.0.1:9999`, you may need to manually edit `/var/www/splynx/addons/splynx-libreqos/config/config.json` to reflect the new URL, then run the Splynx cache clear command.

---

## Troubleshooting & Maintenance

### Clearing Splynx Cache
If you change the URL or API key in Splynx, you must clear the PHP cache using the Splynx 6.1 CLI syntax:
```bash
sudo /var/www/splynx/system/script/tools cache/clear
```

### "Token Expired" Errors
Tokens are stored in the shim's RAM. If you restart the shim (`sudo systemctl restart libreqos-shim`), all active tokens are destroyed. You must **fully refresh the Splynx Admin page** (Ctrl+Shift+R) so Splynx requests a fresh token.

### Logs
To view live logs from the shim:
```bash
sudo journalctl -u libreqos-shim -n 50 -f
```

---

## Technical Implementation Details

### Dynamic Y-Axis Scaling
The default Splynx addon (and LibreQoS) scales the graph to the customer's maximum plan speed. If a 1Gbps customer uses 50Kbps, the graph looks flat. 
This shim calculates a dynamic Y-Axis peak (baseline of 10 Mbps, with a 20% headroom above the actual highest traffic burst):

```javascript
let peak = 10 * 1e6; // 10 Mbps baseline
for (const s of history) {
  if (s.down_bps > peak) peak = s.down_bps;
  if (s.up_bps > peak) peak = s.up_bps;
}
const yMax = peak * 1.2;
```

### Handling Expired Tokens
When the Node shim restarts, all active tokens in memory are lost. If the browser tries to reload an iframe with a dead token, it caches the 403 error. The shim enforces `Cache-Control: no-store` on 403 responses to force the browser to request a fresh token from Splynx:

```javascript
res.writeHead(403, { 
  'Content-Type': 'text/plain',
  'Cache-Control': 'no-store' 
});
return res.end('Token expired or invalid. Please reload the page.');
```

### CBOR Translation
The shim translates the Splynx JSON WebSocket (`/splynx/ws/v1`) into LibreQoS CBOR (`:9123/websocket/ws`), managing the `Hello` -> `CircuitById` -> `Private CircuitWatcher` lifecycle.

---

## Roadmap / TODO
- [ ] **Customer Portal Integration**: Expose the iframe on the customer dashboard using Splynx Entry Points.
- [ ] **FUP / CAP Sync**: Parse active `SpLSQ_` limits from MikroTik and inject them into LibreQoS `integrationSplynx.py`.

---

## License
MIT License. Feel free to use, modify, and distribute.

*(Built by the community for WISP operators. Special thanks to the developers of Splynx and LibreQoS for their incredible platforms.)*