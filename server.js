require('dotenv').config();
const express = require('express');
const fs      = require('fs');
const path    = require('path');
const cors    = require('cors');

const app      = express();
const PORT     = process.env.PORT || 3000;
const AUTH_KEY = process.env.AUTH_KEY || 'oncafe-2026';
const DATA_DIR = path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

const TOOL_PATH     = path.join(DATA_DIR, 'current.html');
const VERSIONS_PATH = path.join(DATA_DIR, 'versions');
const META_PATH     = path.join(DATA_DIR, 'meta.json');
fs.mkdirSync(VERSIONS_PATH, { recursive: true });

app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb', type: ['text/html','text/plain','*/*'] }));
app.use(express.raw({  limit: '50mb', type: ['application/octet-stream','*/*'] }));

function auth(req, res, next) {
  if ((req.headers['x-auth-key'] || req.query.key) !== AUTH_KEY)
    return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function loadMeta() {
  try { return JSON.parse(fs.readFileSync(META_PATH)); }
  catch { return { version: 0, lastSaved: null, lastAuthor: null, saves: [] }; }
}
function saveMeta(m) { fs.writeFileSync(META_PATH, JSON.stringify(m, null, 2)); }

// GET / ── redirect to tool
app.get('/', (req, res) => {
  if (req.headers.accept && req.headers.accept.includes('text/html') && fs.existsSync(TOOL_PATH))
    return res.redirect('/tool');
  const m = loadMeta();
  res.json({ app:'OnCAFE QA Server', version:m.version, lastSaved:m.lastSaved, status:'running' });
});

// GET /tool ── serve HTML
app.get('/tool', (req, res) => {
  if (!fs.existsSync(TOOL_PATH)) {
    res.type('html');
    return res.status(404).send('<!DOCTYPE html><html><body style="background:#111;color:#fff;font-family:sans-serif;padding:40px"><h2>Tool not yet uploaded</h2><p>POST HTML to /tool with x-auth-key header</p></body></html>');
  }
  let html = fs.readFileSync(TOOL_PATH, 'utf8');
  if (html.charCodeAt(0) === 0xFEFF) html = html.slice(1);
  const buf = Buffer.from(html, 'utf8');
  res.type('html');
  res.set('Cache-Control', 'no-store');
  res.set('Content-Length', buf.length);
  res.status(200).end(buf);
});

// POST /tool ── save full HTML
app.post('/tool', auth, (req, res) => {
  let html = req.body;
  if (Buffer.isBuffer(html)) html = html.toString('utf8');
  if (typeof html !== 'string') html = String(html);
  if (html.charCodeAt(0) === 0xFEFF) html = html.slice(1);
  if (!html || html.length < 100) return res.status(400).json({ error: 'Empty body' });

  const author  = req.headers['x-author'] || 'Unknown';
  const meta    = loadMeta();
  const version = (meta.version || 0) + 1;
  const ts      = new Date().toISOString();

  if (fs.existsSync(TOOL_PATH)) {
    const arc = 'v' + String(meta.version).padStart(4,'0') + '_' + ts.slice(0,19).replace(/:/g,'-') + '.html';
    try { fs.copyFileSync(TOOL_PATH, path.join(VERSIONS_PATH, arc)); } catch(e) {}
  }

  fs.writeFileSync(TOOL_PATH, html, 'utf8');
  meta.version = version; meta.lastSaved = ts; meta.lastAuthor = author;
  if (!meta.saves) meta.saves = [];
  meta.saves.unshift({ version, ts, author, size: html.length });
  if (meta.saves.length > 100) meta.saves = meta.saves.slice(0, 100);
  saveMeta(meta);

  console.log('[SAVE] v' + version + ' by ' + author + ' — ' + Math.round(html.length/1024) + 'KB');
  res.json({ ok: true, version, ts, author });
});

// POST /state ── merge state JSON into stored HTML
app.post('/state', auth, (req, res) => {
  const author = req.headers['x-author'] || 'Auto-save';
  try {
    const state = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    if (!state || state.length < 2) return res.status(400).json({ error: 'Empty state' });
    if (!fs.existsSync(TOOL_PATH)) return res.status(404).json({ error: 'No HTML uploaded yet' });

    let html = fs.readFileSync(TOOL_PATH, 'utf8');
    html = html.replace(/<script id="baked-state">[\s\S]*?<\/script>/g, '');
    html = html.replace('</head>', '<script id="baked-state">window.__BAKED_STATE__=' + state + ';<\/script></head>');
    fs.writeFileSync(TOOL_PATH, html, 'utf8');

    const meta = loadMeta();
    meta.lastSaved = new Date().toISOString();
    meta.lastAuthor = author;
    saveMeta(meta);

    console.log('[STATE] Saved by ' + author);
    res.json({ ok: true, author, ts: meta.lastSaved });
  } catch(e) {
    console.error('[STATE]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /tool/meta
app.get('/tool/meta', (req, res) => res.json(loadMeta()));

// GET /versions
app.get('/versions', auth, (req, res) => res.json(loadMeta().saves || []));

app.listen(PORT, () => {
  console.log('OnCAFE QA Server running on port ' + PORT);
  console.log('AUTH_KEY: ' + AUTH_KEY);
  console.log('Data dir: ' + DATA_DIR);
});
