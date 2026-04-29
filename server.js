/**
 * OnCAFE QA Tool Server
 * Hosts the HTML tool, handles saves, version history
 * Deploy free to Railway: railway.app
 */
require('dotenv').config();
const express  = require('express');
const fs       = require('fs');
const path     = require('path');
const cors     = require('cors');
const crypto   = require('crypto');

const app      = express();
const PORT     = process.env.PORT || 3000;
const AUTH_KEY = process.env.AUTH_KEY || 'oncafe-2026';
const DATA_DIR = path.join(__dirname, 'data');

fs.mkdirSync(DATA_DIR, { recursive: true });

const TOOL_PATH     = path.join(DATA_DIR, 'current.html');
const VERSIONS_PATH = path.join(DATA_DIR, 'versions');
const META_PATH     = path.join(DATA_DIR, 'meta.json');
fs.mkdirSync(VERSIONS_PATH, { recursive: true });

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb', type: ['text/html','text/plain','*/*'] }));
app.use(express.raw({ limit: '50mb', type: ['application/octet-stream','*/*'] }));

// ── Auth middleware ───────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  const key = req.headers['x-auth-key'] || req.query.key;
  if (key !== AUTH_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── Load meta ─────────────────────────────────────────────────────────────────
function loadMeta() {
  try { return JSON.parse(fs.readFileSync(META_PATH)); }
  catch { return { version: 0, lastSaved: null, lastAuthor: null, saves: [] }; }
}
function saveMeta(meta) {
  fs.writeFileSync(META_PATH, JSON.stringify(meta, null, 2));
}

// ── Routes ────────────────────────────────────────────────────────────────────

// GET / — health check + info
app.get('/', (req, res) => {
  const meta = loadMeta();
  res.json({
    app:        'OnCAFE QA Tool Server',
    version:    meta.version,
    lastSaved:  meta.lastSaved,
    lastAuthor: meta.lastAuthor,
    saves:      meta.saves?.length || 0,
    status:     'running'
  });
});

// GET /tool — serve latest HTML
app.get('/tool', (req, res) => {
  if (!fs.existsSync(TOOL_PATH)) {
    return res.status(404).send('<!-- Tool not yet uploaded -->');
  }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(TOOL_PATH);
});

// GET /tool/meta — version info without full HTML
app.get('/tool/meta', (req, res) => {
  const meta = loadMeta();
  res.json(meta);
});

// POST /tool — save updated HTML (requires auth)
app.post('/tool', requireAuth, (req, res) => {
  const html   = req.body;
  const author = req.headers['x-author'] || 'Unknown';

  // Accept body as Buffer or string
  let htmlStr;
  if (Buffer.isBuffer(html)) {
    htmlStr = html.toString('utf8');
  } else if (typeof html === 'string') {
    htmlStr = html;
  } else {
    htmlStr = String(html);
  }
  // Strip UTF-8 BOM if present
  if (htmlStr.charCodeAt(0) === 0xFEFF) htmlStr = htmlStr.slice(1);
  if (!htmlStr || htmlStr.length < 10) {
    return res.status(400).json({ error: 'Empty body' });
  }

  const meta    = loadMeta();
  const version = (meta.version || 0) + 1;
  const ts      = new Date().toISOString();

  // Archive previous version
  if (fs.existsSync(TOOL_PATH)) {
    const archiveName = `v${String(meta.version).padStart(4,'0')}_${ts.slice(0,19).replace(/:/g,'-')}.html`;
    fs.copyFileSync(TOOL_PATH, path.join(VERSIONS_PATH, archiveName));
  }

  // Save new current
  fs.writeFileSync(TOOL_PATH, htmlStr, 'utf8');

  // Update meta
  meta.version    = version;
  meta.lastSaved  = ts;
  meta.lastAuthor = author;
  if (!meta.saves) meta.saves = [];
  meta.saves.unshift({ version, ts, author, size: html.length });
  if (meta.saves.length > 100) meta.saves = meta.saves.slice(0, 100);
  saveMeta(meta);

  console.log(`[SAVE] v${version} by ${author} — ${(html.length/1024).toFixed(0)}KB`);
  res.json({ ok: true, version, ts, author });
});

// GET /versions — list saved versions
app.get('/versions', requireAuth, (req, res) => {
  const meta = loadMeta();
  res.json(meta.saves || []);
});

// GET /version/:v — restore a specific version
app.get('/version/:v', requireAuth, (req, res) => {
  const files = fs.readdirSync(VERSIONS_PATH)
    .filter(f => f.startsWith('v' + String(req.params.v).padStart(4,'0')));
  if (!files.length) return res.status(404).json({ error: 'Version not found' });
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.sendFile(path.join(VERSIONS_PATH, files[0]));
});

app.listen(PORT, () => {
  console.log(`\nOnCAFE QA Server running on port ${PORT}`);
  console.log(`AUTH_KEY: ${AUTH_KEY}`);
  console.log(`Data dir: ${DATA_DIR}\n`);
});
