const express = require('express');
const cors    = require('cors');
const { google } = require('googleapis');
const app = express();
app.use(cors());
app.use(express.json());

const PIXEL = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7','base64');

// In-memory store for opens/clicks (resets on redeploy, but that's ok)
const trackingStore = {}; // { email: { opened: bool, clicked: bool, time: Date } }

async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function updateStatus(sheetId, tab, email, newStatus) {
  if (!sheetId || !email) return;
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${tab||'Sheet1'}!A1:Z500`
    });
    const rows = r.data.values || [];
    if (rows.length < 2) return;

    const headers  = rows[0].map(h => h.toLowerCase().trim());
    const emailCol = headers.findIndex(h => h.includes('email'));
    const statCol  = headers.findIndex(h => h.includes('merge status') || h === 'status');
    if (emailCol < 0 || statCol < 0) return;

    let targetRow = -1;
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][emailCol]||'').toLowerCase().trim() === email.toLowerCase().trim()) {
        targetRow = i + 1; break;
      }
    }
    if (targetRow < 0) return;

    const P = { EMAIL_SENT:1, EMAIL_OPENED:2, EMAIL_CLICKED:3, EMAIL_BOUNCED:4, UNSUBSCRIBED:5 };
    const cur = ((rows[targetRow-1]||[])[statCol]||'').toUpperCase();
    if ((P[cur]||0) >= (P[newStatus]||0)) return;

    const col = toCol(statCol+1);
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${tab||'Sheet1'}!${col}${targetRow}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[newStatus]] },
    });
    console.log(`✅ ${email} → ${newStatus}`);
  } catch(e) { console.error('updateStatus error:', e.message); }
}

async function logEvent(e) {
  if (!process.env.TRACKING_SHEET_ID) return;
  try {
    const sheets = await getSheets();
    const now = new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'});
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.TRACKING_SHEET_ID,
      range: 'Tracking!A:F', valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[now, e.type, e.email, e.campaign||'', e.url||'', e.ip||'']] },
    });
  } catch(e) {}
}

function toCol(n) {
  let s=''; while(n>0){const r=(n-1)%26;s=String.fromCharCode(65+r)+s;n=Math.floor((n-1)/26);} return s;
}

// ── Keep alive ping ────────────────────────────────────────
app.get('/ping', (req,res) => res.json({ status:'alive', time: new Date() }));

// ── 📬 Email Opened ────────────────────────────────────────
app.get('/track/open', async (req,res) => {
  // Return pixel IMMEDIATELY — never make email client wait
  res.set({'Content-Type':'image/gif','Cache-Control':'no-cache,no-store,must-revalidate','Pragma':'no-cache'});
  res.send(PIXEL);

  const {email, campaign, sheetId, tab} = req.query;
  if (!email) return;

  const key = email.toLowerCase();
  console.log(`📬 OPENED: ${email}`);

  // Store in memory
  if (!trackingStore[key]) trackingStore[key] = {};
  trackingStore[key].opened = true;
  trackingStore[key].openTime = new Date().toISOString();

  // Update sheet in background
  setImmediate(async () => {
    await updateStatus(sheetId, tab, email, 'EMAIL_OPENED');
    await logEvent({type:'EMAIL_OPENED', email, campaign});
  });
});

// ── 🖱️ Link Clicked ───────────────────────────────────────
app.get('/track/click', async (req,res) => {
  const {email, campaign, url, sheetId, tab} = req.query;

  // Redirect IMMEDIATELY
  const dest = url ? decodeURIComponent(url) : 'https://google.com';
  res.redirect(dest);

  if (!email) return;
  const key = email.toLowerCase();
  console.log(`🖱️ CLICKED: ${email}`);

  if (!trackingStore[key]) trackingStore[key] = {};
  trackingStore[key].clicked = true;
  trackingStore[key].clickTime = new Date().toISOString();

  setImmediate(async () => {
    await updateStatus(sheetId, tab, email, 'EMAIL_CLICKED');
    await logEvent({type:'EMAIL_CLICKED', email, campaign, url});
  });
});

// ── 🔍 Check endpoint — background.js polls this ──────────
app.get('/check', (req,res) => {
  const email = (req.query.email||'').toLowerCase();
  const data  = trackingStore[email] || {};
  res.json({ opened: !!data.opened, clicked: !!data.clicked, openTime: data.openTime, clickTime: data.clickTime });
});

// ── 🚫 Unsubscribe ─────────────────────────────────────────
app.get('/unsubscribe', async (req,res) => {
  const {email, campaign, sheetId, tab} = req.query;
  console.log(`🚫 UNSUBSCRIBED: ${email}`);
  setImmediate(async () => {
    await updateStatus(sheetId, tab, email, 'UNSUBSCRIBED');
    await logEvent({type:'UNSUBSCRIBED', email, campaign});
  });
  res.send(`<!DOCTYPE html><html><head><title>Unsubscribed</title>
  <style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;display:flex;align-items:center;justify-content:center;min-height:100vh}.card{background:white;border-radius:16px;padding:48px;text-align:center;max-width:400px;box-shadow:0 4px 24px rgba(0,0,0,.08)}.icon{font-size:52px;margin-bottom:16px}h1{font-size:22px;color:#202124;margin-bottom:10px}p{font-size:14px;color:#5f6368;line-height:1.7}.em{font-weight:600;color:#1a73e8}</style>
  </head><body><div class="card"><div class="icon">✅</div><h1>Unsubscribed</h1>
  <p>The address <span class="em">${email}</span> has been removed.</p></div></body></html>`);
});

// ── 📊 Dashboard ───────────────────────────────────────────
app.get('/dashboard', async (req,res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).send('Add ?key=YOUR_PASSWORD');
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: process.env.TRACKING_SHEET_ID, range:'Tracking!A:F' });
    const rows   = (r.data.values||[]).slice(1).reverse();
    const opens  = rows.filter(r=>r[1]==='EMAIL_OPENED').length;
    const clicks = rows.filter(r=>r[1]==='EMAIL_CLICKED').length;
    const unsubs = rows.filter(r=>r[1]==='UNSUBSCRIBED').length;
    const bounces = rows.filter(r=>r[1]==='EMAIL_BOUNCED').length;

    res.send(`<!DOCTYPE html><html><head><title>📊 Dashboard</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
      *{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;padding:24px}
      h1{font-size:22px;color:#202124;margin-bottom:20px}
      .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}
      .stat{background:white;border-radius:12px;padding:16px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)}
      .n{font-size:36px;font-weight:700;line-height:1}.l{font-size:11px;color:#5f6368;margin-top:6px}
      .o .n{color:#1a73e8}.c .n{color:#1e8e3e}.u .n{color:#d93025}.b .n{color:#f57c00}
      table{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}
      th{background:#1a73e8;color:white;padding:10px 14px;text-align:left;font-size:12px}
      td{padding:8px 14px;font-size:12px;border-bottom:1px solid #f1f3f4}
      .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
      .EMAIL_OPENED{background:#e8f0fe;color:#1a73e8}
      .EMAIL_CLICKED{background:#e6f4ea;color:#1e8e3e}
      .UNSUBSCRIBED{background:#fce8e6;color:#d93025}
      .EMAIL_BOUNCED{background:#fff3e0;color:#f57c00}
      .EMAIL_SENT{background:#f1f3f4;color:#5f6368}
    </style></head><body>
    <h1>📊 Mail Merge Dashboard</h1>
    <div class="stats">
      <div class="stat o"><div class="n">${opens}</div><div class="l">📬 Opened</div></div>
      <div class="stat c"><div class="n">${clicks}</div><div class="l">🖱️ Clicked</div></div>
      <div class="stat b"><div class="n">${bounces}</div><div class="l">🔴 Bounced</div></div>
      <div class="stat u"><div class="n">${unsubs}</div><div class="l">🚫 Unsubscribed</div></div>
    </div>
    <table>
      <tr><th>Date & Time</th><th>Event</th><th>Email</th><th>Campaign</th></tr>
      ${rows.slice(0,100).map(r=>`<tr>
        <td>${r[0]||''}</td>
        <td><span class="badge ${r[1]||''}">${r[1]||''}</span></td>
        <td>${r[2]||''}</td><td>${r[3]||''}</td>
      </tr>`).join('')}
    </table></body></html>`);
  } catch(e) { res.status(500).send('Error: '+e.message); }
});

app.get('/', (req,res) => res.json({ status:'✅ Running', time: new Date() }));
const PORT = process.env.PORT||3000;
app.listen(PORT, () => console.log(`✅ Tracker on port ${PORT}`));
