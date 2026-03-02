// ═══════════════════════════════════════════════════════════
//  Mail Merge Tracking Server
//  Writes status back to Google Sheet — just like YAMM!
//  EMAIL_SENT → EMAIL_OPENED → EMAIL_CLICKED → UNSUBSCRIBED
// ═══════════════════════════════════════════════════════════

const express = require('express');
const cors    = require('cors');
const { google } = require('googleapis');
const app = express();

app.use(cors());
app.use(express.json());

const PIXEL = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');

// ── Google Sheets auth ─────────────────────────────────────
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ═══════════════════════════════════════════════════════════
//  CORE — Update Merge Status in the sender's Google Sheet
//  This is exactly how YAMM works!
// ═══════════════════════════════════════════════════════════
async function updateMergeStatus(sheetId, tab, email, newStatus) {
  if (!sheetId || !email) return;
  try {
    const sheets = await getSheetsClient();
    const sheetTab = tab || 'Sheet1';

    // Read all data to find the row with this email
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${sheetTab}!A1:Z500`,
    });

    const rows = result.data.values || [];
    if (rows.length < 2) return;

    const headers  = rows[0].map(h => h.toLowerCase().trim());
    const emailCol = headers.findIndex(h => h.includes('email'));
    const statCol  = headers.findIndex(h => h.includes('merge status') || h === 'status');

    if (emailCol === -1 || statCol === -1) return;

    // Find the row number matching this email
    let targetRow = -1;
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim()) {
        targetRow = i + 1; // 1-based
        break;
      }
    }
    if (targetRow === -1) return;

    // Status priority — never downgrade
    const PRIORITY = { EMAIL_SENT: 1, EMAIL_OPENED: 2, EMAIL_CLICKED: 3, RESPONDED: 4, UNSUBSCRIBED: 5 };
    const current = ((rows[targetRow - 1] || [])[statCol] || '').toUpperCase();
    if ((PRIORITY[current] || 0) >= (PRIORITY[newStatus] || 0)) return;

    // Write new status
    const col = toColLetter(statCol + 1);
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetTab}!${col}${targetRow}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[newStatus]] },
    });

    console.log(`✅ ${email} → ${newStatus} (row ${targetRow})`);
  } catch (e) {
    console.error('Sheet update error:', e.message);
  }
}

async function logEvent(event) {
  if (!process.env.TRACKING_SHEET_ID) return;
  try {
    const sheets = await getSheetsClient();
    const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.TRACKING_SHEET_ID,
      range: 'Tracking!A:F',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[now, event.type, event.email, event.campaign || '', event.url || '', event.ip || '']] },
    });
  } catch (e) { /* silent fail */ }
}

function toColLetter(n) {
  let s = '';
  while (n > 0) { const r = (n-1)%26; s = String.fromCharCode(65+r)+s; n = Math.floor((n-1)/26); }
  return s;
}

// ═══════════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════════

// 📬 Email Opened
app.get('/track/open', async (req, res) => {
  const { email, campaign, sheetId, tab } = req.query;
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`📬 OPENED: ${email}`);
  await updateMergeStatus(sheetId, tab, email, 'EMAIL_OPENED');
  await logEvent({ type: 'EMAIL_OPENED', email, campaign, ip });
  res.set({ 'Content-Type': 'image/gif', 'Cache-Control': 'no-cache, no-store' });
  res.send(PIXEL);
});

// 🖱️ Link Clicked
app.get('/track/click', async (req, res) => {
  const { email, campaign, url, sheetId, tab } = req.query;
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`🖱️ CLICKED: ${email}`);
  await updateMergeStatus(sheetId, tab, email, 'EMAIL_CLICKED');
  await logEvent({ type: 'EMAIL_CLICKED', email, campaign, url, ip });
  res.redirect(decodeURIComponent(url || 'https://google.com'));
});

// 🚫 Unsubscribe
app.get('/unsubscribe', async (req, res) => {
  const { email, campaign, sheetId, tab } = req.query;
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`🚫 UNSUBSCRIBED: ${email}`);
  await updateMergeStatus(sheetId, tab, email, 'UNSUBSCRIBED');
  await logEvent({ type: 'UNSUBSCRIBED', email, campaign, ip });
  res.send(`<!DOCTYPE html><html><head><title>Unsubscribed</title>
  <style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;display:flex;align-items:center;justify-content:center;min-height:100vh}.card{background:white;border-radius:16px;padding:48px;text-align:center;max-width:400px;box-shadow:0 4px 24px rgba(0,0,0,.08)}.icon{font-size:52px;margin-bottom:16px}h1{font-size:22px;color:#202124;margin-bottom:10px}p{font-size:14px;color:#5f6368;line-height:1.7}.em{font-weight:600;color:#1a73e8}</style>
  </head><body><div class="card"><div class="icon">✅</div>
  <h1>You've been unsubscribed</h1>
  <p>The address <span class="em">${email}</span> has been removed from this mailing list.<br>You won't receive further emails from this campaign.</p>
  </div></body></html>`);
});

// 📊 Dashboard
app.get('/dashboard', async (req, res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).send('Unauthorized. Add ?key=YOUR_PASSWORD');
  try {
    const sheets = await getSheetsClient();
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.TRACKING_SHEET_ID,
      range: 'Tracking!A:F',
    });
    const rows   = (result.data.values || []).slice(1).reverse();
    const opens  = rows.filter(r => r[1] === 'EMAIL_OPENED').length;
    const clicks = rows.filter(r => r[1] === 'EMAIL_CLICKED').length;
    const unsubs = rows.filter(r => r[1] === 'UNSUBSCRIBED').length;

    const badgeColor = { EMAIL_OPENED:'#e8f0fe;color:#1a73e8', EMAIL_CLICKED:'#e6f4ea;color:#1e8e3e', UNSUBSCRIBED:'#fce8e6;color:#d93025', EMAIL_SENT:'#f1f3f4;color:#5f6368' };

    res.send(`<!DOCTYPE html><html><head><title>Dashboard</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;padding:24px}
    h1{font-size:22px;color:#202124;margin-bottom:20px}
    .stats{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:22px}
    .stat{background:white;border-radius:12px;padding:20px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)}
    .n{font-size:40px;font-weight:700;line-height:1}.l{font-size:12px;color:#5f6368;margin-top:6px}
    .o .n{color:#1a73e8}.c .n{color:#1e8e3e}.u .n{color:#d93025}
    table{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}
    th{background:#1a73e8;color:white;padding:10px 14px;text-align:left;font-size:12px}
    td{padding:8px 14px;font-size:12px;border-bottom:1px solid #f1f3f4}
    .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
    </style></head><body>
    <h1>📊 Mail Merge Tracking</h1>
    <div class="stats">
      <div class="stat o"><div class="n">${opens}</div><div class="l">📬 Opened</div></div>
      <div class="stat c"><div class="n">${clicks}</div><div class="l">🖱️ Clicked</div></div>
      <div class="stat u"><div class="n">${unsubs}</div><div class="l">🚫 Unsubscribed</div></div>
    </div>
    <table><tr><th>Date &amp; Time</th><th>Event</th><th>Email</th><th>Campaign</th></tr>
    ${rows.slice(0,100).map(r=>{
      const bc = badgeColor[r[1]] || '#f1f3f4;color:#5f6368';
      return `<tr><td>${r[0]||''}</td><td><span class="badge" style="background:${bc.split(';')[0].replace('background:','')};${bc.split(';')[1]||''}">${r[1]||''}</span></td><td>${r[2]||''}</td><td>${r[3]||''}</td></tr>`;
    }).join('')}
    </table></body></html>`);
  } catch(e) { res.status(500).send('Error: ' + e.message); }
});

app.get('/', (req, res) => res.json({ status: '✅ Mail Merge Tracker running', time: new Date() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Tracker on port ${PORT}`));
