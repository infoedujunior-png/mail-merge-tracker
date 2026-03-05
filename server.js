// ═══════════════════════════════════════════════════════════
//  Mail Merge Tracking Server v5.0 — FULL 24/7 SOLUTION
//  ✅ Refresh tokens — schedule 24hr+ later, Chrome closed!
//  ✅ Server-side bounce detection — no Chrome needed!
//  ✅ Cell colors, unsubscribe, all tracking working
// ═══════════════════════════════════════════════════════════

const express = require('express');
const cors    = require('cors');
const fs      = require('fs');
const path    = require('path');
const { google } = require('googleapis');
const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const PIXEL = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7','base64');

const trackingStore = {};
const scheduleStore = {};
let lastBounceCheck = 0;

// ══════════════════════════════════════════════════════════
//  PERSISTENT TOKEN STORAGE — survives Render restarts! ✅
// ══════════════════════════════════════════════════════════
const TOKENS_FILE = path.join('/tmp', 'edujunior_tokens.json');

function loadUserStore() {
  try {
    if (fs.existsSync(TOKENS_FILE)) {
      const data = JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf8'));
      console.log(`📂 Loaded ${Object.keys(data).length} users from disk`);
      return data;
    }
  } catch(e) { console.error('Load tokens error:', e.message); }
  return {};
}

function saveUserStore() {
  try {
    // Save only essential fields — not processedBounces Set (not serializable)
    const toSave = {};
    for (const [email, user] of Object.entries(userStore)) {
      toSave[email] = {
        refreshToken: user.refreshToken,
        accessToken:  user.accessToken,
        sheetId:      user.sheetId,
        sheetTab:     user.sheetTab,
        savedAt:      user.savedAt,
      };
    }
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(toSave, null, 2));
    console.log(`💾 Saved ${Object.keys(toSave).length} users to disk`);
  } catch(e) { console.error('Save tokens error:', e.message); }
}

// Load on startup
const userStore = loadUserStore();

// ── Service Account Sheets ─────────────────────────────────
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ✅ REFRESH TOKEN — get new access token anytime
async function getAccessToken(userEmail) {
  const user = userStore[userEmail];
  if (!user) return null;
  if (!user.refreshToken) return user.accessToken || null;
  try {
    const res = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        client_id:     process.env.OAUTH_CLIENT_ID,
        client_secret: process.env.OAUTH_CLIENT_SECRET,
        refresh_token: user.refreshToken,
        grant_type:    'refresh_token',
      }),
    });
    const data = await res.json();
    if (data.access_token) {
      user.accessToken = data.access_token;
      saveUserStore(); // ✅ Save latest access token
      console.log(`🔑 Token refreshed for ${userEmail}`);
      return data.access_token;
    }
    console.error(`Token refresh failed: ${data.error}`);
    return user.accessToken || null;
  } catch(e) { return user.accessToken || null; }
}

const STATUS_COLORS = {
  SCHEDULED:     { red:0.68, green:0.85, blue:0.90 }, // light blue
  EMAIL_SENT:    { red:0.85, green:0.85, blue:0.85 },
  EMAIL_OPENED:  { red:0.72, green:0.94, blue:0.74 },
  EMAIL_CLICKED: { red:0.20, green:0.66, blue:0.33 },
  EMAIL_BOUNCED: { red:0.96, green:0.40, blue:0.40 },
  UNSUBSCRIBED:  { red:1.00, green:0.76, blue:0.28 },
};

function toCol(n) { let s=''; while(n>0){const r=(n-1)%26;s=String.fromCharCode(65+r)+s;n=Math.floor((n-1)/26);} return s; }
function sleep(ms) { return new Promise(r=>setTimeout(r,ms)); }

// ── Update sheet via service account ──────────────────────
async function updateStatus(sheetId, tab, email, newStatus) {
  if (!sheetId||!email) return;
  const sheetTab = tab||'Sheet1';
  console.log(`🔄 ${email} → ${newStatus} | ${sheetId} | ${sheetTab}`);
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId:sheetId, range:`${sheetTab}!A1:Z500` });
    const rows = r.data.values||[];
    if (rows.length<2) return;
    const headers = rows[0].map(h=>(h||'').toLowerCase().trim());
    const emailCol = headers.findIndex(h=>h.includes('email'));
    let statCol    = headers.findIndex(h=>h.includes('merge status')||h==='status');
    if (emailCol<0) { console.log('❌ No email col'); return; }
    if (statCol<0) {
      statCol = headers.length;
      await sheets.spreadsheets.values.update({ spreadsheetId:sheetId, range:`${sheetTab}!${toCol(statCol+1)}1`, valueInputOption:'USER_ENTERED', requestBody:{values:[['Merge Status']]} });
    }
    let row=-1;
    for (let i=1;i<rows.length;i++) { if((rows[i][emailCol]||'').toLowerCase().trim()===email.toLowerCase().trim()){row=i+1;break;} }
    if (row<0) { console.log(`❌ Not found: ${email}`); return; }
    const P={SCHEDULED:0,EMAIL_SENT:1,EMAIL_OPENED:2,EMAIL_CLICKED:3,EMAIL_BOUNCED:4,UNSUBSCRIBED:5};
    const cur=((rows[row-1]||[])[statCol]||'').toUpperCase().trim();
    if ((P[cur]||0)>=(P[newStatus]||0)) { console.log(`⏭️ Skip ${cur}>=${newStatus}`); return; }
    await sheets.spreadsheets.values.update({ spreadsheetId:sheetId, range:`${sheetTab}!${toCol(statCol+1)}${row}`, valueInputOption:'USER_ENTERED', requestBody:{values:[[newStatus]]} });

    // ✅ Get sheet GID
    let gid = 0;
    try {
      const meta = await sheets.spreadsheets.get({ spreadsheetId:sheetId, fields:'sheets.properties' });
      const sheetObj = (meta.data.sheets||[]).find(s => s.properties.title === sheetTab);
      gid = sheetObj?.properties?.sheetId ?? 0;
    } catch(e) { console.log('GID fetch error:', e.message); }

    console.log('GID:', gid, '| row:', row, '| statCol:', statCol);

    // ✅ Use ONE updateCells request — sets BOTH color AND note together
    const color  = STATUS_COLORS[newStatus];
    const isBold = ['EMAIL_CLICKED','EMAIL_BOUNCED'].includes(newStatus);
    const nowStr = new Date().toLocaleString('en-IN', {
      timeZone:'Asia/Kolkata', day:'2-digit', month:'short',
      year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true
    });
    const noteMap = { EMAIL_SENT:'Sent', EMAIL_OPENED:'Opened', EMAIL_CLICKED:'Clicked', EMAIL_BOUNCED:'Bounced', UNSUBSCRIBED:'Unsubscribed' };
    const noteText = (noteMap[newStatus]||newStatus) + ' on: ' + nowStr;

    const cellValue = {
      note: noteText,
    };
    if (color) {
      cellValue.userEnteredFormat = {
        backgroundColor: color,
        textFormat: {
          bold: isBold,
          foregroundColor: isBold ? {red:1,green:1,blue:1} : {red:0.1,green:0.1,blue:0.1}
        }
      };
    }

    try {
      const fieldMask = color ? 'note,userEnteredFormat(backgroundColor,textFormat)' : 'note';
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: sheetId,
        requestBody: { requests: [{
          updateCells: {
            range: { sheetId:gid, startRowIndex:row-1, endRowIndex:row, startColumnIndex:statCol, endColumnIndex:statCol+1 },
            rows: [{ values: [cellValue] }],
            fields: fieldMask,
          }
        }]}
      });
      console.log('✅ Color+Note OK:', noteText);
    } catch(e) { console.error('❌ batchUpdate FAILED:', e.message, JSON.stringify(e.errors||[])); }

    console.log(`✅ ${email} → ${newStatus} row ${row}`);
  } catch(e) { console.error(`updateStatus error: ${e.message}`); }
}

// ── Update sheet via user token ────────────────────────────
async function updateStatusUserToken(token, sheetId, tab, email, status) {
  const sheetTab=tab||'Sheet1';
  try {
    const res=await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab+'!A1:Z500')}`,{headers:{Authorization:`Bearer ${token}`}});
    if (!res.ok) return;
    const data=await res.json();
    const rows=data.values||[];
    if (rows.length<2) return;
    const headers=rows[0].map(h=>(h||'').toLowerCase().trim());
    const emailCol=headers.findIndex(h=>h.includes('email'));
    let statCol=headers.findIndex(h=>h.includes('merge status')||h==='status');
    if (emailCol<0) return;
    if (statCol<0) {
      statCol=headers.length;
      await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab+'!'+toCol(statCol+1)+'1')}?valueInputOption=USER_ENTERED`,{method:'PUT',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify({values:[['Merge Status']]})});
    }
    let row=-1;
    for(let i=1;i<rows.length;i++){if((rows[i][emailCol]||'').toLowerCase().trim()===email.toLowerCase().trim()){row=i+1;break;}}
    if (row<0) return;
    const P={SCHEDULED:0,EMAIL_SENT:1,EMAIL_OPENED:2,EMAIL_CLICKED:3,EMAIL_BOUNCED:4,UNSUBSCRIBED:5};
    const cur=((rows[row-1]||[])[statCol]||'').toUpperCase().trim();
    if ((P[cur]||0)>=(P[status]||0)) return;
    await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab+'!'+toCol(statCol+1)+row)}?valueInputOption=USER_ENTERED`,{method:'PUT',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify({values:[[status]]})});
    // ✅ Color + Note via USER TOKEN (same token used for sheet write)
    try {
      const nowStr  = new Date().toLocaleString('en-IN', { timeZone:'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true });
      const noteMap = { EMAIL_SENT:'Sent', EMAIL_OPENED:'Opened', EMAIL_CLICKED:'Clicked', EMAIL_BOUNCED:'Bounced', UNSUBSCRIBED:'Unsubscribed' };
      const noteText = (noteMap[status]||status) + ' on: ' + nowStr;
      const color    = STATUS_COLORS[status];
      const isBold   = ['EMAIL_CLICKED','EMAIL_BOUNCED'].includes(status);

      // Get GID via user token
      let gid = 0;
      try {
        const metaRes = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}?fields=sheets.properties`, { headers:{Authorization:`Bearer ${token}`} });
        const metaData = await metaRes.json();
        const sheetObj = (metaData.sheets||[]).find(s => s.properties.title === sheetTab);
        gid = sheetObj?.properties?.sheetId ?? 0;
      } catch(e) {}

      const cellValue = { note: noteText };
      if (color) {
        cellValue.userEnteredFormat = {
          backgroundColor: color,
          textFormat: { bold:isBold, foregroundColor: isBold?{red:1,green:1,blue:1}:{red:0.1,green:0.1,blue:0.1} }
        };
      }
      const fieldMask = color ? 'note,userEnteredFormat(backgroundColor,textFormat)' : 'note';

      // batchUpdate via USER TOKEN
      const batchRes = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}:batchUpdate`, {
        method: 'POST',
        headers: { Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
        body: JSON.stringify({ requests: [{ updateCells: {
          range: { sheetId:gid, startRowIndex:row-1, endRowIndex:row, startColumnIndex:statCol, endColumnIndex:statCol+1 },
          rows: [{ values: [cellValue] }],
          fields: fieldMask,
        }}]})
      });

      if (batchRes.ok) {
        console.log('✅ Color+Note (user-token):', noteText);
      } else {
        const errText = await batchRes.text();
        console.error('❌ Color+Note error:', errText.slice(0,200));
      }
    } catch(e) { console.error('❌ Color+Note exception:', e.message); }

    console.log(`✅ User-token: ${email} → ${status}`);
  } catch(e) { console.error('updateStatusUserToken:', e.message); }
}

async function logEvent(e) {
  if (!process.env.TRACKING_SHEET_ID) return;
  try { const sheets=await getSheets(); const now=new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}); await sheets.spreadsheets.values.append({spreadsheetId:process.env.TRACKING_SHEET_ID,range:'Tracking!A:F',valueInputOption:'USER_ENTERED',requestBody:{values:[[now,e.type,e.email,e.campaign||'',e.url||'',' ']]}});} catch(e){}
}

// ═══════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════
//  SERVER-SIDE BOUNCE DETECTION — Instant + Scheduled
// ═══════════════════════════════════════════════════════════

// Run bounce check every 2 minutes automatically (no need for ping!)
setInterval(() => {
  checkAllBounces().catch(e => console.error('Interval bounce error:', e.message));
}, 2 * 60 * 1000);

async function checkBouncesForUser(userEmail) {
  const user = userStore[userEmail];
  if (!user?.sheetId) { console.log(`⏭️ Skip ${userEmail} — no sheetId`); return; }

  const token = await getAccessToken(userEmail);
  if (!token) { console.log(`⏭️ Skip ${userEmail} — no token`); return; }

  if (!user.processedBounces) user.processedBounces = new Set();

  try {
    // Try multiple queries — different mail servers use different senders
    const queries = [
      'from:mailer-daemon newer_than:7d',
      'from:postmaster newer_than:7d',
      'subject:"delivery status notification" newer_than:7d',
      'subject:"undeliverable" newer_than:7d',
      'subject:"mail delivery failed" newer_than:7d',
      'subject:"failure notice" newer_than:7d',
    ];

    const seen = new Set();
    const allMsgs = [];

    for (const rawQ of queries) {
      const q = encodeURIComponent(rawQ);
      const res = await fetch(
        `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=20`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      if (!res.ok) {
        console.error(`❌ Gmail API ${res.status} for query: ${rawQ}`);
        continue;
      }
      const data = await res.json();
      for (const m of (data.messages || [])) {
        if (!seen.has(m.id)) { seen.add(m.id); allMsgs.push(m); }
      }
    }

    console.log(`📧 ${userEmail}: ${allMsgs.length} total bounce candidates`);
    if (!allMsgs.length) return;

    for (const msg of allMsgs) {
      if (user.processedBounces.has(msg.id)) continue;

      const mr = await fetch(
        `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=full`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      if (!mr.ok) { user.processedBounces.add(msg.id); continue; }

      const md      = await mr.json();
      const hdrs    = md.payload?.headers || [];
      const subject = hdrs.find(h => h.name === 'Subject')?.value || '';
      const fromHdr = hdrs.find(h => h.name === 'From')?.value || '';
      const body    = getEmailBody(md.payload);
      const full    = subject + ' ' + fromHdr + ' ' + body;

      console.log(`  → Subject: "${subject.slice(0,80)}" | From: "${fromHdr.slice(0,50)}"`);

      // Must look like a bounce
      const looksLikeBounce =
        /mailer.daemon|postmaster|mail.*delivery|delivery.*fail|undeliver|bounce|failure notice|returned mail/i.test(subject + fromHdr);
      if (!looksLikeBounce) {
        console.log(`  → Skipped (not a bounce)`);
        user.processedBounces.add(msg.id);
        continue;
      }

      const info = parseBounce(full, hdrs, userEmail); // ✅ Pass sender to exclude it
      console.log(`  → Parsed: email="${info.email}" reason="${info.reason}" smtp=${info.smtpCode} enh=${info.enhCode}`);

      if (info.email) {
        console.log(`🔴 BOUNCE: ${info.email} — ${info.reason}`);
        await updateStatus(user.sheetId, user.sheetTab || 'Sheet1', info.email, 'EMAIL_BOUNCED');
        await writeBounceReason(user.sheetId, user.sheetTab || 'Sheet1', info.email, info.reason);
        await logEvent({ type:'EMAIL_BOUNCED', email:info.email, campaign:'bounce', url:info.reason });
      } else {
        console.log(`  → No email extracted — skipping`);
      }

      user.processedBounces.add(msg.id);
      await sleep(300);
    }
  } catch(e) { console.error(`Bounce check error ${userEmail}:`, e.message); }
}

async function writeBounceReason(sheetId, tab, email, reason) {
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId:sheetId, range:`${tab}!A1:Z500` });
    const rows = r.data.values || [];
    if (rows.length < 2) return;
    const h  = rows[0].map(h => (h||'').toLowerCase());
    const ec = h.findIndex(h => h.includes('email'));
    let   rc = h.findIndex(h => h.includes('bounce reason'));
    if (rc < 0) {
      rc = h.length;
      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${tab}!${toCol(rc+1)}1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [['Bounce Reason']] }
      });
    }
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][ec]||'').toLowerCase().trim() === email.toLowerCase()) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: sheetId,
          range: `${tab}!${toCol(rc+1)}${i+1}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[reason]] }
        });
        break;
      }
    }
  } catch(e) { console.error('writeBounceReason error:', e.message); }
}

function getEmailBody(payload) {
  let t = '';
  function ex(p) {
    if (p?.body?.data) {
      try { t += Buffer.from(p.body.data, 'base64').toString('utf-8') + ' '; } catch(e) {}
    }
    if (p?.parts) p.parts.forEach(ex);
  }
  ex(payload);
  return t.slice(0, 8000); // Enough context
}

function parseBounce(text, headers = [], senderEmail = '') {
  const b = text.toLowerCase();

  // ── 1. Try DSN headers (most reliable source) ─────────────
  let email = '';
  // Always skip sender email + common system addresses
  const senderLower = (senderEmail || '').toLowerCase();
  const SKIP = ['mailer-daemon','postmaster','noreply','no-reply','bounce','return','daemon'];

  function isValidRecipient(addr) {
    const a = addr.toLowerCase().trim();
    if (!a || !a.includes('@')) return false;
    if (SKIP.some(s => a.includes(s))) return false;
    if (senderLower && a === senderLower) return false; // ✅ Exclude sender!
    return true;
  }

  // ── 1. DSN Headers first (most reliable) ──────────────────
  // X-Failed-Recipients is the BEST source — explicitly lists failed address
  const priorityHeaders = ['x-failed-recipients', 'x-original-to', 'final-recipient', 'original-recipient'];
  for (const hdr of headers) {
    if (priorityHeaders.includes(hdr.name?.toLowerCase())) {
      const m = hdr.value?.match(/([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i);
      if (m?.[1] && isValidRecipient(m[1])) {
        email = m[1].toLowerCase();
        console.log(`  → Email from header "${hdr.name}": ${email}`);
        break;
      }
    }
  }

  // ── 2. Body DSN patterns (second most reliable) ───────────
  if (!email) {
    const bodyPatterns = [
      /final-recipient:\s*rfc822;\s*([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i,
      /original-recipient:\s*rfc822;\s*([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i,
      /x-failed-recipients:\s*([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i,
      /(?:the following address(?:es)? failed|failed recipient(?:s)?):[^@\n]*([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i,
      /(?:delivery to the following|failed to deliver to|undeliverable to|could not deliver to)\s+<?([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})>?/i,
      /(?:original message recipient|to):\s+<?([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})>?/i,
    ];
    for (const p of bodyPatterns) {
      const m = text.match(p);
      if (m?.[1] && isValidRecipient(m[1])) {
        email = m[1].toLowerCase();
        console.log(`  → Email from body pattern: ${email}`);
        break;
      }
    }
  }

  // ── 3. Last resort — any email in body that's not sender ──
  if (!email) {
    const allEmails = [...text.matchAll(/([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/gi)];
    for (const m of allEmails) {
      if (isValidRecipient(m[1])) {
        email = m[1].toLowerCase();
        console.log(`  → Email from fallback scan: ${email}`);
        break;
      }
    }
  }

  // ── 3. SMTP + enhanced status codes ───────────────────────
  const smtpMatch = b.match(/(4\d\d|5\d\d)/);
  const smtpCode  = smtpMatch ? parseInt(smtpMatch[1]) : 0;
  const enhMatch  = b.match(/([45]\.\d\.\d+)/);
  const enhCode   = enhMatch ? enhMatch[1] : '';

  // ── 4. Reason detection ───────────────────────────────────
  let reason = 'Delivery Failed';

  if (
    b.includes('user unknown') || b.includes('no such user') ||
    b.includes('does not exist') || b.includes('invalid address') ||
    b.includes('address rejected') || b.includes('invalid recipient') ||
    b.includes('recipient address rejected') || b.includes('no mailbox') ||
    b.includes('unknown address') || b.includes('unknown user') ||
    b.includes('address not found') || b.includes('bad destination') ||
    b.includes('not a valid') || b.includes('email address does not exist') ||
    ['5.1.1','5.1.2','5.1.3'].includes(enhCode)
  ) reason = 'Invalid Address';

  else if (
    b.includes('mailbox full') || b.includes('over quota') ||
    b.includes('quota exceeded') || b.includes('storage full') ||
    b.includes('insufficient storage') || b.includes('mailbox size limit') ||
    ['4.2.2','5.2.2'].includes(enhCode)
  ) reason = 'Mailbox Full';

  else if (
    b.includes('account disabled') || b.includes('account suspended') ||
    b.includes('account closed') || b.includes('no longer active') ||
    b.includes('deactivated') || b.includes('account inactive') ||
    b.includes('disabled account') || enhCode === '5.1.6'
  ) reason = 'Account Disabled';

  else if (
    b.includes('domain not found') || b.includes('no such domain') ||
    b.includes('host not found') || b.includes('name or service not known') ||
    b.includes('could not find host') || b.includes('domain does not exist') ||
    b.includes('no mx') || b.includes('dns lookup') ||
    ['5.4.4','5.1.2'].includes(enhCode)
  ) reason = 'Domain Not Found';

  else if (
    b.includes('spam') || b.includes('spamhaus') || b.includes('spamcop') ||
    b.includes('bulk mail') || b.includes('content policy') ||
    b.includes('policy violation') || b.includes('message considered spam') ||
    ['5.7.1','5.7.9'].includes(enhCode)
  ) reason = 'Message Blocked (Spam)';

  else if (
    b.includes('blacklist') || b.includes('blocklist') ||
    b.includes('sender blocked') || b.includes('ip blocked') ||
    b.includes('sender reputation') || b.includes('dmarc') ||
    b.includes('spf') || b.includes('dkim') ||
    ['5.7.0','5.7.26','5.7.25'].includes(enhCode)
  ) reason = 'Sender Blocked';

  else if (
    b.includes('message rejected') || b.includes('permanently rejected') ||
    b.includes('transaction failed') || b.includes('rejected by') ||
    smtpCode === 550 || smtpCode === 554 || smtpCode === 551 || smtpCode === 553
  ) reason = 'Message Rejected';

  else if (
    b.includes('relay') || b.includes('not permitted') || b.includes('not allowed to relay')
  ) reason = 'Relay Denied';

  else if (smtpCode >= 400 && smtpCode < 500) {
    reason = 'Temporary Failure (Will Retry)';
  }

  return { email, reason, smtpCode, enhCode };
}

async function checkAllBounces() {
  const users = Object.keys(userStore);
  if (!users.length) return;
  console.log(`🔍 Bounce check for ${users.length} users`);
  lastBounceCheck = Date.now();
  for (const u of users) {
    await checkBouncesForUser(u);
    await sleep(500);
  }
}

//  ROUTES
// ═══════════════════════════════════════════════════════════
app.get('/ping', async (req,res) => {
  res.json({status:'alive',time:new Date(),users:Object.keys(userStore).length});
  checkAllBounces().catch(()=>{});
});

// 🔍 Manual bounce check endpoint — for testing
app.get('/bounce-check', async (req,res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).json({error:'Unauthorized'});
  const users = Object.keys(userStore);
  res.json({ message:`Checking ${users.length} users...`, users });
  for (const u of users) {
    await checkBouncesForUser(u);
    await sleep(500);
  }
});

app.get('/track/open', async (req,res) => {
  res.set({'Content-Type':'image/gif','Cache-Control':'no-cache,no-store'});res.send(PIXEL);
  const {email,campaign,sheetId,tab}=req.query; if(!email)return;
  if(!trackingStore[email.toLowerCase()])trackingStore[email.toLowerCase()]={};
  trackingStore[email.toLowerCase()].opened=true;
  updateStatus(sheetId,tab,email,'EMAIL_OPENED').catch(()=>{});
  logEvent({type:'EMAIL_OPENED',email,campaign}).catch(()=>{});
});

app.get('/track/click', async (req,res) => {
  const {email,campaign,url,sheetId,tab}=req.query;
  res.redirect(url?decodeURIComponent(url):'https://google.com');
  if(!email)return;
  if(!trackingStore[email.toLowerCase()])trackingStore[email.toLowerCase()]={};
  trackingStore[email.toLowerCase()].clicked=true;
  updateStatus(sheetId,tab,email,'EMAIL_CLICKED').catch(()=>{});
  logEvent({type:'EMAIL_CLICKED',email,campaign,url}).catch(()=>{});
});

app.get('/unsubscribe', async (req,res) => {
  const {email,campaign,sheetId,tab}=req.query;
  console.log(`🚫 UNSUBSCRIBE: ${email}`);
  await updateStatus(sheetId,tab,email,'UNSUBSCRIBED');
  await logEvent({type:'UNSUBSCRIBED',email,campaign});
  res.send(`<!DOCTYPE html><html><head><title>Unsubscribed</title><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;display:flex;align-items:center;justify-content:center;min-height:100vh}.card{background:white;border-radius:16px;padding:48px;text-align:center;max-width:420px;box-shadow:0 4px 24px rgba(0,0,0,.08)}.icon{font-size:52px;margin-bottom:16px}h1{font-size:22px;color:#202124;margin-bottom:10px}p{font-size:14px;color:#5f6368;line-height:1.7}.em{font-weight:600;color:#1a73e8}</style></head><body><div class="card"><div class="icon">✅</div><h1>Successfully Unsubscribed</h1><p>The address <span class="em">${email||'your email'}</span> has been removed.</p></div></body></html>`);
});

app.get('/check',(req,res)=>{const k=(req.query.email||'').toLowerCase();const d=trackingStore[k]||{};res.json({opened:!!d.opened,clicked:!!d.clicked});});

// ✅ Save refresh token
app.post('/auth/save-token', async (req,res) => {
  const {code,redirectUri,userEmail,accessToken,sheetId,sheetTab}=req.body;
  if (!code||!userEmail) return res.status(400).json({error:'Missing fields'});
  console.log(`🔐 Saving token for ${userEmail}`);
  try {
    const tr=await fetch('https://oauth2.googleapis.com/token',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code,client_id:process.env.OAUTH_CLIENT_ID,client_secret:process.env.OAUTH_CLIENT_SECRET,redirect_uri:redirectUri,grant_type:'authorization_code'})});
    const tokens=await tr.json();
    console.log(`Token status: ${tr.status} | refresh_token: ${!!tokens.refresh_token}`);
    userStore[userEmail]={refreshToken:tokens.refresh_token||null,accessToken:tokens.access_token||accessToken,sheetId:sheetId||'',sheetTab:sheetTab||'Sheet1',savedAt:new Date().toISOString()};
    saveUserStore(); // ✅ Write to disk immediately!
    console.log(`✅ ${userEmail} saved! has refresh: ${!!tokens.refresh_token}`);
    res.json({success:true,hasRefreshToken:!!tokens.refresh_token});
  } catch(e){console.error('save-token:',e.message);res.status(500).json({error:e.message});}
});

// ✅ Auto restore session — called every time extension opens
// No sign out/in needed — just refreshes access token in memory!
app.post('/auth/restore-session', async (req, res) => {
  const { userEmail, accessToken, sheetId, sheetTab } = req.body;
  if (!userEmail) return res.status(400).json({ error: 'Missing userEmail' });

  if (userStore[userEmail]) {
    // User exists — just update access token + sheet info
    userStore[userEmail].accessToken = accessToken;
    if (sheetId)  userStore[userEmail].sheetId  = sheetId;
    if (sheetTab) userStore[userEmail].sheetTab = sheetTab;
    saveUserStore();
    console.log(`🔄 Session restored for ${userEmail}`);
    return res.json({ restored: true, hasRefreshToken: !!userStore[userEmail].refreshToken });
  } else {
    // User not in store (fresh server start + no file) — need refresh token
    // Save access token at least so bounce check works for now
    userStore[userEmail] = {
      refreshToken: null,
      accessToken,
      sheetId:  sheetId  || '',
      sheetTab: sheetTab || 'Sheet1',
      savedAt:  new Date().toISOString(),
    };
    saveUserStore();
    console.log(`⚠️ New session for ${userEmail} — no refresh token yet`);
    return res.json({ restored: false, needsReauth: true });
  }
});

app.post('/auth/update-sheet',(req,res)=>{
  const{userEmail,sheetId,sheetTab}=req.body;
  if(userEmail){
    if(!userStore[userEmail])userStore[userEmail]={};
    if(sheetId)userStore[userEmail].sheetId=sheetId;
    if(sheetTab)userStore[userEmail].sheetTab=sheetTab;
    saveUserStore(); // ✅ Persist to disk
    console.log(`📋 Sheet updated for ${userEmail}: ${sheetId}/${sheetTab}`);
  }
  res.json({success:true});
});

// ✅ SCHEDULING — 24/7 even Chrome closed!
app.post('/schedule', async (req,res) => {
  try {
    const{scheduleId,scheduledAt,recipients,draftSubject,draftHtml,sender,sheetId,sheetTab,userEmail,token}=req.body;
    if(!scheduledAt||!recipients||!draftHtml)return res.status(400).json({error:'Missing fields'});
    const delay=new Date(scheduledAt).getTime()-Date.now();
    if(delay<0)return res.status(400).json({error:'Past time'});
    const id=scheduleId||('job_'+Date.now());
    scheduleStore[id]={status:'pending',scheduledAt,recipients,sender,sheetId,sheetTab,userEmail};
    if(userEmail&&token&&userStore[userEmail])userStore[userEmail].accessToken=token;
    else if(userEmail&&token)userStore[userEmail]={accessToken:token,sheetId,sheetTab:sheetTab||'Sheet1'};
    console.log(`📅 Job ${id} — ${recipients.length} emails at ${scheduledAt} for ${userEmail}`);

    setTimeout(async()=>{
      console.log(`🚀 RUNNING JOB ${id}`);
      scheduleStore[id].status='running';
      let sent=0;

      // Get fresh token via refresh token!
      let activeToken=token;
      if(userEmail){
        const fresh=await getAccessToken(userEmail);
        if(fresh){activeToken=fresh;console.log(`✅ Fresh token for ${userEmail}`);}
        else console.log(`⚠️ No refresh token — using original`);
      }
      if(!activeToken){scheduleStore[id].status='failed';console.log('❌ No token!');return;}

      const serverUrl='https://mail-merge-tracker.onrender.com';
      const camp=encodeURIComponent((draftSubject||'sched')+'_'+Date.now());
      const sid=encodeURIComponent(sheetId||'');
      const stab=encodeURIComponent(sheetTab||'Sheet1');

      function wrapHtml(raw){
        if(!raw.toLowerCase().includes('<html'))return`<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>${raw}</body></html>`;
        if(!raw.toLowerCase().includes('</body>'))return raw+'</body></html>';
        return raw;
      }

      for(const r of recipients){
        try{
          let html=(draftHtml||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);
          let subject=(draftSubject||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);
          html=wrapHtml(html);
          const enc=encodeURIComponent(r.email);
          html=html.replace('</body>',`<img src="${serverUrl}/track/open?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}" width="1" height="1" style="display:none" alt=""/></body>`);
          html=html.replace(/href="(https?:\/\/[^"]+)"/gi,(m,orig)=>orig.includes(serverUrl)?m:`href="${serverUrl}/track/click?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}&url=${encodeURIComponent(orig)}"`);
          const unsubUrl=`${serverUrl}/unsubscribe?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}`;
          html=html.replace('</body>',`<div style="margin-top:28px;padding-top:14px;border-top:1px solid #e8eaed;text-align:center;font-family:Arial,sans-serif;font-size:11px;color:#9aa0a6;"><a href="${unsubUrl}" style="color:#9aa0a6;">Unsubscribe</a></div></body>`);
          const boundary='mm_'+Math.random().toString(36).slice(2);
          const raw=[`From: ${sender}`,`To: ${r.email}`,`Subject: ${subject}`,`MIME-Version: 1.0`,`Content-Type: multipart/alternative; boundary="${boundary}"`,``,`--${boundary}`,`Content-Type: text/html; charset=UTF-8`,``,html,``,`--${boundary}--`].join('\r\n');
          const encoded=Buffer.from(raw).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
          const sr=await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send',{method:'POST',headers:{Authorization:`Bearer ${activeToken}`,'Content-Type':'application/json'},body:JSON.stringify({raw:encoded})});
          console.log(`📤 ${r.email}: ${sr.status}`);
          if(sr.ok){sent++;if(sheetId)await updateStatusUserToken(activeToken,sheetId,sheetTab||'Sheet1',r.email,'EMAIL_SENT');}
          else{const e=await sr.text();console.log(`❌ ${e.slice(0,100)}`);}
        }catch(e){console.error(`Error ${r.email}:`,e.message);}
        await sleep(400);
      }
      scheduleStore[id].status='done';scheduleStore[id].sent=sent;
      console.log(`✅ Job ${id} DONE: ${sent}/${recipients.length}`);
    },delay);

    res.json({success:true,id,message:`Scheduled! ${recipients.length} emails at ${new Date(scheduledAt).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})} IST`});
  }catch(e){console.error('schedule error:',e.message);res.status(500).json({error:e.message});}
});

app.get('/schedule/:id',(req,res)=>{const j=scheduleStore[req.params.id];if(!j)return res.status(404).json({error:'Not found'});res.json({status:j.status,sent:j.sent,total:j.recipients?.length});});

// 📊 Dashboard
app.get('/dashboard', async (req,res) => {
  if(req.query.key!==process.env.DASHBOARD_KEY)return res.status(401).send('Add ?key=YOUR_PASSWORD');
  try{
    const sheets=await getSheets();
    const r=await sheets.spreadsheets.values.get({spreadsheetId:process.env.TRACKING_SHEET_ID,range:'Tracking!A:F'});
    const rows=(r.data.values||[]).slice(1).reverse();
    const opens=rows.filter(r=>r[1]==='EMAIL_OPENED').length;
    const clicks=rows.filter(r=>r[1]==='EMAIL_CLICKED').length;
    const unsubs=rows.filter(r=>r[1]==='UNSUBSCRIBED').length;
    const bounces=rows.filter(r=>r[1]==='EMAIL_BOUNCED').length;
    const BADGE={EMAIL_OPENED:'background:#e8f5e9;color:#2e7d32',EMAIL_CLICKED:'background:#1b5e20;color:white',UNSUBSCRIBED:'background:#fce8e6;color:#d93025',EMAIL_BOUNCED:'background:#fff3e0;color:#e65100',EMAIL_SENT:'background:#f1f3f4;color:#5f6368'};
    res.send(`<!DOCTYPE html><html><head><title>EduJunior Dashboard</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;padding:24px}h1{font-size:22px;color:#202124;margin-bottom:4px}.sub{font-size:12px;color:#5f6368;margin-bottom:20px}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}.stat{background:white;border-radius:12px;padding:18px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)}.n{font-size:38px;font-weight:700;line-height:1}.l{font-size:11px;color:#5f6368;margin-top:6px}.o .n{color:#2e7d32}.c .n{color:#1b5e20}.b .n{color:#e65100}.u .n{color:#d93025}table{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}th{background:#1a73e8;color:white;padding:10px 14px;text-align:left;font-size:12px}td{padding:8px 14px;font-size:12px;border-bottom:1px solid #f1f3f4}.badge{display:inline-block;padding:3px 9px;border-radius:10px;font-size:11px;font-weight:600}</style></head><body>
    <h1>📊 EduJunior Mail Merge</h1><div class="sub">Users: ${Object.keys(userStore).length} | Jobs: ${Object.keys(scheduleStore).length}</div>
    <div class="stats"><div class="stat o"><div class="n">${opens}</div><div class="l">📬 Opened</div></div><div class="stat c"><div class="n">${clicks}</div><div class="l">🖱️ Clicked</div></div><div class="stat b"><div class="n">${bounces}</div><div class="l">🔴 Bounced</div></div><div class="stat u"><div class="n">${unsubs}</div><div class="l">🚫 Unsub</div></div></div>
    <table><tr><th>Time</th><th>Event</th><th>Email</th><th>Campaign</th></tr>${rows.slice(0,150).map(r=>{const st=BADGE[r[1]]||'background:#f1f3f4;color:#5f6368';return`<tr><td>${r[0]||''}</td><td><span class="badge" style="${st}">${r[1]||''}</span></td><td>${r[2]||''}</td><td>${r[3]||''}</td></tr>`;}).join('')}</table></body></html>`);
  }catch(e){res.status(500).send('Error: '+e.message);}
});

app.get('/',(req,res)=>res.json({status:'✅ EduJunior v5',users:Object.keys(userStore).length,time:new Date()}));
const PORT=process.env.PORT||3000;
app.listen(PORT,()=>console.log(`✅ EduJunior Tracker v5 on port ${PORT}`));
