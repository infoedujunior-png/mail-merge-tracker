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
let scheduleStore   = {};
let lastBounceCheck = 0;

// ══════════════════════════════════════════════════════════
//  RESTORE PENDING JOBS after restart
// ══════════════════════════════════════════════════════════
function restorePendingJobs() {
  const jobs = Object.values(scheduleStore).filter(j => j.status === 'pending');
  if (!jobs.length) { console.log('📅 No pending jobs to restore'); return; }
  console.log(`📅 Restoring ${jobs.length} pending jobs...`);

  for (const job of jobs) {
    const delay = new Date(job.scheduledAt).getTime() - Date.now();

    if (delay < -5 * 60 * 1000) {
      console.log(`⚡ Overdue job ${job.id} — running immediately`);
      scheduleStore[job.id].status = 'running';
      saveScheduleStore();
      executeScheduledJob(job).then(sent => {
        scheduleStore[job.id].status = 'done';
        scheduleStore[job.id].sent = sent;
        if (job.userEmail) addEmailCount(job.userEmail, sent);
        saveScheduleStore();
        console.log(`✅ Overdue job ${job.id} done: ${sent} sent`);
      }).catch(e => {
        scheduleStore[job.id].status = 'failed';
        scheduleStore[job.id].error = e.message;
        saveScheduleStore();
        console.log(`❌ Overdue job ${job.id} failed: ${e.message}`);
      });
    } else if (delay <= 0) {
      setImmediate(() => fireJob(job.id));
    } else {
      console.log(`⏰ Re-scheduling job ${job.id} in ${Math.round(delay/60000)} min`);
      setTimeout(() => fireJob(job.id), delay);
    }
  }
}

async function fireJob(id) {
  const job = scheduleStore[id];
  if (!job || job.status !== 'pending') return;
  console.log(`🚀 FIRING JOB ${id}`);
  scheduleStore[id].status = 'running';
  saveScheduleStore();
  try {
    const sent = await executeScheduledJob(job);
    scheduleStore[id].status = 'done';
    scheduleStore[id].sent   = sent;
    if (job.userEmail) addEmailCount(job.userEmail, sent);
    saveScheduleStore();
    console.log(`✅ Job ${id} done: ${sent}/${job.recipients?.length} sent`);
  } catch(e) {
    scheduleStore[id].status = 'failed';
    scheduleStore[id].error  = e.message;
    saveScheduleStore();
    console.error(`❌ Job ${id} failed: ${e.message}`);
  }
}

// ══════════════════════════════════════════════════════════
//  MONETIZATION SYSTEM
// ══════════════════════════════════════════════════════════
const PLANS = {
  free:   { dailyLimit: 25,  name: 'Free',       price: 0    },
  paid:   { dailyLimit: 400, name: 'Pro',         price: 299  },
  intern: { dailyLimit: 400, name: 'Team (Free)', price: 0    },
};

const PLANS_FILE = path.join('/tmp', 'edujunior_plans.json');
let planStore = {};

function loadPlanStore() {
  try {
    if (fs.existsSync(PLANS_FILE)) {
      const d = JSON.parse(fs.readFileSync(PLANS_FILE, 'utf8'));
      console.log(`📂 Loaded ${Object.keys(d).length} user plans from disk`);
      return d;
    }
  } catch(e) { console.error('Load plans error:', e.message); }
  return {};
}

function savePlanStore() {
  try {
    fs.writeFileSync(PLANS_FILE, JSON.stringify(planStore, null, 2));
  } catch(e) { console.error('Save plans error:', e.message); }
}

planStore = loadPlanStore();

const dailyCountFile = path.join('/tmp', 'edujunior_counts.json');
let dailyCounts = {};

function loadDailyCounts() {
  try {
    if (fs.existsSync(dailyCountFile)) return JSON.parse(fs.readFileSync(dailyCountFile, 'utf8'));
  } catch(e) {}
  return {};
}
function saveDailyCounts() {
  try { fs.writeFileSync(dailyCountFile, JSON.stringify(dailyCounts, null, 2)); } catch(e) {}
}
dailyCounts = loadDailyCounts();

function getTodayIST() {
  return new Date().toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' });
}

function getUserPlan(email) {
  const e = (email || '').toLowerCase();
  const p = planStore[e];
  if (!p) return 'free';
  if (p.plan === 'paid' && p.expiresAt && new Date(p.expiresAt) < new Date()) return 'free';
  return p.plan || 'free';
}

function getDailyUsage(email) {
  const e = (email || '').toLowerCase();
  const today = getTodayIST();
  if (!dailyCounts[e] || dailyCounts[e].date !== today) {
    dailyCounts[e] = { count: 0, date: today };
  }
  return dailyCounts[e].count;
}

function getRemainingEmails(email) {
  const plan    = getUserPlan(email);
  const limit   = PLANS[plan]?.dailyLimit || 25;
  const used    = getDailyUsage(email);
  return Math.max(0, limit - used);
}

function addEmailCount(email, count = 1) {
  const e = (email || '').toLowerCase();
  const today = getTodayIST();
  if (!dailyCounts[e] || dailyCounts[e].date !== today) {
    dailyCounts[e] = { count: 0, date: today };
  }
  dailyCounts[e].count += count;
  saveDailyCounts();
}

// ══════════════════════════════════════════════════════════
//  PERSISTENT TOKEN STORAGE
//  ✅ FIX 1: tokenSavedAt bhi save hoga — no re-signin!
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
    const toSave = {};
    for (const [email, user] of Object.entries(userStore)) {
      toSave[email] = {
        refreshToken: user.refreshToken,
        accessToken:  user.accessToken,
        // ✅ FIX 1: tokenSavedAt save karo — warna restart pe sab expire dikh ta tha
        tokenSavedAt: user.tokenSavedAt || 0,
        sheetId:      user.sheetId,
        sheetTab:     user.sheetTab,
        savedAt:      user.savedAt,
      };
    }
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(toSave, null, 2));
    console.log(`💾 Saved ${Object.keys(toSave).length} users to disk`);
  } catch(e) { console.error('Save tokens error:', e.message); }
}

const userStore = loadUserStore();

// ══════════════════════════════════════════════════════════
//  PERSISTENT SCHEDULE STORAGE
// ══════════════════════════════════════════════════════════
const SCHEDULE_FILE = path.join('/tmp', 'edujunior_schedule.json');

function loadScheduleStore() {
  try {
    if (fs.existsSync(SCHEDULE_FILE)) {
      const data = JSON.parse(fs.readFileSync(SCHEDULE_FILE, 'utf8'));
      const pending = Object.values(data).filter(j => j.status === 'pending');
      console.log(`📂 Loaded ${Object.keys(data).length} jobs (${pending.length} pending)`);
      return data;
    }
  } catch(e) { console.error('Load schedule error:', e.message); }
  return {};
}

function saveScheduleStore() {
  try {
    fs.writeFileSync(SCHEDULE_FILE, JSON.stringify(scheduleStore, null, 2));
  } catch(e) { console.error('Save schedule error:', e.message); }
}

// ── Service Account Sheets ─────────────────────────────────
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ✅ FIX 1 (cont): getAccessToken — refresh token pe poora rely karo
// Agar refresh token hai → hamesha fresh token lo (restart ke baad bhi)
// Agar sirf access token hai → tokenSavedAt se age check karo (ab save hota hai disk pe)
async function getAccessToken(userEmail) {
  const user = userStore[userEmail];
  if (!user) { console.log(`❌ No user record for ${userEmail}`); return null; }

  if (user.refreshToken) {
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
        user.accessToken  = data.access_token;
        user.tokenSavedAt = Date.now();
        saveUserStore(); // ✅ Fresh token disk pe save hoga
        console.log(`🔑 Fresh token for ${userEmail}`);
        return data.access_token;
      }
      console.error(`❌ Refresh failed for ${userEmail}: ${data.error}`);
    } catch(e) { console.error(`❌ Refresh error for ${userEmail}:`, e.message); }
  }

  // No refresh token — check if access token is still fresh (< 50 min old)
  if (user.accessToken) {
    const age = Date.now() - (user.tokenSavedAt || 0);
    const ageMin = Math.round(age / 60000);
    if (age < 50 * 60 * 1000) {
      console.log(`⚡ Using access token for ${userEmail} (${ageMin} min old)`);
      return user.accessToken;
    }
    console.log(`⚠️ Token expired for ${userEmail} (${ageMin} min old) — need sign in`);
    return null;
  }

  console.log(`❌ No token for ${userEmail}`);
  return null;
}

const STATUS_COLORS = {
  SCHEDULED:     { red:0.68, green:0.85, blue:0.90 },
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

    let gid = 0;
    try {
      const meta = await sheets.spreadsheets.get({ spreadsheetId:sheetId, fields:'sheets.properties' });
      const sheetObj = (meta.data.sheets||[]).find(s => s.properties.title === sheetTab);
      gid = sheetObj?.properties?.sheetId ?? 0;
    } catch(e) { console.log('GID fetch error:', e.message); }

    const color  = STATUS_COLORS[newStatus];
    const isBold = ['EMAIL_CLICKED','EMAIL_BOUNCED'].includes(newStatus);
    const nowStr = new Date().toLocaleString('en-IN', {
      timeZone:'Asia/Kolkata', day:'2-digit', month:'short',
      year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true
    });
    const noteMap = { EMAIL_SENT:'Sent', EMAIL_OPENED:'Opened', EMAIL_CLICKED:'Clicked', EMAIL_BOUNCED:'Bounced', UNSUBSCRIBED:'Unsubscribed' };
    const noteText = (noteMap[newStatus]||newStatus) + ' on: ' + nowStr;

    const cellValue = { note: noteText };
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

    try {
      const nowStr  = new Date().toLocaleString('en-IN', { timeZone:'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true });
      const noteMap = { EMAIL_SENT:'Sent', EMAIL_OPENED:'Opened', EMAIL_CLICKED:'Clicked', EMAIL_BOUNCED:'Bounced', UNSUBSCRIBED:'Unsubscribed' };
      const noteText = (noteMap[status]||status) + ' on: ' + nowStr;
      const color    = STATUS_COLORS[status];
      const isBold   = ['EMAIL_CLICKED','EMAIL_BOUNCED'].includes(status);

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
//  SERVER-SIDE BOUNCE DETECTION
// ═══════════════════════════════════════════════════════════

setInterval(() => {
  checkAllBounces().catch(e => console.error('Interval bounce error:', e.message));
}, 2 * 60 * 1000);

async function checkBouncesForUser(userEmail) {
  const user = userStore[userEmail];
  if (!user?.sheetId) { console.log(`⏭️ Skip ${userEmail} — no sheetId`); return; }

  const token = await getAccessToken(userEmail);
  if (!token) { console.log(`⏭️ Skip ${userEmail} — no token`); return; }

  if (!user.processedBounces) user.processedBounces = new Set();
  if (!user.bouncesResetAt || Date.now() - user.bouncesResetAt > 2*60*60*1000) {
    user.processedBounces = new Set();
    user.bouncesResetAt   = Date.now();
    console.log(`🔄 Reset bounce cache for ${userEmail}`);
  }

  try {
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

      const looksLikeBounce =
        /mailer.daemon|postmaster|mail.*delivery|delivery.*fail|undeliver|bounce|failure notice|returned mail|address not found|user unknown|rejected|could not deliver/i.test(subject + fromHdr);

      console.log(`  → Subject: "${subject.slice(0,70)}" | Bounce: ${looksLikeBounce}`);

      if (!looksLikeBounce) {
        user.processedBounces.add(msg.id);
        continue;
      }

      const info = parseBounce(full, hdrs, userEmail, md.payload);
      console.log(`  → Parsed: email="${info.email}" reason="${info.reason}" smtp=${info.smtpCode} enh=${info.enhCode}`);

      if (info.email) {
        console.log(`🔴 BOUNCE: ${info.email} — ${info.reason}`);
        await updateBounceWithUserToken(token, user.sheetId, user.sheetTab || 'Sheet1', info.email, info.reason);
        await logEvent({ type:'EMAIL_BOUNCED', email:info.email, campaign:'bounce', url:info.reason });
      } else {
        console.log(`  → No email extracted — skipping`);
      }

      user.processedBounces.add(msg.id);
      await sleep(300);
    }
  } catch(e) { console.error(`Bounce check error ${userEmail}:`, e.message); }
}

// ✅ FIX 2: Bounce reason sirf NOTE mein — koi alag column nahi!
// Merge Status cell ka note: "Bounced on: DD Mon YYYY HH:MM AM\nReason: Invalid Address"
async function updateBounceWithUserToken(token, sheetId, tab, email, reason) {
  try {
    const sheetTab = tab || 'Sheet1';

    const res = await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab + '!A1:Z500')}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (!res.ok) { console.error(`❌ Bounce sheet read failed: ${res.status}`); return; }

    const data    = await res.json();
    const rows    = data.values || [];
    if (rows.length < 2) return;

    const headers  = rows[0].map(h => (h||'').toLowerCase().trim());
    const emailCol = headers.findIndex(h => h.includes('email'));
    if (emailCol < 0) { console.error('❌ No email column'); return; }

    let statCol = headers.findIndex(h => h.includes('merge status') || h === 'status');
    if (statCol < 0) {
      statCol = headers.length;
      await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab + '!' + toCol(statCol+1) + '1')}?valueInputOption=USER_ENTERED`,
        { method:'PUT', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
          body: JSON.stringify({ values:[['Merge Status']] }) }
      );
    }

    // Find row
    let rowNum = -1;
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][emailCol]||'').toLowerCase().trim() === email.toLowerCase().trim()) {
        rowNum = i + 1; break;
      }
    }
    if (rowNum < 0) { console.log(`❌ Bounce: ${email} not found in sheet`); return; }

    // Priority check
    const P   = { SCHEDULED:0, EMAIL_SENT:1, EMAIL_OPENED:2, EMAIL_CLICKED:3, EMAIL_BOUNCED:4, UNSUBSCRIBED:5 };
    const cur = ((rows[rowNum-1]||[])[statCol]||'').toUpperCase().trim();
    if ((P[cur]||0) >= (P['EMAIL_BOUNCED']||0)) {
      console.log(`⏭️ Skip bounce — current status: ${cur}`);
      return;
    }

    const nowStr = new Date().toLocaleString('en-IN', {
      timeZone:'Asia/Kolkata', day:'2-digit', month:'short',
      year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true
    });

    // Write EMAIL_BOUNCED status value
    await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetTab + '!' + toCol(statCol+1) + rowNum)}?valueInputOption=USER_ENTERED`,
      { method:'PUT', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
        body: JSON.stringify({ values:[['EMAIL_BOUNCED']] }) }
    );

    // ✅ FIX 2: Note mein bounce time + reason dono — koi alag column nahi banta!
    try {
      const metaRes = await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}?fields=sheets.properties`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const metaData = await metaRes.json();
      const sheetObj = (metaData.sheets||[]).find(s => s.properties.title === sheetTab);
      const gid      = sheetObj?.properties?.sheetId ?? 0;

      const color    = STATUS_COLORS['EMAIL_BOUNCED'];
      // ✅ Note mein reason bhi include — "Bounced on: ...\nReason: Invalid Address"
      const noteText = `Bounced on: ${nowStr}\nReason: ${reason}`;

      await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}:batchUpdate`,
        { method:'POST', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
          body: JSON.stringify({ requests:[{ updateCells:{
            range:{ sheetId:gid, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:statCol, endColumnIndex:statCol+1 },
            rows:[{ values:[{
              // ✅ Reason note mein, koi extra column nahi!
              note: noteText,
              userEnteredFormat:{ backgroundColor:color, textFormat:{ bold:true, foregroundColor:{red:1,green:1,blue:1} } }
            }]}],
            fields:'note,userEnteredFormat(backgroundColor,textFormat)'
          }}]})
        }
      );
      console.log(`✅ Bounce updated: ${email} → EMAIL_BOUNCED | Note: ${noteText}`);
    } catch(e) { console.error('Bounce color/note error:', e.message); }

  } catch(e) { console.error('updateBounceWithUserToken error:', e.message); }
}

function getEmailBody(payload) {
  let t = '';
  function ex(p) {
    if (p?.body?.data) {
      try { t += Buffer.from(p.body.data, 'base64').toString('utf-8') + '\n'; } catch(e) {}
    }
    if (p?.parts) p.parts.forEach(ex);
  }
  ex(payload);
  return t.slice(0, 10000);
}

function getDsnPart(payload) {
  let dsn = '';
  function ex(p) {
    if (p?.mimeType === 'message/delivery-status' && p?.body?.data) {
      try { dsn = Buffer.from(p.body.data, 'base64').toString('utf-8'); } catch(e) {}
    }
    if (p?.parts) p.parts.forEach(ex);
  }
  ex(payload);
  return dsn;
}

function parseBounce(text, headers = [], senderEmail = '', payload = null) {
  let email = '';
  const senderLower = (senderEmail || '').toLowerCase();

  function isValid(addr) {
    const a = (addr || '').toLowerCase().trim();
    if (!a || !a.includes('@') || !a.includes('.')) return false;
    if (a === senderLower) return false;
    if (/mailer.daemon|postmaster|noreply|no.reply|bounce|return|daemon/i.test(a)) return false;
    return true;
  }

  const dsn = payload ? getDsnPart(payload) : '';
  if (dsn) {
    const finalMatch = dsn.match(/Final-Recipient:\s*rfc822;\s*([^\s\r\n]+)/i);
    if (finalMatch?.[1] && isValid(finalMatch[1])) {
      email = finalMatch[1].replace(/[<>]/g, '').toLowerCase();
      console.log(`  → DSN Final-Recipient: ${email}`);
    }
    if (!email) {
      const origMatch = dsn.match(/Original-Recipient:\s*rfc822;\s*([^\s\r\n]+)/i);
      if (origMatch?.[1] && isValid(origMatch[1])) {
        email = origMatch[1].replace(/[<>]/g, '').toLowerCase();
        console.log(`  → DSN Original-Recipient: ${email}`);
      }
    }
  }

  if (!email) {
    for (const hdr of headers) {
      const name = (hdr.name || '').toLowerCase();
      if (['x-failed-recipients','x-original-to'].includes(name)) {
        const m = hdr.value?.match(/([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/);
        if (m?.[1] && isValid(m[1])) {
          email = m[1].toLowerCase();
          console.log(`  → Header "${hdr.name}": ${email}`);
          break;
        }
      }
    }
  }

  if (!email) {
    const patterns = [
      /Final-Recipient:\s*rfc822;\s*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/i,
      /Original-Recipient:\s*rfc822;\s*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/i,
      /X-Failed-Recipients:\s*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/i,
      /(?:failed to deliver|could not deliver|undeliverable|delivery failed)[^@\n]{0,80}?([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/i,
      /(?:the following address|the following recipient)[^@\n]{0,80}?([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/i,
    ];
    for (const p of patterns) {
      const m = text.match(p);
      if (m?.[1] && isValid(m[1])) {
        email = m[1].toLowerCase();
        console.log(`  → Body pattern: ${email}`);
        break;
      }
    }
  }

  if (!email) {
    const all = [...text.matchAll(/([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/g)];
    for (const m of all) {
      if (isValid(m[1])) {
        email = m[1].toLowerCase();
        console.log(`  → Fallback scan: ${email}`);
        break;
      }
    }
  }

  const smtpMatch = text.match(/\b(5\d\d|4\d\d)\b/);
  const smtpCode  = smtpMatch ? parseInt(smtpMatch[1]) : 0;
  const enhMatch  = text.match(/\b([45]\.\d\.\d+)\b/);
  const enhCode   = enhMatch ? enhMatch[1] : '';

  let dsnStatus = '';
  if (dsn) {
    const sm = dsn.match(/Status:\s*([45]\.\d+\.\d+)/i);
    if (sm) dsnStatus = sm[1];
  }
  const effectiveEnh = dsnStatus || enhCode;

  const b = text.toLowerCase();
  let reason = 'Delivery Failed';

  if (['5.1.1','5.1.2','5.1.3'].includes(effectiveEnh) ||
      b.includes('user unknown') || b.includes('no such user') ||
      b.includes('does not exist') || b.includes('invalid address') ||
      b.includes('address rejected') || b.includes('unknown user') ||
      b.includes('no mailbox') || b.includes('recipient not found')) {
    reason = 'Invalid Address';
  } else if (['4.2.2','5.2.2'].includes(effectiveEnh) ||
      b.includes('mailbox full') || b.includes('over quota') ||
      b.includes('quota exceeded') || b.includes('storage full')) {
    reason = 'Mailbox Full';
  } else if (effectiveEnh === '5.1.6' ||
      b.includes('account disabled') || b.includes('account suspended') ||
      b.includes('no longer active') || b.includes('deactivated')) {
    reason = 'Account Disabled';
  } else if (['5.4.4'].includes(effectiveEnh) ||
      b.includes('domain not found') || b.includes('no such domain') ||
      b.includes('dns') || b.includes('host not found')) {
    reason = 'Domain Not Found';
  } else if (['5.7.1','5.7.9'].includes(effectiveEnh) ||
      b.includes('spam') || b.includes('policy violation') ||
      b.includes('content rejected')) {
    reason = 'Message Blocked (Spam)';
  } else if (['5.7.0','5.7.26'].includes(effectiveEnh) ||
      b.includes('dmarc') || b.includes('spf') || b.includes('dkim') ||
      b.includes('blacklist') || b.includes('sender blocked')) {
    reason = 'Sender Blocked';
  } else if (smtpCode === 550 || smtpCode === 554 || smtpCode === 551 ||
      b.includes('rejected') || b.includes('permanently rejected')) {
    reason = 'Message Rejected';
  } else if (smtpCode >= 400 && smtpCode < 500) {
    reason = 'Temporary Failure (Will Retry)';
  } else if (b.includes('relay') || b.includes('not permitted')) {
    reason = 'Relay Denied';
  }

  return { email, reason, smtpCode, enhCode: effectiveEnh };
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

// ═══════════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════════
app.get('/ping', async (req,res) => {
  res.json({status:'alive',time:new Date(),users:Object.keys(userStore).length});
  checkAllBounces().catch(()=>{});
});

app.get('/bounce-check', async (req,res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).json({error:'Unauthorized'});
  const users = Object.keys(userStore);
  res.json({ message:`Checking ${users.length} users...`, users });
  for (const u of users) {
    await checkBouncesForUser(u);
    await sleep(500);
  }
});

app.get('/bounce-reset', (req,res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).json({error:'Unauthorized'});
  const email = req.query.email;
  let count = 0;
  for (const [u, user] of Object.entries(userStore)) {
    if (email && u !== email.toLowerCase()) continue;
    user.processedBounces = new Set();
    user.bouncesResetAt   = 0;
    count++;
  }
  console.log(`🔄 Bounce cache reset for ${count} users`);
  res.json({ success:true, reset: count, message: `Cache cleared! Next bounce check will re-process all emails.` });
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

app.post('/auth/save-token', async (req,res) => {
  const {code,redirectUri,userEmail,accessToken,sheetId,sheetTab}=req.body;
  if (!code||!userEmail) return res.status(400).json({error:'Missing fields'});
  console.log(`🔐 Saving token for ${userEmail}`);
  try {
    const tr=await fetch('https://oauth2.googleapis.com/token',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code,client_id:process.env.OAUTH_CLIENT_ID,client_secret:process.env.OAUTH_CLIENT_SECRET,redirect_uri:redirectUri,grant_type:'authorization_code'})});
    const tokens=await tr.json();
    console.log(`Token status: ${tr.status} | refresh_token: ${!!tokens.refresh_token}`);
    userStore[userEmail]={
      refreshToken: tokens.refresh_token||null,
      accessToken:  tokens.access_token||accessToken,
      tokenSavedAt: Date.now(), // ✅ FIX 1: Initial tokenSavedAt set karo
      sheetId:      sheetId||'',
      sheetTab:     sheetTab||'Sheet1',
      savedAt:      new Date().toISOString()
    };
    saveUserStore();
    console.log(`✅ ${userEmail} saved! has refresh: ${!!tokens.refresh_token}`);
    res.json({success:true,hasRefreshToken:!!tokens.refresh_token});
  } catch(e){console.error('save-token:',e.message);res.status(500).json({error:e.message});}
});

app.post('/auth/restore-session', async (req, res) => {
  const { userEmail, accessToken, sheetId, sheetTab } = req.body;
  if (!userEmail) return res.status(400).json({ error: 'Missing userEmail' });

  if (userStore[userEmail]) {
    userStore[userEmail].accessToken  = accessToken;
    userStore[userEmail].tokenSavedAt = Date.now(); // ✅ FIX 1: Restore pe bhi time update
    if (sheetId)  userStore[userEmail].sheetId  = sheetId;
    if (sheetTab) userStore[userEmail].sheetTab = sheetTab;
    saveUserStore();
    console.log(`🔄 Session restored for ${userEmail} | sheetId: ${userStore[userEmail].sheetId || 'none'}`);
    checkBouncesForUser(userEmail).catch(e => console.error('Bounce after restore:', e.message));
    return res.json({ restored: true, hasRefreshToken: !!userStore[userEmail].refreshToken });
  } else {
    userStore[userEmail] = {
      refreshToken: null,
      accessToken,
      tokenSavedAt: Date.now(),
      sheetId:  sheetId  || '',
      sheetTab: sheetTab || 'Sheet1',
      savedAt:  new Date().toISOString(),
    };
    saveUserStore();
    console.log(`✅ Session created for ${userEmail}`);
    if (sheetId) checkBouncesForUser(userEmail).catch(e => console.error('Bounce on new session:', e.message));
    return res.json({ restored: true, hasRefreshToken: false });
  }
});

app.post('/auth/update-sheet',(req,res)=>{
  const{userEmail,sheetId,sheetTab}=req.body;
  if(userEmail){
    if(!userStore[userEmail])userStore[userEmail]={};
    if(sheetId)userStore[userEmail].sheetId=sheetId;
    if(sheetTab)userStore[userEmail].sheetTab=sheetTab;
    saveUserStore();
    console.log(`📋 Sheet updated for ${userEmail}: ${sheetId}/${sheetTab}`);
  }
  res.json({success:true});
});

app.post('/schedule', async (req,res) => {
  try {
    const {scheduleId,scheduledAt,recipients,draftSubject,draftHtml,
           sender,sheetId,sheetTab,userEmail,token} = req.body;
    if (!scheduledAt||!recipients||!draftHtml)
      return res.status(400).json({error:'Missing fields'});

    const delay = new Date(scheduledAt).getTime() - Date.now();
    if (delay < -60000) return res.status(400).json({error:'Past time'});

    const id = scheduleId || ('job_' + Date.now());

    if (userEmail && token) {
      if (!userStore[userEmail]) userStore[userEmail] = { sheetId, sheetTab: sheetTab||'Sheet1' };
      userStore[userEmail].accessToken  = token;
      userStore[userEmail].tokenSavedAt = Date.now(); // ✅ FIX 1
      saveUserStore();
    }

    scheduleStore[id] = {
      id, status:'pending', scheduledAt,
      recipients, draftSubject, draftHtml,
      sender, sheetId, sheetTab: sheetTab||'Sheet1',
      userEmail, createdAt: new Date().toISOString()
    };
    saveScheduleStore();

    console.log(`📅 Job ${id} saved — ${recipients.length} emails at ${scheduledAt}`);

    const actualDelay = Math.max(0, delay);
    setTimeout(() => fireJob(id), actualDelay);

    res.json({
      success:true, id,
      message:`Scheduled! ${recipients.length} emails at ${new Date(scheduledAt).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})} IST`
    });
  } catch(e) {
    console.error('schedule error:', e.message);
    res.status(500).json({error:e.message});
  }
});

app.get('/schedule/:id',(req,res)=>{const j=scheduleStore[req.params.id];if(!j)return res.status(404).json({error:'Not found'});res.json({status:j.status,sent:j.sent,total:j.recipients?.length});});

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

app.get('/user/status', (req, res) => {
  const email = (req.query.email || '').toLowerCase();
  if (!email) return res.status(400).json({ error: 'Missing email' });
  const plan           = getUserPlan(email);
  const limit          = PLANS[plan]?.dailyLimit || 25;
  const used           = getDailyUsage(email);
  const remaining      = Math.max(0, limit - used);
  const hasRefreshToken = !!(userStore[email]?.refreshToken);
  res.json({ plan, planName: PLANS[plan]?.name || 'Free', limit, used, remaining, email, hasRefreshToken });
});

app.post('/user/check-limit', (req, res) => {
  const { email, count } = req.body;
  if (!email) return res.status(400).json({ error: 'Missing email' });
  const remaining = getRemainingEmails(email);
  const plan      = getUserPlan(email);
  const limit     = PLANS[plan]?.dailyLimit || 25;
  if ((count || 1) > remaining) {
    return res.json({
      allowed: false, plan, limit, used: getDailyUsage(email), remaining,
      message: remaining === 0
        ? 'Daily limit reached! Upgrade to Pro for 400 emails/day.'
        : `Only ${remaining} emails left today.`
    });
  }
  res.json({ allowed: true, plan, limit, used: getDailyUsage(email), remaining });
});

app.post('/user/track-send', (req, res) => {
  const { email, count } = req.body;
  if (email) addEmailCount(email, count || 1);
  res.json({ success: true });
});

app.post('/payment/create-order', async (req, res) => {
  const { email, months } = req.body;
  if (!email) return res.status(400).json({ error: 'Missing email' });
  const m      = parseInt(months) || 1;
  const amount = PLANS.paid.price * m * 100;
  try {
    const response = await fetch('https://api.razorpay.com/v1/orders', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + Buffer.from(`${process.env.RAZORPAY_KEY_ID}:${process.env.RAZORPAY_KEY_SECRET}`).toString('base64'),
      },
      body: JSON.stringify({ amount, currency: 'INR', notes: { email, months: m } }),
    });
    const order = await response.json();
    if (!order.id) return res.status(500).json({ error: 'Order creation failed', details: order });
    console.log(`💳 Order created: ${order.id} for ${email} — ₹${amount/100}`);
    res.json({ success: true, orderId: order.id, amount, currency: 'INR', keyId: process.env.RAZORPAY_KEY_ID });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/payment/verify', async (req, res) => {
  const { email, orderId, paymentId, signature, months } = req.body;
  const crypto = require('crypto');
  const expected = crypto.createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
    .update(`${orderId}|${paymentId}`).digest('hex');
  if (expected !== signature) return res.status(400).json({ error: 'Invalid signature' });
  const m = parseInt(months) || 1;
  const expiresAt = new Date();
  expiresAt.setMonth(expiresAt.getMonth() + m);
  const e = email.toLowerCase();
  planStore[e] = { plan: 'paid', expiresAt: expiresAt.toISOString(), paymentId, orderId, activatedAt: new Date().toISOString() };
  savePlanStore();
  console.log(`✅ PAID: ${e} — Pro until ${expiresAt.toDateString()}`);
  res.json({ success: true, plan: 'paid', expiresAt: expiresAt.toISOString() });
});

function isAdmin(req) { return req.query.key === process.env.DASHBOARD_KEY || req.headers['x-admin-key'] === process.env.DASHBOARD_KEY; }

app.post('/admin/bulk-grant', (req, res) => {
  const { key, emails, plan, months } = req.body;
  if (key !== process.env.DASHBOARD_KEY) return res.status(401).json({ error: 'Unauthorized' });
  if (!emails || !Array.isArray(emails)) return res.status(400).json({ error: 'emails array required' });
  const p = plan || 'intern';
  let expiresAt = null;
  if (months) { const d = new Date(); d.setMonth(d.getMonth() + parseInt(months)); expiresAt = d.toISOString(); }
  const results = [];
  for (const email of emails) {
    const e = email.toLowerCase().trim();
    if (!e) continue;
    planStore[e] = { plan: p, expiresAt, addedBy: 'admin-bulk', addedAt: new Date().toISOString() };
    results.push(e);
  }
  savePlanStore();
  console.log(`✅ Bulk grant: ${results.length} users → ${p}`);
  res.json({ success: true, granted: results.length, plan: p, emails: results });
});

app.post('/admin/grant', (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { email, plan, months } = req.body;
  if (!email) return res.status(400).json({ error: 'Missing email' });
  const p = plan || 'intern';
  let expiresAt = null;
  if (months) { const d = new Date(); d.setMonth(d.getMonth() + months); expiresAt = d.toISOString(); }
  const e = email.toLowerCase().trim();
  planStore[e] = { plan: p, expiresAt, addedBy: 'admin', addedAt: new Date().toISOString() };
  savePlanStore();
  console.log(`✅ ADMIN GRANT: ${e} → ${p}`);
  res.json({ success: true, email: e, plan: p, expiresAt });
});

app.post('/admin/revoke', (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const e = (req.body.email || '').toLowerCase().trim();
  if (!e) return res.status(400).json({ error: 'Missing email' });
  planStore[e] = { plan: 'free', revokedAt: new Date().toISOString() };
  savePlanStore();
  res.json({ success: true, email: e, plan: 'free' });
});

app.get('/admin/users', (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
  const users = Object.keys({ ...userStore, ...planStore }).map(email => ({
    email,
    plan:      getUserPlan(email),
    planName:  PLANS[getUserPlan(email)]?.name || 'Free',
    used:      getDailyUsage(email),
    limit:     PLANS[getUserPlan(email)]?.dailyLimit || 25,
    remaining: getRemainingEmails(email),
    expiresAt: planStore[email]?.expiresAt || null,
    hasToken:  !!userStore[email]?.refreshToken,
  }));
  res.json({ users, total: users.length });
});

app.get('/admin', async (req, res) => {
  if (!isAdmin(req)) return res.status(401).send(`
    <html><body style="font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#f8f9fa">
    <div style="text-align:center"><h2>🔐 Admin Access</h2>
    <form onsubmit="location.href='/admin?key='+document.getElementById('k').value;return false">
    <input id="k" type="password" placeholder="Admin Key" style="padding:10px;border:1px solid #ddd;border-radius:8px;margin:10px">
    <button style="padding:10px 20px;background:#1a73e8;color:white;border:none;border-radius:8px;cursor:pointer">Login</button>
    </form></div></body></html>
  `);

  const adminKey = req.query.key;
  const allEmails = [...new Set([...Object.keys(userStore), ...Object.keys(planStore)])];
  const users = allEmails.map(email => ({
    email, plan: getUserPlan(email),
    planName: PLANS[getUserPlan(email)]?.name || 'Free',
    used: getDailyUsage(email), limit: PLANS[getUserPlan(email)]?.dailyLimit || 25,
    expiresAt: planStore[email]?.expiresAt, hasToken: !!userStore[email]?.refreshToken,
  }));

  const totalPaid = users.filter(u => u.plan === 'paid').length;
  const totalIntern = users.filter(u => u.plan === 'intern').length;
  const totalFree = users.filter(u => u.plan === 'free').length;
  const revenue = totalPaid * PLANS.paid.price;

  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({spreadsheetId:process.env.TRACKING_SHEET_ID,range:'Tracking!A:F'});
    const rows = (r.data.values||[]).slice(1).reverse();
    const opens   = rows.filter(r=>r[1]==='EMAIL_OPENED').length;
    const clicks  = rows.filter(r=>r[1]==='EMAIL_CLICKED').length;
    const bounces = rows.filter(r=>r[1]==='EMAIL_BOUNCED').length;
    const unsubs  = rows.filter(r=>r[1]==='UNSUBSCRIBED').length;

    res.send(`<!DOCTYPE html>
<html><head><title>EduJunior Admin</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f2f5;min-height:100vh}
.header{background:linear-gradient(135deg,#1a73e8,#0d47a1);color:white;padding:20px 30px;display:flex;align-items:center;gap:12px}
.header h1{font-size:20px;font-weight:600}
.container{padding:24px;max-width:1200px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
.card{background:white;border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.06);text-align:center}
.card .num{font-size:36px;font-weight:700;line-height:1.1}
.card .lbl{font-size:12px;color:#5f6368;margin-top:6px}
.green .num{color:#2e7d32} .blue .num{color:#1a73e8} .orange .num{color:#e65100} .red .num{color:#d93025} .purple .num{color:#6a1b9a}
.section{background:white;border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:20px}
.section h2{font-size:15px;font-weight:600;color:#202124;margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid #f1f3f4}
.grant-form{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.grant-form input,.grant-form select{padding:9px 13px;border:1px solid #ddd;border-radius:8px;font-size:13px;flex:1;min-width:180px}
.btn{padding:9px 18px;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600}
.btn-blue{background:#1a73e8;color:white} .btn-red{background:#d93025;color:white} .btn-green{background:#2e7d32;color:white}
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:#f8f9fa;color:#5f6368;padding:10px 14px;text-align:left;font-weight:600;border-bottom:2px solid #e8eaed}
td{padding:9px 14px;border-bottom:1px solid #f1f3f4;vertical-align:middle}
tr:hover td{background:#f8f9fa}
.badge{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600}
.badge-paid{background:#e8f5e9;color:#2e7d32}
.badge-intern{background:#e3f2fd;color:#1565c0}
.badge-free{background:#f5f5f5;color:#757575}
.bar-bg{background:#e8eaed;border-radius:4px;height:6px;width:80px;display:inline-block;vertical-align:middle}
.bar-fill{background:#1a73e8;border-radius:4px;height:6px}
.msg{padding:10px 16px;border-radius:8px;margin-top:10px;font-size:13px;display:none}
.msg.ok{background:#e8f5e9;color:#2e7d32} .msg.err{background:#fce8e6;color:#d93025}
</style></head>
<body>
<div class="header">
  <div>📊</div>
  <div>
    <h1>EduJunior Mail Merge — Admin Dashboard</h1>
    <div style="font-size:12px;opacity:.8">${new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})}</div>
  </div>
</div>
<div class="container">
  <div class="grid">
    <div class="card green"><div class="num">${totalPaid}</div><div class="lbl">💳 Paid Users</div></div>
    <div class="card blue"><div class="num">${totalIntern}</div><div class="lbl">🎓 Interns (Free Pro)</div></div>
    <div class="card purple"><div class="num">${totalFree}</div><div class="lbl">🆓 Free Users</div></div>
    <div class="card green"><div class="num">₹${revenue}</div><div class="lbl">💰 Monthly Revenue</div></div>
    <div class="card blue"><div class="num">${opens}</div><div class="lbl">📬 Opens</div></div>
    <div class="card green"><div class="num">${clicks}</div><div class="lbl">🖱️ Clicks</div></div>
    <div class="card red"><div class="num">${bounces}</div><div class="lbl">🔴 Bounces</div></div>
    <div class="card orange"><div class="num">${unsubs}</div><div class="lbl">🚫 Unsubs</div></div>
  </div>
  <div class="section">
    <h2>🎓 Grant / Revoke Access</h2>
    <div class="grant-form">
      <input type="email" id="grantEmail" placeholder="intern@email.com">
      <select id="grantPlan">
        <option value="intern">Intern (Free Pro)</option>
        <option value="paid">Paid Pro</option>
        <option value="free">Free (Revoke)</option>
      </select>
      <select id="grantMonths">
        <option value="">Permanent</option>
        <option value="1">1 Month</option>
        <option value="3">3 Months</option>
        <option value="6">6 Months</option>
        <option value="12">1 Year</option>
      </select>
      <button class="btn btn-blue" onclick="grantAccess()">✅ Grant Access</button>
      <button class="btn btn-red" onclick="revokeAccess()">❌ Revoke</button>
    </div>
    <div id="grantMsg" class="msg"></div>
  </div>
  <div class="section">
    <h2>👥 All Users (${users.length})</h2>
    <table>
      <tr><th>Email</th><th>Plan</th><th>Today's Usage</th><th>Token</th><th>Expires</th><th>Action</th></tr>
      ${users.map(u => `
      <tr>
        <td>${u.email}</td>
        <td><span class="badge badge-${u.plan}">${u.planName}</span></td>
        <td>
          <span style="font-size:12px;color:#5f6368">${u.used}/${u.limit}</span>
          <div class="bar-bg"><div class="bar-fill" style="width:${Math.min(100,u.used/u.limit*100)}%"></div></div>
        </td>
        <td>${u.hasToken ? '✅' : '⚠️'}</td>
        <td style="font-size:11px;color:#5f6368">${u.expiresAt ? new Date(u.expiresAt).toLocaleDateString('en-IN') : '—'}</td>
        <td><button class="btn btn-red" style="padding:4px 10px;font-size:11px" onclick="quickRevoke('${u.email}')">Revoke</button></td>
      </tr>`).join('')}
    </table>
  </div>
  <div class="section">
    <h2>📋 Recent Events</h2>
    <table>
      <tr><th>Time</th><th>Event</th><th>Email</th></tr>
      ${rows.slice(0,50).map(r=>`<tr>
        <td style="font-size:11px;color:#5f6368">${r[0]||''}</td>
        <td><span class="badge" style="${{EMAIL_OPENED:'background:#e8f5e9;color:#2e7d32',EMAIL_CLICKED:'background:#1b5e20;color:white',UNSUBSCRIBED:'background:#fce8e6;color:#d93025',EMAIL_BOUNCED:'background:#fff3e0;color:#e65100',EMAIL_SENT:'background:#f1f3f4;color:#5f6368'}[r[1]]||'background:#f1f3f4;color:#333'}">${r[1]||''}</span></td>
        <td>${r[2]||''}</td>
      </tr>`).join('')}
    </table>
  </div>
</div>
<script>
const KEY = '${adminKey}';
async function grantAccess() {
  const email  = document.getElementById('grantEmail').value.trim();
  const plan   = document.getElementById('grantPlan').value;
  const months = document.getElementById('grantMonths').value;
  if (!email) return showMsg('Enter email!', false);
  const r = await fetch('/admin/grant?key='+KEY, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ email, plan, months: months ? parseInt(months) : null })
  });
  const d = await r.json();
  if (d.success) { showMsg('✅ ' + email + ' → ' + plan + ' access granted!', true); setTimeout(()=>location.reload(),1500); }
  else showMsg('❌ ' + d.error, false);
}
async function revokeAccess() {
  const email = document.getElementById('grantEmail').value.trim();
  if (!email) return showMsg('Enter email!', false);
  const r = await fetch('/admin/revoke?key='+KEY, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ email })
  });
  const d = await r.json();
  if (d.success) { showMsg('✅ ' + email + ' revoked!', true); setTimeout(()=>location.reload(),1500); }
  else showMsg('❌ ' + d.error, false);
}
async function quickRevoke(email) {
  if (!confirm('Revoke ' + email + '?')) return;
  await fetch('/admin/revoke?key='+KEY, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ email })
  });
  location.reload();
}
function showMsg(text, ok) {
  const el = document.getElementById('grantMsg');
  el.textContent = text; el.className = 'msg ' + (ok ? 'ok' : 'err');
  el.style.display = 'block'; setTimeout(()=>el.style.display='none', 3000);
}
</script>
</body></html>`);
  } catch(e) { res.status(500).send('Error: ' + e.message); }
});

app.get('/',(req,res)=>res.json({status:'✅ EduJunior v5',users:Object.keys(userStore).length,jobs:Object.keys(scheduleStore).length,time:new Date()}));

// ═══════════════════════════════════════════════════════════
//  executeScheduledJob
// ═══════════════════════════════════════════════════════════
async function executeScheduledJob(job) {
  const { recipients, draftSubject, draftHtml, sender,
          sheetId, sheetTab, userEmail } = job;
  let sent = 0;

  let activeToken = await getAccessToken(userEmail);
  if (!activeToken) {
    console.log(`❌ No token for ${userEmail} — job failed`);
    throw new Error('No access token available');
  }
  console.log(`✅ Token ready for ${userEmail}`);

  const serverUrl = 'https://mail-merge-tracker.onrender.com';
  const camp      = encodeURIComponent((draftSubject||'sched') + '_' + Date.now());
  const sid       = encodeURIComponent(sheetId || '');
  const stab      = encodeURIComponent(sheetTab || 'Sheet1');

  function wrapHtml(raw) {
    if (!raw.toLowerCase().includes('<html'))
      return `<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>${raw}</body></html>`;
    if (!raw.toLowerCase().includes('</body>')) return raw + '</body></html>';
    return raw;
  }

  for (const r of recipients) {
    try {
      let html    = (draftHtml||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);
      let subject = (draftSubject||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);
      html = wrapHtml(html);

      const enc      = encodeURIComponent(r.email);
      const unsubUrl = `${serverUrl}/unsubscribe?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}`;
      html = html.replace('</body>',
        `<img src="${serverUrl}/track/open?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}" width="1" height="1" style="display:none" alt=""/></body>`);
      html = html.replace(/href="(https?:\/\/[^"]+)"/gi,(m,orig) =>
        orig.includes(serverUrl) ? m :
        `href="${serverUrl}/track/click?email=${enc}&campaign=${camp}&sheetId=${sid}&tab=${stab}&url=${encodeURIComponent(orig)}"`);
      html = html.replace('</body>',
        `<div style="margin-top:28px;padding-top:14px;border-top:1px solid #e8eaed;text-align:center;font-family:Arial,sans-serif;font-size:11px;color:#9aa0a6;">
        <a href="${unsubUrl}" style="color:#9aa0a6;">Unsubscribe</a></div></body>`);

      const boundary = 'mm_' + Math.random().toString(36).slice(2);
      const rawEmail = [
        `From: ${sender}`, `To: ${r.email}`, `Subject: ${subject}`,
        `MIME-Version: 1.0`, `Content-Type: multipart/alternative; boundary="${boundary}"`,
        ``, `--${boundary}`, `Content-Type: text/html; charset=UTF-8`, ``, html, ``, `--${boundary}--`
      ].join('\r\n');
      const encoded = Buffer.from(rawEmail).toString('base64')
        .replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');

      const sr = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
        method:'POST',
        headers: { Authorization:`Bearer ${activeToken}`, 'Content-Type':'application/json' },
        body: JSON.stringify({ raw: encoded })
      });
      console.log(`📤 ${r.email}: ${sr.status}`);

      if (sr.ok) {
        sent++;
        if (sheetId) await updateStatusUserToken(activeToken, sheetId, sheetTab||'Sheet1', r.email, 'EMAIL_SENT');
      } else {
        const err = await sr.text();
        console.log(`❌ ${r.email}: ${err.slice(0,100)}`);
        if (err.includes('401') || err.includes('Invalid Credentials') || err.includes('invalid_token')) {
          console.log(`🔄 401 detected — refreshing token for ${userEmail}`);
          if (userStore[userEmail]) {
            userStore[userEmail].tokenSavedAt = 0;
          }
          activeToken = await getAccessToken(userEmail);
          if (activeToken) {
            try {
              const retry = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
                method:'POST',
                headers: { Authorization:`Bearer ${activeToken}`, 'Content-Type':'application/json' },
                body: JSON.stringify({ raw: encoded })
              });
              if (retry.ok) {
                sent++;
                if (sheetId) await updateStatusUserToken(activeToken, sheetId, sheetTab||'Sheet1', r.email, 'EMAIL_SENT');
                console.log(`✅ Retry success: ${r.email}`);
              }
            } catch(retryErr) { console.error(`Retry failed: ${retryErr.message}`); }
          }
        }
      }
    } catch(e) { console.error(`Error ${r.email}:`, e.message); }
    await sleep(400);
  }
  return sent;
}

// ═══════════════════════════════════════════════════════════
//  STARTUP
// ═══════════════════════════════════════════════════════════
scheduleStore = loadScheduleStore();

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ EduJunior Tracker v5 on port ${PORT}`);
  setTimeout(() => restorePendingJobs(), 3000);
});
