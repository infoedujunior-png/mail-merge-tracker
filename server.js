// ═══════════════════════════════════════════════════════════
//  Mail Merge Tracking Server v5.0
//  ✅ Colors fixed (EMAIL_OPENED = light green)
//  ✅ Unsubscribe fixed (waits for sheet update)
//  ✅ Server-side scheduling (works even Chrome is closed!)
// ═══════════════════════════════════════════════════════════

const express = require('express');
const cors    = require('cors');
const { google } = require('googleapis');
const app = express();
app.use(cors());
app.use(express.json());

const PIXEL = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7','base64');
const trackingStore = {}; // { email: { opened, clicked } }
const scheduleStore = {}; // { id: job }

async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

const STATUS_COLORS = {
  EMAIL_SENT:    { red: 0.85, green: 0.85, blue: 0.85 },
  EMAIL_OPENED:  { red: 0.72, green: 0.94, blue: 0.74 }, // light green
  EMAIL_CLICKED: { red: 0.20, green: 0.66, blue: 0.33 }, // dark green
  EMAIL_BOUNCED: { red: 0.96, green: 0.40, blue: 0.40 }, // red
  UNSUBSCRIBED:  { red: 1.00, green: 0.76, blue: 0.28 }, // orange
};

async function updateStatus(sheetId, tab, email, newStatus) {
  if (!sheetId || !email) return;
  const sheetTab = tab || 'Sheet1';
  console.log(`🔄 ${email} → ${newStatus} | sheet: ${sheetId} | tab: ${sheetTab}`);
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${sheetTab}!A1:Z500`,
    });
    const rows = r.data.values || [];
    if (rows.length < 2) return;

    const headers  = rows[0].map(h => (h||'').toLowerCase().trim());
    const emailCol = headers.findIndex(h => h.includes('email'));
    let   statCol  = headers.findIndex(h => h.includes('merge status') || h === 'status');
    if (emailCol < 0) { console.log('❌ No email column:', headers); return; }

    // ✅ Auto-create Merge Status column if missing
    if (statCol < 0) {
      console.log('⚠️ Creating Merge Status column...');
      statCol = headers.length;
      const newCol = toCol(statCol + 1);
      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${sheetTab}!${newCol}1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [['Merge Status']] },
      });
      console.log(`✅ Merge Status column created at ${newCol}1`);
    }

    let targetRow = -1;
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][emailCol]||'').toLowerCase().trim() === email.toLowerCase().trim()) {
        targetRow = i + 1; break;
      }
    }
    if (targetRow < 0) { console.log(`❌ Email not found: ${email}`); return; }

    const P = { EMAIL_SENT:1, EMAIL_OPENED:2, EMAIL_CLICKED:3, EMAIL_BOUNCED:4, UNSUBSCRIBED:5 };
    const cur = ((rows[targetRow-1]||[])[statCol]||'').toUpperCase().trim();
    if ((P[cur]||0) >= (P[newStatus]||0)) { console.log(`⏭️ Skip: ${cur}>=${newStatus}`); return; }

    // Write text
    const col = toCol(statCol + 1);
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetTab}!${col}${targetRow}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[newStatus]] },
    });

    // Apply color
    const color = STATUS_COLORS[newStatus];
    if (color) {
      let gid = 0;
      try {
        const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId, fields: 'sheets.properties' });
        const sh = (meta.data.sheets||[]).find(s => s.properties.title === sheetTab);
        gid = sh?.properties?.sheetId ?? 0;
      } catch(e) {}

      const isBold = ['EMAIL_CLICKED','EMAIL_BOUNCED'].includes(newStatus);
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: sheetId,
        requestBody: { requests: [{
          repeatCell: {
            range: { sheetId: gid, startRowIndex: targetRow-1, endRowIndex: targetRow, startColumnIndex: statCol, endColumnIndex: statCol+1 },
            cell: { userEnteredFormat: {
              backgroundColor: color,
              textFormat: { bold: isBold, foregroundColor: isBold ? {red:1,green:1,blue:1} : {red:0.1,green:0.1,blue:0.1} }
            }},
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          }
        }]}
      });
      console.log(`🎨 Color applied for ${newStatus}`);
    }

    console.log(`✅ DONE: ${email} → ${newStatus} row ${targetRow}`);
  } catch(e) { console.error(`❌ Error: ${e.message}`); }
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
  } catch(e){}
}

function toCol(n) {
  let s=''; while(n>0){const r=(n-1)%26;s=String.fromCharCode(65+r)+s;n=Math.floor((n-1)/26);} return s;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Update sheet using USER's OAuth token (for scheduled sends) ──
async function updateSheetWithUserToken(token, sheetId, sheetTab, email, status) {
  try {
    const tab = sheetTab || 'Sheet1';
    const res = await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(tab+'!A1:Z500')}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (!res.ok) { console.log(`Sheet read failed: ${res.status}`); return; }
    const data = await res.json();
    const rows = data.values || [];
    if (rows.length < 2) return;

    const headers  = rows[0].map(h => (h||'').toLowerCase().trim());
    const emailCol = headers.findIndex(h => h.includes('email'));
    let   statCol  = headers.findIndex(h => h.includes('merge status') || h === 'status');

    // ✅ AUTO-CREATE "Merge Status" column if missing!
    if (emailCol < 0) { console.log('No email column found:', headers); return; }
    if (statCol < 0) {
      console.log('⚠️ Merge Status column missing — creating it...');
      statCol = headers.length; // new column at end
      const newColLetter = toCol(statCol + 1);
      await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(tab+'!'+newColLetter+'1')}?valueInputOption=USER_ENTERED`,
        {
          method: 'PUT',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ values: [['Merge Status']] }),
        }
      );
      console.log(`✅ Merge Status column created at ${newColLetter}1`);
    }

    let targetRow = -1;
    for (let i = 1; i < rows.length; i++) {
      if ((rows[i][emailCol]||'').toLowerCase().trim() === email.toLowerCase().trim()) {
        targetRow = i + 1; break;
      }
    }
    if (targetRow < 0) { console.log(`Email not found: ${email}`); return; }

    const P = { EMAIL_SENT:1, EMAIL_OPENED:2, EMAIL_CLICKED:3, EMAIL_BOUNCED:4, UNSUBSCRIBED:5 };
    const cur = ((rows[targetRow-1]||[])[statCol]||'').toUpperCase().trim();
    if ((P[cur]||0) >= (P[status]||0)) return;

    const col = toCol(statCol + 1);
    await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(tab+'!'+col+targetRow)}?valueInputOption=USER_ENTERED`,
      {
        method: 'PUT',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ values: [[status]] }),
      }
    );

    // Apply color using service account
    try {
      const sheets = await getSheets();
      const color = STATUS_COLORS[status];
      if (color) {
        let gid = 0;
        try {
          const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId, fields: 'sheets.properties' });
          const sh = (meta.data.sheets||[]).find(s => s.properties.title === tab);
          gid = sh?.properties?.sheetId ?? 0;
        } catch(e) {}
        await sheets.spreadsheets.batchUpdate({
          spreadsheetId: sheetId,
          requestBody: { requests: [{ repeatCell: {
            range: { sheetId:gid, startRowIndex:targetRow-1, endRowIndex:targetRow, startColumnIndex:statCol, endColumnIndex:statCol+1 },
            cell: { userEnteredFormat: { backgroundColor: color, textFormat: { bold: false } } },
            fields: 'userEnteredFormat(backgroundColor,textFormat)',
          }}]}
        });
      }
    } catch(e) {}

    console.log(`✅ Scheduled sheet updated: ${email} → ${status} row ${targetRow}`);
  } catch(e) { console.error('updateSheetWithUserToken error:', e.message); }
}


// ── ROUTES ─────────────────────────────────────────────────

app.get('/ping', (req,res) => res.json({ status:'alive', time:new Date() }));

// 📬 Opened
app.get('/track/open', async (req,res) => {
  res.set({'Content-Type':'image/gif','Cache-Control':'no-cache,no-store'}); res.send(PIXEL);
  const {email,campaign,sheetId,tab} = req.query;
  if (!email) return;
  const k = email.toLowerCase();
  if (!trackingStore[k]) trackingStore[k] = {};
  trackingStore[k].opened = true;
  updateStatus(sheetId, tab, email, 'EMAIL_OPENED').catch(console.error);
  logEvent({type:'EMAIL_OPENED',email,campaign}).catch(()=>{});
});

// 🖱️ Clicked
app.get('/track/click', async (req,res) => {
  const {email,campaign,url,sheetId,tab} = req.query;
  res.redirect(url ? decodeURIComponent(url) : 'https://google.com');
  if (!email) return;
  const k = email.toLowerCase();
  if (!trackingStore[k]) trackingStore[k] = {};
  trackingStore[k].clicked = true;
  updateStatus(sheetId, tab, email, 'EMAIL_CLICKED').catch(console.error);
  logEvent({type:'EMAIL_CLICKED',email,campaign,url}).catch(()=>{});
});

// 🚫 Unsubscribe — FIXED: awaits update before responding
app.get('/unsubscribe', async (req,res) => {
  const {email,campaign,sheetId,tab} = req.query;
  console.log(`🚫 UNSUBSCRIBE: ${email} | sheetId: ${sheetId}`);
  await updateStatus(sheetId, tab, email, 'UNSUBSCRIBED');
  await logEvent({type:'UNSUBSCRIBED',email,campaign});
  res.send(`<!DOCTYPE html><html><head><title>Unsubscribed</title>
  <style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;display:flex;align-items:center;justify-content:center;min-height:100vh}.card{background:white;border-radius:16px;padding:48px;text-align:center;max-width:420px;box-shadow:0 4px 24px rgba(0,0,0,.08)}.icon{font-size:52px;margin-bottom:16px}h1{font-size:22px;color:#202124;margin-bottom:10px}p{font-size:14px;color:#5f6368;line-height:1.7}.em{font-weight:600;color:#1a73e8}</style>
  </head><body><div class="card"><div class="icon">✅</div><h1>Successfully Unsubscribed</h1>
  <p>The address <span class="em">${email||'your email'}</span> has been removed from this mailing list.<br><br>You won't receive any further emails from this campaign.</p>
  </div></body></html>`);
});

// 🔍 Check tracking
app.get('/check', (req,res) => {
  const k = (req.query.email||'').toLowerCase();
  const d = trackingStore[k]||{};
  res.json({opened:!!d.opened,clicked:!!d.clicked});
});

// ═══════════════════════════════════════════════════════════
//  📅 SERVER-SIDE SCHEDULING — Chrome band ho toh bhi kaam kare!
// ═══════════════════════════════════════════════════════════
app.post('/schedule', async (req,res) => {
  try {
    const { scheduleId, scheduledAt, recipients, draftSubject, draftHtml, sender, sheetId, sheetTab, token } = req.body;
    if (!scheduledAt||!recipients||!draftHtml||!token) return res.status(400).json({error:'Missing fields'});

    const delay = new Date(scheduledAt).getTime() - Date.now();
    if (delay < 0) return res.status(400).json({error:'Time is in the past'});

    const id = scheduleId || ('job_'+Date.now());
    scheduleStore[id] = { status:'pending', scheduledAt, recipients, sender, sheetId, sheetTab };

    console.log(`📅 Job ${id} scheduled for ${scheduledAt} — ${recipients.length} emails`);

    setTimeout(async () => {
      console.log(`🚀 SCHEDULE RUNNING: Job ${id} | ${recipients.length} emails`);
      scheduleStore[id].status = 'running';
      let sent = 0;

      const serverUrl = 'https://mail-merge-tracker.onrender.com';
      const campaign  = encodeURIComponent((draftSubject||'scheduled') + '_' + Date.now());
      const sid       = encodeURIComponent(sheetId||'');
      const stab      = encodeURIComponent(sheetTab||'Sheet1');

      // ✅ Always wrap in full HTML so pixel/footer always inject correctly
      function wrapHtml(rawHtml) {
        if (!rawHtml.toLowerCase().includes('<html')) {
          return `<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>${rawHtml}</body></html>`;
        }
        if (!rawHtml.toLowerCase().includes('</body>')) {
          return rawHtml + '</body></html>';
        }
        return rawHtml;
      }

      for (const r of recipients) {
        try {
          console.log(`📤 Sending to: ${r.email}`);
          let html    = (draftHtml||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);
          let subject = (draftSubject||'').replace(/\{\{(\w[\w\s]*)\}\}/g,(m,k)=>r[k.trim().toLowerCase()]||r[k.trim()]||m);

          // Wrap in full HTML structure first
          html = wrapHtml(html);

          const emailEnc = encodeURIComponent(r.email);

          // ✅ Inject open tracking pixel — always works now
          const pixel = `<img src="${serverUrl}/track/open?email=${emailEnc}&campaign=${campaign}&sheetId=${sid}&tab=${stab}" width="1" height="1" style="display:none;border:0;" alt="" />`;
          html = html.replace('</body>', pixel + '</body>');

          // ✅ Wrap links for click tracking
          html = html.replace(/href="(https?:\/\/[^"]+)"/gi, (m, orig) => {
            if (orig.includes(serverUrl)) return m;
            const wrapped = `${serverUrl}/track/click?email=${emailEnc}&campaign=${campaign}&sheetId=${sid}&tab=${stab}&url=${encodeURIComponent(orig)}`;
            return `href="${wrapped}"`;
          });

          // ✅ Add unsubscribe footer — always works now
          const unsubUrl = `${serverUrl}/unsubscribe?email=${emailEnc}&campaign=${campaign}&sheetId=${sid}&tab=${stab}`;
          const footer = `<div style="margin-top:28px;padding-top:14px;border-top:1px solid #e8eaed;text-align:center;font-family:Arial,sans-serif;font-size:11px;color:#9aa0a6;"><a href="${unsubUrl}" style="color:#9aa0a6;text-decoration:underline;">Unsubscribe</a></div>`;
          html = html.replace('</body>', footer + '</body>');

          const boundary = 'mm_'+Math.random().toString(36).slice(2);
          const raw = [
            `From: ${sender}`,`To: ${r.email}`,`Subject: ${subject}`,
            `MIME-Version: 1.0`,`Content-Type: multipart/alternative; boundary="${boundary}"`,
            ``,`--${boundary}`,`Content-Type: text/html; charset=UTF-8`,``,html,``,`--${boundary}--`
          ].join('\r\n');
          const encoded = Buffer.from(raw).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');

          const sr = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
            method:'POST',
            headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
            body: JSON.stringify({raw:encoded}),
          });

          const srStatus = sr.status;
          console.log(`📬 Gmail API response for ${r.email}: ${srStatus}`);

          if (sr.ok) {
            sent++;
            console.log(`✅ Email sent to ${r.email}`);
            // ✅ Update sheet — uses user's own token
            if (sheetId) {
              await updateSheetWithUserToken(token, sheetId, sheetTab||'Sheet1', r.email, 'EMAIL_SENT');
            }
          } else {
            const errBody = await sr.text();
            console.log(`❌ Gmail send failed for ${r.email}: ${errBody.slice(0,200)}`);
          }
        } catch(e) { console.error(`❌ Error sending to ${r.email}:`, e.message); }
        await sleep(400);
      }

      scheduleStore[id].status = 'done';
      scheduleStore[id].sent   = sent;
      console.log(`✅ Job ${id} COMPLETE: ${sent}/${recipients.length} sent`);
    }, delay);

    res.json({ success:true, id, message:`Scheduled! ${recipients.length} emails will be sent at ${new Date(scheduledAt).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})}` });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/schedule/:id', (req,res) => {
  const job = scheduleStore[req.params.id];
  if (!job) return res.status(404).json({error:'Not found'});
  res.json({status:job.status, sent:job.sent, total:job.recipients?.length});
});

// 📊 Dashboard
app.get('/dashboard', async (req,res) => {
  if (req.query.key !== process.env.DASHBOARD_KEY) return res.status(401).send('Add ?key=YOUR_PASSWORD');
  try {
    const sheets = await getSheets();
    const r = await sheets.spreadsheets.values.get({spreadsheetId:process.env.TRACKING_SHEET_ID,range:'Tracking!A:F'});
    const rows   = (r.data.values||[]).slice(1).reverse();
    const opens  = rows.filter(r=>r[1]==='EMAIL_OPENED').length;
    const clicks = rows.filter(r=>r[1]==='EMAIL_CLICKED').length;
    const unsubs = rows.filter(r=>r[1]==='UNSUBSCRIBED').length;
    const bounces= rows.filter(r=>r[1]==='EMAIL_BOUNCED').length;
    const BADGE = {EMAIL_OPENED:'background:#e8f5e9;color:#2e7d32',EMAIL_CLICKED:'background:#1b5e20;color:white',UNSUBSCRIBED:'background:#fce8e6;color:#d93025',EMAIL_BOUNCED:'background:#fff3e0;color:#e65100',EMAIL_SENT:'background:#f1f3f4;color:#5f6368'};
    res.send(`<!DOCTYPE html><html><head><title>📊 Dashboard</title><meta name="viewport" content="width=device-width,initial-scale=1">
    <style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#f8f9fa;padding:24px}h1{font-size:22px;color:#202124;margin-bottom:20px}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}.stat{background:white;border-radius:12px;padding:18px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)}.n{font-size:38px;font-weight:700;line-height:1}.l{font-size:11px;color:#5f6368;margin-top:6px}.o .n{color:#2e7d32}.c .n{color:#1b5e20}.b .n{color:#e65100}.u .n{color:#d93025}table{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}th{background:#1a73e8;color:white;padding:10px 14px;text-align:left;font-size:12px}td{padding:8px 14px;font-size:12px;border-bottom:1px solid #f1f3f4}.badge{display:inline-block;padding:3px 9px;border-radius:10px;font-size:11px;font-weight:600}</style>
    </head><body><h1>📊 Mail Merge Dashboard</h1>
    <div class="stats">
      <div class="stat o"><div class="n">${opens}</div><div class="l">📬 Opened</div></div>
      <div class="stat c"><div class="n">${clicks}</div><div class="l">🖱️ Clicked</div></div>
      <div class="stat b"><div class="n">${bounces}</div><div class="l">🔴 Bounced</div></div>
      <div class="stat u"><div class="n">${unsubs}</div><div class="l">🚫 Unsubscribed</div></div>
    </div>
    <table><tr><th>Date & Time</th><th>Event</th><th>Email</th><th>Campaign</th></tr>
    ${rows.slice(0,150).map(r=>{const st=BADGE[r[1]]||'background:#f1f3f4;color:#5f6368';return`<tr><td>${r[0]||''}</td><td><span class="badge" style="${st}">${r[1]||''}</span></td><td>${r[2]||''}</td><td>${r[3]||''}</td></tr>`;}).join('')}
    </table></body></html>`);
  } catch(e){res.status(500).send('Error: '+e.message);}
});

app.get('/', (req,res) => res.json({status:'✅ v5 Running',time:new Date()}));
const PORT = process.env.PORT||3000;
app.listen(PORT, ()=>console.log(`✅ Tracker v5 on port ${PORT}`));
