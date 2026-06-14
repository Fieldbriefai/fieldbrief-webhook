import express from 'express';
import { Anthropic } from '@anthropic-ai/sdk';
import twilio from 'twilio';
import Stripe from 'stripe';
import cron from 'node-cron';

// ============================================================================
// INITIALIZATION
// ============================================================================
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY || '');
const twilioClient = twilio(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN
);

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || 'patm1fGCuyaDhi5RC.0ab7a30ee2453980d68154847713f309a8eb310764f48840e66af79cd9c2cb06';
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appbcR8hJtuXwpEI8';
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER || '+18559835461';
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || 'https://fieldbrief-webhook.onrender.com';

const TABLES = {
  SUBSCRIBERS: 'tblhEsWe6OP3aX9LN',
  CUSTOMERS: 'tbl10XZx1pL0mzz6q',
  EQUIPMENT: 'tblMifbGMpnctvf4n',
  WORK_ORDERS: 'tblxINDV3BoyJ58uk',
  PARTS_USED: 'tbls7gxcmPYoCqzAq',
  SUPPLIERS: 'tblTvXiLkPFScWYWX',
  INVOICES: 'tbllE1LpvhIPl11Zx',
  SUPPORT_TICKETS: 'tble3uY3LsYc4ORT7',
  SMS_LOG: 'tbl06XD7Dcn1r4R7F',
  TECHS: 'tblffsygUr53MXqYQ',
};

// ============================================================================
// AIRTABLE HELPERS
// ============================================================================
async function airtableRequest(method, tableId, data = null, recordId = null) {
  const endpoint = recordId
    ? `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}/${recordId}`
    : `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}`;
  const options = {
    method,
    headers: {
      'Authorization': `Bearer ${AIRTABLE_TOKEN}`,
      'Content-Type': 'application/json',
    },
  };
  if (data) options.body = JSON.stringify(data);
  try {
    const response = await fetch(endpoint, options);
    if (!response.ok) {
      console.error(`Airtable error (${method} ${tableId}):`, await response.text());
      return null;
    }
    return await response.json();
  } catch (error) {
    console.error('Airtable request error:', error);
    return null;
  }
}

async function airtableQuery(tableId, filterFormula) {
  const endpoint = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}?filterByFormula=${encodeURIComponent(filterFormula)}`;
  try {
    const response = await fetch(endpoint, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${AIRTABLE_TOKEN}`, 'Content-Type': 'application/json' },
    });
    if (!response.ok) { console.error('Airtable query error:', await response.text()); return []; }
    const data = await response.json();
    return data.records || [];
  } catch (error) { console.error('Airtable query error:', error); return []; }
}

async function airtableCreate(tableId, fields) {
  const result = await airtableRequest('POST', tableId, { typecast: true, fields });
  return result?.id || null;
}

async function airtableUpdate(tableId, recordId, fields) {
  const result = await airtableRequest('PATCH', tableId, { typecast: true, fields }, recordId);
  return result?.id || null;
}

// ============================================================================
// SMS & TWILIO HELPERS
// ----------------------------------------------------------------------------
// IMPORTANT: sendSMS() is ONLY used for OUTBOUND-initiated messages
// (morning brief cron, Stripe welcome). Inbound replies MUST go through
// TwiML only. Sending via BOTH paths is what triggered Twilio error 30039
// (loop filter). Handlers now return a string; the /sms route sends it as
// a single TwiML response.
// ============================================================================
async function sendSMS(toNumber, message) {
  try {
    const response = await twilioClient.messages.create({ body: message, from: TWILIO_PHONE_NUMBER, to: toNumber });
    console.log(`SMS sent to ${toNumber}:`, response.sid);
    return response.sid;
  } catch (error) { console.error(`Failed to send SMS to ${toNumber}:`, error); return null; }
}

function createTwiMLResponse(message) {
  return `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${escapeXML(message)}</Message></Response>`;
}

function escapeXML(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function replyTwiML(res, message) {
  res.type('text/xml').send(createTwiMLResponse(message || 'Message received.'));
}

// Business-local date (Pacific) as YYYY-MM-DD. offsetDays shifts by N days.
// Server runs UTC on Render; stamping/querying dates in UTC mis-buckets any
// job logged after ~4-5pm Pacific onto the next day, breaking JOBS/PARTS/BRIEF.
const BUSINESS_TZ = process.env.BUSINESS_TZ || 'America/Los_Angeles';
function localDate(offsetDays = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return d.toLocaleDateString('en-CA', { timeZone: BUSINESS_TZ });
}

// ============================================================================
// CLAUDE AI FUNCTIONS
// ============================================================================
async function classifyIntent(smsBody) {
  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 100,
      system: `You are an SMS intent classifier for a field service job management system.
Classify the incoming SMS into ONE of these categories:
- "job_log": Contractor reporting a completed job, parts used, or work done
- "command": User asking for info (PARTS, JOBS, INVOICE, BRIEF, HELP, STATUS)
- "support": Problem, question, or complaint
- "feature_request": Suggesting a new feature
- "cancel": Wants to cancel subscription
- "billing": Billing or payment related
- "general": General conversation or greeting
Respond with ONLY the category name, nothing else.`,
      messages: [{ role: 'user', content: smsBody }],
    });
    return message.content[0].type === 'text' ? message.content[0].text.trim().toLowerCase() : 'general';
  } catch (error) { console.error('Claude classification error:', error); return 'general'; }
}

async function parseJobLog(smsBody, subscriberName) {
  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1500,
      system: `You are a job log parser for field service contractors. Extract structured data from casual SMS messages.
The contractor ${subscriberName} is reporting work they completed today.
Parse the text into this JSON structure (include only fields that are present):
{
  "customer": { "name": "", "address": "", "city": "", "state": "" },
  "equipment": { "category": "", "manufacturer": "", "model": "", "serial_number": "", "fuel_type": "" },
  "work_order": { "job_type": "", "description": "", "labor_hours": 0, "status": "Completed" },
  "parts": [{ "name": "", "supplier": "", "cost": 0, "quantity": 1, "category": "" }]
}
Common abbreviations: WM=Weil-McLain, circ=circulator pump, EWT=electric water tank, ASHP=air-source heat pump, RTU=rooftop unit.
Handle incomplete info gracefully. Multiple jobs in one text are OK. Respond with ONLY valid JSON.`,
      messages: [{ role: 'user', content: smsBody }],
    });
    const responseText = message.content[0].type === 'text' ? message.content[0].text : '{}';
    const jsonMatch = responseText.match(/```json\n?([\s\S]*?)\n?```/) || responseText.match(/({[\s\S]*})/);
    return JSON.parse(jsonMatch ? jsonMatch[1] : responseText);
  } catch (error) { console.error('Claude parse error:', error); return null; }
}

async function generateAIResponse(smsBody, ticketType) {
  try {
    const prompts = {
      support: 'You are a friendly field service support assistant. Keep response to 1-2 sentences, max 160 chars.',
      feature_request: 'Thank the contractor for their feature suggestion. Keep to 1-2 sentences, max 160 chars.',
      billing: 'Address the billing question helpfully. Keep to 1-2 sentences, max 160 chars.',
      cancel: 'Acknowledge their cancellation request. Keep to 1-2 sentences, max 160 chars.',
    };
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514', max_tokens: 100,
      system: prompts[ticketType] || prompts.support,
      messages: [{ role: 'user', content: smsBody }],
    });
    return message.content[0].type === 'text' ? message.content[0].text.trim() : 'Thanks for reaching out.';
  } catch (error) { return 'Thanks for reaching out. We\'ll review this and get back to you.'; }
}

async function generateMorningBrief(yesterdayJobs) {
  try {
    const jobSummary = yesterdayJobs.map(j =>
      `- ${j.fields.customer_name || 'Unknown'}: ${j.fields.job_type || 'Service'} (${j.fields.labor_hours || 0}h)`
    ).join('\n');
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514', max_tokens: 200,
      system: 'Create a short motivational morning summary of yesterday\'s work for a field service contractor. 2-3 sentences.',
      messages: [{ role: 'user', content: `Yesterday's jobs:\n${jobSummary || 'No jobs logged'}` }],
    });
    return message.content[0].type === 'text' ? message.content[0].text.trim() : 'Good morning! Have a productive day.';
  } catch (error) { return 'Good morning! Have a productive day ahead.'; }
}

// ============================================================================
// JOB LOG HANDLER
// Returns reply string. Does NOT call sendSMS — caller sends via TwiML.
// ============================================================================
async function handleJobLog(smsBody, subscriberPhone, subscriberName) {
  const parsedData = await parseJobLog(smsBody, subscriberName);
  if (!parsedData) {
    return 'Got your text but had trouble reading it. Re-send with customer, work done, hours, and parts and I\'ll try again.';
  }
  const settings = await getSubscriberSettings(subscriberPhone);
  try {
    let customerName = parsedData.customer?.name?.trim() || 'Unknown';
    if (parsedData.customer?.name) {
      const existing = await airtableQuery(TABLES.CUSTOMERS,
        `AND({customer_name} = "${customerName}", {subscriber_phone} = "${subscriberPhone}")`);
      if (existing.length === 0) {
        await airtableCreate(TABLES.CUSTOMERS, {
          customer_name: customerName,
          address: parsedData.customer.address || '',
          city: parsedData.customer.city || '',
          state: parsedData.customer.state || '',
          subscriber_phone: subscriberPhone,
        });
      }
    }
    let equipmentLabel = '';
    if (parsedData.equipment && (parsedData.equipment.model || parsedData.equipment.serial_number)) {
      equipmentLabel = [parsedData.equipment.manufacturer, parsedData.equipment.model].filter(Boolean).join(' ');
      const existingEquip = parsedData.equipment.serial_number
        ? await airtableQuery(TABLES.EQUIPMENT, `{serial_number} = "${parsedData.equipment.serial_number}"`)
        : [];
      if (existingEquip.length === 0) {
        await airtableCreate(TABLES.EQUIPMENT, {
          equipment_label: equipmentLabel || 'Unknown Equipment',
          category: parsedData.equipment.category || '',
          manufacturer: parsedData.equipment.manufacturer || '',
          model: parsedData.equipment.model || '',
          serial_number: parsedData.equipment.serial_number || '',
          fuel_type: parsedData.equipment.fuel_type || '',
          customer_name: customerName,
          subscriber_phone: subscriberPhone,
        });
      }
    }
    const woLabel = `${customerName} - ${localDate()}`;
    const hours = parsedData.work_order?.labor_hours || 0;
    if (parsedData.work_order) {
      await airtableCreate(TABLES.WORK_ORDERS, {
        wo_label: woLabel,
        job_type: parsedData.work_order.job_type || 'Service',
        description: parsedData.work_order.description || '',
        labor_hours: hours,
        labor_rate: settings.rate || 0,
        status: 'Completed',
        date: localDate(),
        customer_name: customerName,
        customer_address: parsedData.customer?.address || '',
        equipment_label: equipmentLabel,
        tech_name: subscriberName,
        subscriber_phone: subscriberPhone,
        raw_sms: smsBody,
      });
    }
    const partLines = [];
    if (parsedData.parts && Array.isArray(parsedData.parts)) {
      for (const part of parsedData.parts) {
        if (part.supplier) {
          const existingSuppliers = await airtableQuery(TABLES.SUPPLIERS, `{supplier_name} = "${part.supplier}"`);
          if (existingSuppliers.length === 0) {
            await airtableCreate(TABLES.SUPPLIERS, { supplier_name: part.supplier });
          }
        }
        const cost = part.cost || 0;
        const qty = part.quantity || 1;
        // Bill at the contractor's markup (set once via SET MARKUP). Falls back to cost.
        const markupPrice = Math.round(cost * (1 + (settings.markup || 0) / 100) * 100) / 100;
        await airtableCreate(TABLES.PARTS_USED, {
          part_name: part.name || '',
          supplier_name: part.supplier || '',
          cost,
          markup_price: markupPrice,
          quantity: qty,
          category: part.category || '',
          wo_label: woLabel,
          subscriber_phone: subscriberPhone,
          date: localDate(),
        });
        partLines.push({ name: part.name || 'part', qty, price: markupPrice });
      }
    }
    // Rich confirmation so the contractor can trust (and correct) what was captured.
    const desc = parsedData.work_order?.description || parsedData.work_order?.job_type || 'Work';
    const rate = settings.rate || 0;
    const partsTotal = partLines.reduce((s, p) => s + p.price * p.qty, 0);
    const laborTotal = hours * rate;
    let msg = `✓ Logged — ${customerName} (${localDate()})\n${desc}` + (hours ? ` · ${hours}h` : '');
    if (hours && rate) msg += ` @ $${rate} = $${laborTotal.toFixed(2)}`;
    if (partLines.length) {
      msg += `\nParts: ` + partLines.map(p => `${p.name} $${p.price.toFixed(2)}${p.qty > 1 ? '×' + p.qty : ''}`).join(', ');
    }
    const grand = laborTotal + partsTotal;
    if (grand > 0) msg += `\nTotal: $${grand.toFixed(2)}`;
    if (hours && !rate) msg += `\n(set your rate to price labor: SET RATE 215)`;
    msg += `\nWrong? Reply UNDO, or FIX HOURS/CUSTOMER/PART.`;
    return msg;
  } catch (error) {
    console.error('Job log error:', error);
    return 'Error logging your job. Please try again.';
  }
}

// ============================================================================
// SUBSCRIBER SETTINGS (set-once: rate, company, license, markup, email, pay note)
// ============================================================================
async function getSubscriberSettings(phone) {
  const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${phone}"`);
  const rec = subs[0]; const f = rec?.fields || {};
  return {
    recId: rec?.id || null,
    name: f['Full Name'] || '',
    company: f['Company'] || f['Company Name'] || '',
    license: f['License'] || '',
    rate: f['Hourly Rate'] || 0,
    markup: f['Markup Pct'] || 0,
    email: f['Contractor Email'] || '',
    payNote: f['Pay Note'] || '',
    allowTechInvoicing: !!f['Allow Tech Invoicing'],
  };
}

async function handleSettings(command, phone) {
  const s = await getSubscriberSettings(phone);
  if (!s.recId) return 'Account not found.';
  const m = command.match(/^\s*SET\s+(\w+)\s+([\s\S]+)$/i);
  if (!m) {
    return `Your settings:\nCompany: ${s.company || '—'}\nRate: $${s.rate || 0}/hr\nMarkup: ${s.markup || 0}%\nLicense: ${s.license || '—'}\nEmail: ${s.email || '—'}\nPay note: ${s.payNote || '—'}\nTech invoicing: ${s.allowTechInvoicing ? 'on' : 'off (owner only)'}\n\nChange: SET RATE 215 · SET MARKUP 30 · SET COMPANY name · SET LICENSE # · SET EMAIL you@co.com · SET PAY note · SET TECHINVOICE on/off`;
  }
  const key = m[1].toUpperCase(); const val = m[2].trim();
  const map = { RATE: 'Hourly Rate', MARKUP: 'Markup Pct', COMPANY: 'Company', LICENSE: 'License', EMAIL: 'Contractor Email', PAY: 'Pay Note', TECHINVOICE: 'Allow Tech Invoicing' };
  const field = map[key];
  if (!field) return `Unknown setting "${key}". Options: RATE, MARKUP, COMPANY, LICENSE, EMAIL, PAY, TECHINVOICE.`;
  let value = val;
  if (key === 'RATE' || key === 'MARKUP') value = parseFloat(val.replace(/[^0-9.]/g, '')) || 0;
  if (key === 'TECHINVOICE') value = /^(on|yes|true|1|enable|enabled)$/i.test(val);
  const ok = await airtableUpdate(TABLES.SUBSCRIBERS, s.recId, { [field]: value });
  if (!ok) return 'Could not save that. Try again.';
  const shown = (key === 'RATE') ? '$' + value + '/hr' : (key === 'MARKUP') ? value + '%' : (key === 'TECHINVOICE') ? (value ? 'on' : 'off') : value;
  return `✓ ${key} set to ${shown}.`;
}

// Most recent work order for a subscriber (for UNDO / FIX).
async function latestWorkOrder(phone) {
  const recs = await airtableQuery(TABLES.WORK_ORDERS, `{subscriber_phone} = "${phone}"`);
  recs.sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  return recs[0] || null;
}

async function handleUndo(phone) {
  const wo = await latestWorkOrder(phone);
  if (!wo) return 'Nothing to undo.';
  const label = wo.fields.wo_label;
  let partCount = 0;
  if (label) {
    const parts = await airtableQuery(TABLES.PARTS_USED, `{wo_label} = "${label}"`);
    for (const p of parts) { await airtableRequest('DELETE', TABLES.PARTS_USED, null, p.id); partCount++; }
  }
  await airtableRequest('DELETE', TABLES.WORK_ORDERS, null, wo.id);
  return `Removed: ${wo.fields.job_type || 'job'} for ${wo.fields.customer_name} (${wo.fields.date})${partCount ? `, ${partCount} part(s)` : ''}. Re-text it to log again.`;
}

async function handleFix(command, phone) {
  const wo = await latestWorkOrder(phone);
  if (!wo) return 'Nothing to fix yet — log a job first.';
  const m = command.match(/^\s*FIX\s+(\w+)\s+([\s\S]+)$/i);
  if (!m) return 'Fix your last entry: FIX HOURS 2 · FIX CUSTOMER name · FIX JOB description · FIX PART 185';
  const key = m[1].toUpperCase(); const val = m[2].trim();
  if (key === 'HOURS') {
    const h = parseFloat(val) || 0;
    await airtableUpdate(TABLES.WORK_ORDERS, wo.id, { labor_hours: h });
    return `✓ Hours on ${wo.fields.customer_name}'s job set to ${h}h.`;
  }
  if (key === 'JOB' || key === 'DESC') {
    await airtableUpdate(TABLES.WORK_ORDERS, wo.id, { description: val });
    return `✓ Updated the job description for ${wo.fields.customer_name}.`;
  }
  if (key === 'CUSTOMER') {
    const oldLabel = wo.fields.wo_label;
    const newLabel = `${val} - ${wo.fields.date || localDate()}`;
    await airtableUpdate(TABLES.WORK_ORDERS, wo.id, { customer_name: val, wo_label: newLabel });
    if (oldLabel) {
      const parts = await airtableQuery(TABLES.PARTS_USED, `{wo_label} = "${oldLabel}"`);
      for (const p of parts) await airtableUpdate(TABLES.PARTS_USED, p.id, { wo_label: newLabel });
    }
    return `✓ Customer changed to ${val}.`;
  }
  if (key === 'PART') {
    const parts = await airtableQuery(TABLES.PARTS_USED, `{wo_label} = "${wo.fields.wo_label}"`);
    if (parts.length === 0) return 'No parts on the last job to fix.';
    if (parts.length > 1) return `That job has ${parts.length} parts — open your dashboard to edit a specific one (STATUS for the link).`;
    const s = await getSubscriberSettings(phone);
    const cost = parseFloat(val.replace(/[^0-9.]/g, '')) || 0;
    const markupPrice = Math.round(cost * (1 + (s.markup || 0) / 100) * 100) / 100;
    await airtableUpdate(TABLES.PARTS_USED, parts[0].id, { cost, markup_price: markupPrice });
    return `✓ Part cost set to $${cost.toFixed(2)}${markupPrice !== cost ? ` (bills at $${markupPrice.toFixed(2)})` : ''}.`;
  }
  return 'Fix options: FIX HOURS 2 · FIX CUSTOMER name · FIX JOB description · FIX PART 185';
}

// ----------------------------------------------------------------------------
// TECHS (shared-account model: each tech's phone maps to the owner's account)
// ----------------------------------------------------------------------------
function normalizePhone(raw) {
  const d = String(raw || '').replace(/[^0-9]/g, '');
  if (d.length === 10) return '+1' + d;
  if (d.length === 11 && d[0] === '1') return '+' + d;
  if (String(raw).trim().startsWith('+')) return String(raw).trim();
  return d ? '+' + d : '';
}

async function handleAddTech(command, accountPhone) {
  const m = command.match(/^\s*ADD\s+TECH\s+(\S+)\s+([\s\S]+)$/i);
  if (!m) return 'Usage: ADD TECH [phone] [name]. Example: ADD TECH 8055551234 Mike';
  const phone = normalizePhone(m[1]);
  const name = m[2].trim();
  if (!phone || phone.replace(/[^0-9]/g, '').length < 11) return 'That phone number looks off. Try: ADD TECH 8055551234 Mike';
  const existing = await airtableQuery(TABLES.TECHS, `{Phone} = "${phone}"`);
  if (existing.length > 0) {
    await airtableUpdate(TABLES.TECHS, existing[0].id, { Name: name, 'Account Phone': accountPhone, Active: true });
    return `✓ Updated ${name} (${phone}). They can text jobs into your account.`;
  }
  const id = await airtableCreate(TABLES.TECHS, { Phone: phone, Name: name, 'Account Phone': accountPhone, Active: true });
  if (!id) return 'Could not add that tech. Try again.';
  return `✓ Added ${name} (${phone}). They can now text jobs in — each tagged as theirs. No billing access.`;
}

async function handleRemoveTech(command, accountPhone) {
  const m = command.match(/^\s*REMOVE\s+TECH\s+(\S+)/i);
  if (!m) return 'Usage: REMOVE TECH [phone]';
  const phone = normalizePhone(m[1]);
  const techs = await airtableQuery(TABLES.TECHS, `AND({Phone} = "${phone}", {Account Phone} = "${accountPhone}")`);
  if (techs.length === 0) return `No tech found with ${phone}.`;
  await airtableUpdate(TABLES.TECHS, techs[0].id, { Active: false });
  return `✓ Removed ${techs[0].fields.Name || phone}. They can no longer text into your account.`;
}

async function handleListTechs(accountPhone, isOwner) {
  if (!isOwner) return 'Only the account owner can view the tech list.';
  const techs = await airtableQuery(TABLES.TECHS, `AND({Account Phone} = "${accountPhone}", {Active} = 1)`);
  if (techs.length === 0) return 'No techs added yet. Add one: ADD TECH 8055551234 Mike';
  const list = techs.map(t => `- ${t.fields.Name || 'Tech'} (${t.fields.Phone})`).join('\n');
  return `Your techs:\n${list}\n\nAdd: ADD TECH [phone] [name] · Remove: REMOVE TECH [phone]`;
}

// HISTORY [customer] — every past job + hours at that property (any tech).
async function handleHistory(command, accountPhone) {
  const customer = command.replace(/^\s*HISTORY\s*/i, '').trim();
  if (!customer) return 'Usage: HISTORY [customer]. Example: HISTORY Henderson';
  const esc = customer.replace(/"/g, '\\"').toLowerCase();
  const jobs = (await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${accountPhone}", FIND("${esc}", LOWER({customer_name})))`))
    .sort((a, b) => (b.fields.date || '').localeCompare(a.fields.date || ''));
  if (jobs.length === 0) return `No history found for "${customer}".`;
  const name = jobs[0].fields.customer_name || customer;
  const totalHours = jobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  const lines = jobs.slice(0, 6).map(j => {
    const who = j.fields.tech_name ? ` · ${j.fields.tech_name}` : '';
    return `${j.fields.date || '?'}: ${j.fields.job_type || 'service'} (${j.fields.labor_hours || 0}h)${who}`;
  }).join('\n');
  const more = jobs.length > 6 ? `\n+${jobs.length - 6} older` : '';
  return `${name} — ${jobs.length} job(s), ${totalHours}h total:\n${lines}${more}`;
}

// ============================================================================
// COMMAND HANDLERS
// Returns reply string. Does NOT call sendSMS.
// ============================================================================
async function handleCommand(command, subscriberPhone, subscriberName, isOwner = false) {
  const cmd = command.toUpperCase().trim();
  if (cmd === 'HELP' || cmd === 'COMMANDS' || cmd === 'INFO') {
    return 'Just text a job to log it. Commands: JOBS · PARTS · INVOICE [customer] [rate] · HISTORY [customer] · BRIEF · STATUS · SETTINGS · TECHS · ADD TECH [phone] [name] · UNDO · FIX · HELP';
  }
  if (cmd === 'SETTINGS' || cmd === 'SET' || cmd.startsWith('SET ')) {
    return await handleSettings(command, subscriberPhone);
  }
  if (cmd === 'UNDO') {
    return await handleUndo(subscriberPhone);
  }
  if (cmd === 'FIX' || cmd.startsWith('FIX ')) {
    return await handleFix(command, subscriberPhone);
  }
  if (cmd.startsWith('HISTORY')) {
    return await handleHistory(command, subscriberPhone);
  }
  if (cmd === 'TECHS') {
    return await handleListTechs(subscriberPhone, isOwner);
  }
  if (cmd.startsWith('ADD TECH')) {
    if (!isOwner) return 'Only the account owner can add techs.';
    return await handleAddTech(command, subscriberPhone);
  }
  if (cmd.startsWith('REMOVE TECH')) {
    if (!isOwner) return 'Only the account owner can remove techs.';
    return await handleRemoveTech(command, subscriberPhone);
  }
  if (cmd === 'JOBS') {
    const today = localDate();
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${today}")`);
    if (jobs.length === 0) return 'No jobs logged today yet.';
    const jobList = jobs.slice(0, 3).map(j =>
      `- ${j.fields.customer_name}: ${j.fields.job_type} (${j.fields.labor_hours || 0}h)`).join('\n');
    return `Today's jobs:\n${jobList}${jobs.length > 3 ? `\n+${jobs.length - 3} more` : ''}`;
  }
  if (cmd === 'PARTS') {
    const today = localDate();
    const parts = await airtableQuery(TABLES.PARTS_USED,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${today}")`);
    if (parts.length === 0) return 'No parts logged today.';
    const partList = parts.slice(0, 5).map(p =>
      `- ${p.fields.part_name} x${p.fields.quantity || 1} ($${(p.fields.cost || 0).toFixed(2)})`).join('\n');
    const total = parts.reduce((sum, p) => sum + (p.fields.cost || 0), 0);
    return `Today's parts:\n${partList}\nTotal: $${total.toFixed(2)}`;
  }
  if (cmd.startsWith('INVOICE')) {
    return await handleInvoiceCommand(command, subscriberPhone, subscriberName, isOwner);
  }
  if (cmd === 'BRIEF') {
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${localDate(-1)}")`);
    return await generateMorningBrief(jobs);
  }
  if (cmd === 'STATUS') {
    const s = await getSubscriberSettings(subscriberPhone);
    if (!s.recId) return 'Account not found.';
    return `${s.company || 'Your account'} — active.\nRate $${s.rate || 0}/hr · markup ${s.markup || 0}%\nDashboard: ${BASE_URL}/dashboard/${s.recId}`;
  }
  return 'Unknown command. Reply HELP for available commands.';
}

// ============================================================================
// INVOICING
// INVOICE <customer> [hourlyRate] -> builds invoice from logged jobs+parts,
// saves a Draft, returns a link to a review/send page. No payment processing.
// ============================================================================
async function handleInvoiceCommand(command, subscriberPhone, subscriberName, isOwner = true) {
  if (!isOwner) {
    const s = await getSubscriberSettings(subscriberPhone);
    if (!s.allowTechInvoicing) {
      return 'Only the account owner can send invoices. (The owner can allow techs with: SET TECHINVOICE on)';
    }
  }
  let rest = command.replace(/^\s*INVOICE\s*/i, '').trim();
  let rate = 0;
  const rateMatch = rest.match(/\s+\$?(\d+(?:\.\d{1,2})?)\s*$/);
  if (rateMatch) { rate = parseFloat(rateMatch[1]); rest = rest.slice(0, rateMatch.index).trim(); }
  const customer = rest;
  if (!customer) return 'Usage: INVOICE [customer] [hourly rate]. Example: INVOICE Smith 215';

  const esc = customer.replace(/"/g, '\\"').toLowerCase();
  const jobs = await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${subscriberPhone}", FIND("${esc}", LOWER({customer_name})))`);
  if (jobs.length === 0) return `No jobs found for "${customer}". Check the name and try again.`;

  if (!rate) rate = jobs.find(j => j.fields.labor_rate)?.fields.labor_rate || 0;
  const laborHours = jobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  if (laborHours > 0 && !rate) {
    return `Found ${jobs.length} job(s), ${laborHours}h labor for ${customer}. Add your hourly rate to price labor: INVOICE ${customer} 215`;
  }

  const parts = await airtableQuery(TABLES.PARTS_USED,
    `AND({subscriber_phone} = "${subscriberPhone}", FIND("${esc}", LOWER({wo_label})))`);

  const customerName = jobs[0].fields.customer_name || customer;
  const address = jobs.find(j => j.fields.customer_address)?.fields.customer_address || '';
  const lineJobs = jobs.map(j => ({
    date: j.fields.date || '', type: j.fields.job_type || 'Service',
    desc: j.fields.description || j.fields.job_type || 'Service', hours: j.fields.labor_hours || 0,
  }));
  const lineParts = parts.map(p => {
    const price = (p.fields.markup_price && p.fields.markup_price > 0) ? p.fields.markup_price : (p.fields.cost || 0);
    const qty = p.fields.quantity || 1;
    return { name: p.fields.part_name || 'Part', qty, price, total: price * qty };
  });
  const laborTotal = laborHours * rate;
  const partsTotal = lineParts.reduce((s, p) => s + p.total, 0);
  const total = laborTotal + partsTotal;

  const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${subscriberPhone}"`);
  const company = subs[0]?.fields['Company Name'] || subscriberName || 'My Company';

  const invNum = `INV-${localDate().replace(/-/g, '')}-${Math.floor(1000 + Math.random() * 9000)}`;
  const snapshot = {
    invNum, company, contractor: subscriberName, subscriberPhone,
    customer: customerName, address, date: localDate(),
    rate, laborHours, laborTotal, lineJobs, lineParts, partsTotal, total, status: 'Draft',
  };
  const recId = await airtableCreate(TABLES.INVOICES, {
    invoice_label: invNum, customer_name: customerName,
    wo_label: jobs[0].fields.wo_label || '', amount: total,
    status: 'Draft', subscriber_phone: subscriberPhone, notes: JSON.stringify(snapshot),
  });
  if (!recId) return 'Could not create the invoice. Please try again.';
  return `Invoice ${invNum} for ${customerName}: $${total.toFixed(2)} (${laborHours}h @ $${rate}/hr + $${partsTotal.toFixed(2)} parts). Review & send -> ${BASE_URL}/invoice/${recId}`;
}

const money = n => '$' + (Number(n) || 0).toFixed(2);

// Renders the invoice card (shared by the view page and the email body).
function renderInvoiceBody(s) {
  const jobRows = (s.lineJobs || []).map(j =>
    `<tr><td>${escapeHTML(j.date)}</td><td>${escapeHTML(j.desc)}</td><td class="r">${j.hours}h</td><td class="r">${money(s.rate)}/hr</td><td class="r">${money(j.hours * s.rate)}</td></tr>`).join('');
  const partRows = (s.lineParts || []).map(p =>
    `<tr><td colspan="2">${escapeHTML(p.name)}</td><td class="r">x${p.qty}</td><td class="r">${money(p.price)}</td><td class="r">${money(p.total)}</td></tr>`).join('');
  const pay = s.payment ? `<div class="pay"><div class="lbl">How to pay</div>${
    (s.payment.methods && s.payment.methods.length ? `<div>${s.payment.methods.map(escapeHTML).join(' · ')}</div>` : '')}${
    (s.payment.note ? `<div class="note">${escapeHTML(s.payment.note)}</div>` : '')}</div>` : '';
  return `<div class="inv">
    <div class="top"><div><div class="co">${escapeHTML(s.company)}</div><div class="mut">${escapeHTML(s.contractor || '')}</div></div>
      <div class="r"><div class="co">INVOICE</div><div class="mut">${escapeHTML(s.invNum)}</div><div class="mut">${escapeHTML(s.date)}</div></div></div>
    <div class="billto"><span class="lbl">Bill to</span> ${escapeHTML(s.customer)}${s.address ? ' · ' + escapeHTML(s.address) : ''}</div>
    <table><thead><tr><th>Date</th><th>Work performed</th><th class="r">Hrs/Qty</th><th class="r">Rate</th><th class="r">Amount</th></tr></thead>
      <tbody>${jobRows || ''}${partRows || ''}</tbody></table>
    <div class="tot"><div><span class="mut">Labor</span> ${money(s.laborTotal)}</div><div><span class="mut">Parts</span> ${money(s.partsTotal)}</div>
      <div class="grand">Total ${money(s.total)}</div></div>
    ${pay}
  </div>`;
}
function escapeHTML(str) {
  return String(str == null ? '' : str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
const INV_CSS = `body{font:15px/1.5 -apple-system,system-ui,sans-serif;color:#1a1a1a;background:#f4f0e8;margin:0;padding:24px}
.inv{max-width:640px;margin:0 auto;background:#fff;border:1px solid #e4ddcf;border-radius:14px;padding:26px}
.top{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:2px solid #c0532b;padding-bottom:14px}
.co{font-size:1.15rem;font-weight:800}.co:first-letter{color:#c0532b}.mut{color:#6b6256;font-size:.85rem}.r{text-align:right}
.billto{margin:14px 0;font-size:.95rem}.lbl{color:#6b6256;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse;margin:8px 0}th{font-size:.72rem;color:#6b6256;text-transform:uppercase;text-align:left;border-bottom:1px solid #eee;padding:7px 6px}
td{padding:8px 6px;border-bottom:1px solid #f1ece1;font-size:.9rem}
.tot{margin-top:14px;text-align:right}.tot>div{padding:2px 0}.grand{font-size:1.25rem;font-weight:800;border-top:2px solid #1a1a1a;display:inline-block;padding-top:8px;margin-top:6px}
.pay{margin-top:20px;background:#f7f2e8;border:1px solid #e4ddcf;border-radius:10px;padding:12px 14px}.pay .note{margin-top:4px;font-size:.9rem}`;

async function sendInvoiceEmail({ to, replyTo, fromName, subject, html }) {
  const key = process.env.RESEND_API_KEY;
  if (!key) return { ok: false, error: 'RESEND_API_KEY not set — add it in Render env after verifying fieldbrief.ai in Resend.' };
  const from = `${fromName} <${process.env.INVOICE_FROM || 'invoices@fieldbrief.ai'}>`;
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from, to: [to], reply_to: replyTo, subject, html }),
    });
    const data = await r.json();
    if (!r.ok) return { ok: false, error: data?.message || JSON.stringify(data) };
    return { ok: true, id: data.id };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SUPPORT TICKET
// Returns reply string. Does NOT call sendSMS.
// ============================================================================
async function handleSupportTicket(smsBody, subscriberPhone, subscriberName, ticketType) {
  try {
    const aiResponse = await generateAIResponse(smsBody, ticketType);
    let ticketStatus = 'Open';
    let ticketSubtype = 'Support Request';
    if (ticketType === 'cancel') { ticketStatus = 'Escalated'; ticketSubtype = 'Cancellation'; }
    else if (ticketType === 'billing') { ticketStatus = 'Escalated'; ticketSubtype = 'Billing Question'; }
    else if (ticketType === 'feature_request') { ticketSubtype = 'Feature Request'; }
    await airtableCreate(TABLES.SUPPORT_TICKETS, {
      ticket_label: `${ticketSubtype} - ${subscriberName} - ${localDate()}`,
      type: ticketSubtype,
      status: ticketStatus,
      subscriber_phone: subscriberPhone,
      subscriber_name: subscriberName,
      description: smsBody,
      ai_response: aiResponse,
      created_date: localDate(),
    });
    return aiResponse;
  } catch (error) {
    console.error('Support ticket error:', error);
    return 'Thanks for reaching out. We\'ll review this soon.';
  }
}

async function logSMS(fromNumber, body, intent, response) {
  try {
    await airtableCreate(TABLES.SMS_LOG, {
      msg_label: `${intent} - ${fromNumber} - ${new Date().toISOString()}`,
      direction: 'inbound',
      body: body,
      from_number: fromNumber,
      to_number: TWILIO_PHONE_NUMBER,
      timestamp: new Date().toISOString(),
      parsed_intent: intent,
      subscriber_phone: fromNumber,
    });
  } catch (error) { console.error('SMS log error:', error); }
}

// ============================================================================
// HTTP ROUTES
// ============================================================================
app.get('/', (req, res) => {
  res.status(200).send('FieldBrief webhook is running');
});

// ----------------------------------------------------------------------------
// /test — live web tester. Drives the REAL /sms pipeline over HTTP so the
// product can be exercised end-to-end while carrier SMS delivery is still
// gated (A2P 10DLC / toll-free verification). Same logic as a real inbound
// text; only the SMS transport is bypassed.
// ----------------------------------------------------------------------------
app.get('/test', (req, res) => {
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FieldBrief — Live Tester</title>
<style>
  :root{--ink:#1a1a1a;--paper:#f4f0e8;--accent:#c0532b;--mut:#6b6256}
  *{box-sizing:border-box}body{margin:0;font:16px/1.5 -apple-system,system-ui,sans-serif;background:var(--paper);color:var(--ink)}
  .wrap{max-width:560px;margin:0 auto;padding:24px 18px 60px}
  h1{font-size:1.5rem;margin:0 0 2px}.h1 b{color:var(--accent)}
  .sub{color:var(--mut);font-size:.86rem;margin:0 0 18px}
  .chat{background:#fff;border:1px solid #e4ddcf;border-radius:14px;min-height:240px;padding:14px;overflow:auto}
  .msg{margin:8px 0;display:flex}.me{justify-content:flex-end}
  .bubble{max-width:80%;padding:9px 13px;border-radius:14px;white-space:pre-wrap;font-size:.92rem}
  .me .bubble{background:var(--accent);color:#fff;border-bottom-right-radius:4px}
  .bot .bubble{background:#efe9dd;color:var(--ink);border-bottom-left-radius:4px}
  .sys{color:var(--mut);font-size:.78rem;text-align:center;margin:6px 0}
  form{display:flex;gap:8px;margin-top:12px}
  input[type=text]{flex:1;padding:12px;border:1px solid #d8cfbd;border-radius:10px;font-size:1rem;background:#fff}
  button{padding:12px 16px;border:0;border-radius:10px;background:var(--accent);color:#fff;font-weight:600;font-size:1rem;cursor:pointer}
  button:disabled{opacity:.5}
  .num{font-size:.8rem;color:var(--mut);margin:10px 0 0}.num input{font:inherit;border:1px solid #d8cfbd;border-radius:7px;padding:4px 7px;width:150px}
  .ex{margin:14px 0 0;font-size:.8rem;color:var(--mut)}.ex code{background:#ece5d6;padding:2px 6px;border-radius:5px;cursor:pointer;display:inline-block;margin:3px 4px 0 0}
</style></head><body><div class="wrap">
<h1 class="h1">Field<b>Brief</b> · live tester</h1>
<p class="sub">Texts the real backend — Claude parses it and writes to your Airtable. (SMS transport bypassed until carrier verification clears.)</p>
<div class="chat" id="chat"><div class="sys">Type a job below, or tap an example. Replies are exactly what the SMS line would send.</div></div>
<form id="f"><input type="text" id="b" placeholder="e.g. Smith 12 Main St, boiler tune-up, 2hr, $45 filter" autocomplete="off" autofocus><button id="send">Send</button></form>
<p class="num">From number: <input type="text" id="from" value="+18054527511"> <span id="who"></span></p>
<div class="ex">Try:
<code>JOBS</code><code>PARTS</code><code>BRIEF</code><code>HELP</code>
<code>Garcia 88 Oak Ave Goleta, WM boiler no-heat, replaced igniter 1.5hr, $40 igniter from Ferguson</code></div>
</div><script>
const chat=document.getElementById('chat'),f=document.getElementById('f'),b=document.getElementById('b'),send=document.getElementById('send'),from=document.getElementById('from');
function add(t,cls){const d=document.createElement('div');d.className='msg '+cls;d.innerHTML='<div class="bubble"></div>';d.firstChild.textContent=t;chat.appendChild(d);chat.scrollTop=chat.scrollHeight;}
function sys(t){const d=document.createElement('div');d.className='sys';d.textContent=t;chat.appendChild(d);chat.scrollTop=chat.scrollHeight;}
document.querySelectorAll('.ex code').forEach(c=>c.onclick=()=>{b.value=c.textContent;b.focus();});
f.onsubmit=async e=>{e.preventDefault();const body=b.value.trim();if(!body)return;add(body,'me');b.value='';send.disabled=true;
 try{const r=await fetch('/sms',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({From:from.value.trim(),To:'+18053104809',Body:body})});
  const xml=await r.text();const m=xml.match(/<Message>([\\s\\S]*?)<\\/Message>/);
  const txt=m?m[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&apos;/g,"'"):'(no reply)';
  add(txt,'bot');}catch(err){sys('Error: '+err.message);}finally{send.disabled=false;b.focus();}};
</script></body></html>`);
});

// ----------------------------------------------------------------------------
// INVOICE PAGES
// ----------------------------------------------------------------------------
async function getInvoice(recId) {
  const rec = await airtableRequest('GET', TABLES.INVOICES, null, recId);
  if (!rec || !rec.fields) return null;
  let snap = {};
  try { snap = JSON.parse(rec.fields.notes || '{}'); } catch { snap = {}; }
  return { rec, snap };
}

// Contractor's review & send page
app.get('/invoice/:id', async (req, res) => {
  const inv = await getInvoice(req.params.id);
  if (!inv) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Invoice not found.</p>');
  const { rec, snap } = inv;
  const sent = (rec.fields.status && rec.fields.status !== 'Draft');
  const id = req.params.id;
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(snap.invNum || 'Invoice')} — review & send</title><style>${INV_CSS}
.bar{max-width:640px;margin:0 auto 14px;display:flex;justify-content:space-between;align-items:center}
.bar h1{font-size:1.1rem;margin:0}.badge{font-size:.72rem;padding:3px 9px;border-radius:20px;background:#efe9dd;color:#6b6256}
.send{max-width:640px;margin:16px auto 0;background:#fff;border:1px solid #e4ddcf;border-radius:14px;padding:20px}
.send h2{font-size:1rem;margin:0 0 12px}label{display:block;font-size:.8rem;color:#6b6256;margin:12px 0 4px}
input[type=email],input[type=text],textarea{width:100%;padding:10px;border:1px solid #d8cfbd;border-radius:9px;font:inherit;box-sizing:border-box}
.methods{display:flex;flex-wrap:wrap;gap:10px;margin-top:6px}.methods label{display:flex;align-items:center;gap:6px;margin:0;color:#1a1a1a;font-size:.9rem;background:#f7f2e8;padding:7px 11px;border-radius:8px;border:1px solid #e4ddcf;cursor:pointer}
button{margin-top:16px;width:100%;padding:13px;border:0;border-radius:10px;background:#c0532b;color:#fff;font-weight:700;font-size:1rem;cursor:pointer}
.ok{max-width:640px;margin:0 auto 14px;background:#e7f3e7;border:1px solid #bcd9bc;color:#2c6b2c;border-radius:10px;padding:12px 14px;font-size:.9rem}
.printbtn{background:#1a1a1a}</style></head><body>
<div class="bar"><h1>Review & send</h1><span class="badge">${escapeHTML(rec.fields.status || 'Draft')}</span></div>
${sent ? `<div class="ok">✓ Sent to ${escapeHTML(snap.customerEmail || 'customer')}${rec.fields.sent_date ? ' on ' + escapeHTML(rec.fields.sent_date) : ''}. You can resend with new details below.</div>` : ''}
${renderInvoiceBody(snap)}
<form class="send" method="POST" action="/invoice/${id}/send" onsubmit="return confirm('Send this invoice to your customer now?')">
  <h2>Email this invoice to your customer</h2>
  <label>Customer email *</label><input type="email" name="customer_email" required placeholder="customer@email.com" value="${escapeHTML(snap.customerEmail || '')}">
  <label>Your email (so their reply reaches you) *</label><input type="email" name="reply_to" required placeholder="you@yourcompany.com" value="${escapeHTML(snap.replyTo || '')}">
  <label>Payment methods you accept (shown on the invoice)</label>
  <div class="methods">
    ${['Cash', 'Check', 'Venmo', 'Zelle', 'Card in person', 'Other'].map(m =>
      `<label><input type="checkbox" name="methods" value="${m}" ${(snap.payment?.methods || []).includes(m) ? 'checked' : ''}>${m}</label>`).join('')}
  </div>
  <label>Payment details / note</label><textarea name="pay_note" rows="2" placeholder="e.g. Venmo @your-handle · Checks payable to Your Company · Due in 14 days">${escapeHTML(snap.payment?.note || '')}</textarea>
  <label style="display:flex;align-items:flex-start;gap:8px;margin-top:16px;color:#1a1a1a;font-size:.9rem"><input type="checkbox" name="confirm_match" value="1" required style="margin-top:3px;flex:none">I've checked these line items against the work I logged — they're correct.</label>
  <button type="submit">Send invoice to customer</button>
</form>
<form class="send" method="GET" action="/invoice/${id}/view" target="_blank" style="background:none;border:0;padding:8px 0">
  <button class="printbtn" type="submit">Preview what the customer sees</button>
</form>
</body></html>`);
});

// Send action
app.post('/invoice/:id/send', async (req, res) => {
  const id = req.params.id;
  const inv = await getInvoice(id);
  if (!inv) return res.status(404).send('Invoice not found.');
  const { snap } = inv;
  const customerEmail = (req.body.customer_email || '').trim();
  const replyTo = (req.body.reply_to || '').trim();
  const methods = [].concat(req.body.methods || []);
  const payNote = (req.body.pay_note || '').trim();
  if (!customerEmail) return res.status(400).send('Customer email required.');

  // Double-check: re-pull the logged work and confirm the invoice still matches.
  // Blocks sending if a job/part was edited or removed after this draft was built.
  const esc = (snap.customer || '').replace(/"/g, '\\"').toLowerCase();
  const curJobs = await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${snap.subscriberPhone}", FIND("${esc}", LOWER({customer_name})))`);
  const curParts = await airtableQuery(TABLES.PARTS_USED,
    `AND({subscriber_phone} = "${snap.subscriberPhone}", FIND("${esc}", LOWER({wo_label})))`);
  const curHours = curJobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  const curPartsTotal = curParts.reduce((s, p) =>
    s + ((p.fields.markup_price && p.fields.markup_price > 0) ? p.fields.markup_price : (p.fields.cost || 0)) * (p.fields.quantity || 1), 0);
  const curTotal = curHours * (snap.rate || 0) + curPartsTotal;
  if (Math.abs(curTotal - (snap.total || 0)) > 0.01) {
    return res.status(200).type('html').send(`<div style="max-width:560px;margin:40px auto;font:15px/1.5 sans-serif;padding:24px;border:1px solid #e4b4b4;background:#fbeaea;border-radius:12px;color:#8a2b2b">
      <h2 style="margin:0 0 8px">Hold on — this doesn't match the logged work anymore</h2>
      <p>This invoice was built for <b>${money(snap.total)}</b>, but the jobs logged for ${escapeHTML(snap.customer)} now total <b>${money(curTotal)}</b> (a job or part changed since you created it).</p>
      <p>Nothing was sent. Rebuild a fresh invoice — text <b>INVOICE ${escapeHTML(snap.customer)}</b> again — then send that one.</p></div>`);
  }

  snap.customerEmail = customerEmail;
  snap.replyTo = replyTo;
  snap.payment = { methods, note: payNote };
  snap.status = 'Sent';

  const viewUrl = `${BASE_URL}/invoice/${id}/view`;
  const emailHtml = `<!doctype html><html><head><meta charset="utf-8"><style>${INV_CSS}</style></head><body>
${renderInvoiceBody(snap)}
<p style="max-width:640px;margin:16px auto;color:#6b6256;font:13px sans-serif;text-align:center">
View this invoice online: <a href="${viewUrl}">${viewUrl}</a></p></body></html>`;

  const result = await sendInvoiceEmail({
    to: customerEmail, replyTo: replyTo || undefined, fromName: snap.company || 'FieldBrief',
    subject: `Invoice ${snap.invNum} from ${snap.company || 'your service provider'}`, html: emailHtml,
  });

  // Persist regardless; record send status in notes for traceability.
  snap.lastSend = result.ok ? { ok: true, id: result.id } : { ok: false, error: result.error };
  await airtableUpdate(TABLES.INVOICES, id, {
    status: result.ok ? 'Sent' : 'Draft',
    sent_date: result.ok ? localDate() : undefined,
    payment_method: methods[0] || undefined,
    notes: JSON.stringify(snap),
  });

  if (!result.ok) {
    return res.status(200).type('html').send(`<div style="max-width:560px;margin:40px auto;font:15px/1.5 sans-serif;padding:24px;border:1px solid #e4b4b4;background:#fbeaea;border-radius:12px;color:#8a2b2b">
      <h2 style="margin:0 0 8px">Couldn't send yet</h2>
      <p>${escapeHTML(result.error)}</p>
      <p style="color:#6b6256">The invoice is saved. Once email is set up, reopen the link and hit send again.</p>
      <p><a href="/invoice/${id}">← back to invoice</a></p></div>`);
  }
  res.type('html').send(`<div style="max-width:560px;margin:40px auto;font:15px/1.5 sans-serif;padding:24px;border:1px solid #bcd9bc;background:#e7f3e7;border-radius:12px;color:#2c6b2c">
    <h2 style="margin:0 0 8px">✓ Invoice sent</h2>
    <p>${escapeHTML(snap.invNum)} emailed to <b>${escapeHTML(customerEmail)}</b> for ${money(snap.total)}.</p>
    <p><a href="/invoice/${id}/view" target="_blank">View what the customer received →</a></p></div>`);
});

// Customer-facing view (what the email links to)
app.get('/invoice/:id/view', async (req, res) => {
  const inv = await getInvoice(req.params.id);
  if (!inv) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Invoice not found.</p>');
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(inv.snap.invNum || 'Invoice')}</title><style>${INV_CSS}
.pb{display:block;max-width:640px;margin:14px auto 0;text-align:center}.pb button{background:#1a1a1a;color:#fff;border:0;border-radius:9px;padding:10px 18px;font:inherit;cursor:pointer}
@media print{.pb{display:none}}</style></head><body>
${renderInvoiceBody(inv.snap)}
<div class="pb"><button onclick="window.print()">Print / Save PDF</button></div>
</body></html>`);
});

// Read-only subscriber dashboard (link surfaced via STATUS)
app.get('/dashboard/:id', async (req, res) => {
  const sub = await airtableRequest('GET', TABLES.SUBSCRIBERS, null, req.params.id);
  if (!sub || !sub.fields) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Not found.</p>');
  const phone = sub.fields['Phone Number'] || '';
  const company = sub.fields['Company'] || sub.fields['Company Name'] || sub.fields['Full Name'] || 'Your business';
  const rate = sub.fields['Hourly Rate'] || 0;
  const markup = sub.fields['Markup Pct'] || 0;
  const jobs = (await airtableQuery(TABLES.WORK_ORDERS, `{subscriber_phone} = "${phone}"`))
    .sort((a, b) => (b.fields.date || '').localeCompare(a.fields.date || ''));
  const invoices = (await airtableQuery(TABLES.INVOICES, `{subscriber_phone} = "${phone}"`))
    .sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  const weekAgo = localDate(-7);
  const weekJobs = jobs.filter(j => (j.fields.date || '') >= weekAgo);
  const weekHours = weekJobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  const jobRows = jobs.slice(0, 15).map(j =>
    `<tr><td>${escapeHTML(j.fields.date || '')}</td><td>${escapeHTML(j.fields.customer_name || '')}</td><td>${escapeHTML(j.fields.job_type || '')}</td><td class="r">${j.fields.labor_hours || 0}h</td></tr>`).join('') || '<tr><td colspan="4" class="mut">No jobs yet.</td></tr>';
  const invRows = invoices.slice(0, 15).map(i =>
    `<tr><td>${escapeHTML(i.fields.invoice_label || '')}</td><td>${escapeHTML(i.fields.customer_name || '')}</td><td class="r">${money(i.fields.amount || 0)}</td><td>${escapeHTML(i.fields.status || '')}</td><td><a href="/invoice/${i.id}">open</a></td></tr>`).join('') || '<tr><td colspan="5" class="mut">No invoices yet.</td></tr>';
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(company)} — FieldBrief</title><style>${INV_CSS}
.dash{max-width:720px;margin:0 auto;padding:20px}
.cards{display:flex;gap:10px;margin:14px 0}.card{flex:1;background:#fff;border:1px solid #e4ddcf;border-radius:12px;padding:14px;text-align:center}
.card .n{font-size:1.5rem;font-weight:800}.card .l{color:#6b6256;font-size:.78rem}
h2{font-size:1rem;margin:22px 0 6px}.sec{background:#fff;border:1px solid #e4ddcf;border-radius:12px;padding:4px 14px}
a{color:#c0532b;text-decoration:none}</style></head><body><div class="dash">
<div style="border-bottom:2px solid #c0532b;padding-bottom:12px;display:flex;justify-content:space-between;align-items:flex-start">
  <div class="co" style="font-size:1.3rem">${escapeHTML(company)}</div>
  <div class="mut r">$${rate}/hr · ${markup}% markup</div></div>
<div class="cards">
  <div class="card"><div class="n">${weekJobs.length}</div><div class="l">jobs this week</div></div>
  <div class="card"><div class="n">${weekHours}h</div><div class="l">hours this week</div></div>
  <div class="card"><div class="n">${invoices.length}</div><div class="l">invoices</div></div>
</div>
<h2>Recent jobs</h2><div class="sec"><table><thead><tr><th>Date</th><th>Customer</th><th>Type</th><th class="r">Hrs</th></tr></thead><tbody>${jobRows}</tbody></table></div>
<h2>Invoices</h2><div class="sec"><table><thead><tr><th>#</th><th>Customer</th><th class="r">Amount</th><th>Status</th><th></th></tr></thead><tbody>${invRows}</tbody></table></div>
<p class="mut" style="margin-top:20px;font-size:.8rem">Read-only snapshot · text your line to log jobs or send invoices</p>
</div></body></html>`);
});

app.post('/sms', async (req, res) => {
  const fromNumber = req.body.From || '';
  const smsBody = req.body.Body || '';
  const upper = smsBody.trim().toUpperCase();
  console.log(`SMS from ${fromNumber}: ${smsBody}`);

  // --------------------------------------------------------------------------
  // UNIVERSAL INTENTS — handled BEFORE the subscriber lookup so they work
  // for everyone, signed up or not. This is critical for DEMO (sign-up flow)
  // and STOP (compliance).
  // --------------------------------------------------------------------------
  if (upper === 'STOP' || upper === 'STOPALL' || upper === 'UNSUBSCRIBE' ||
      upper === 'CANCEL' || upper === 'END' || upper === 'QUIT') {
    const msg = 'You have been unsubscribed. Reply START to resubscribe.';
    logSMS(fromNumber, smsBody, 'stop', msg);
    return replyTwiML(res, msg);
  }

  if (upper === 'START' || upper === 'UNSTOP' || upper === 'YES') {
    const msg = 'You are resubscribed to FieldBrief. Reply HELP for commands.';
    logSMS(fromNumber, smsBody, 'start', msg);
    return replyTwiML(res, msg);
  }

  if (upper === 'DEMO') {
    const msg = 'Welcome to the FieldBrief demo! Try texting a job like: "Smith 123 Main St, WM boiler tune-up, 2hr, $45 filter". Reply HELP for commands. Sign up at fieldbrief.ai';
    logSMS(fromNumber, smsBody, 'demo', msg);
    return replyTwiML(res, msg);
  }

  // HELP is intentionally NOT handled here — it falls through to the account
  // lookup so signed-up users get the full command list from handleCommand,
  // and non-subscribers get the signup prompt.

  // --------------------------------------------------------------------------
  // ACCOUNT LOOKUP — sender is either the account owner (Subscribers) or one
  // of their techs (Techs). Resolve to the account phone so all data lands in
  // one place; actorName tags who actually did the work.
  // --------------------------------------------------------------------------
  let accountPhone, actorName, isOwner = false;
  const owner = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${fromNumber}"`);
  if (owner.length > 0) {
    accountPhone = fromNumber;
    actorName = owner[0].fields['Full Name'] || 'Owner';
    isOwner = true;
  } else {
    const tech = await airtableQuery(TABLES.TECHS, `AND({Phone} = "${fromNumber}", {Active} = 1)`);
    if (tech.length > 0) {
      accountPhone = tech[0].fields['Account Phone'] || '';
      actorName = tech[0].fields['Name'] || 'Tech';
    }
  }
  if (!accountPhone) {
    const signupPrompt = 'Hey! You\'re not set up yet. Reply DEMO to try it free, or visit fieldbrief.ai to get started.';
    logSMS(fromNumber, smsBody, 'signup_prompt', signupPrompt);
    return replyTwiML(res, signupPrompt);
  }

  try {
    let response = '', intent;
    // Explicit keyword commands route deterministically — skip the AI classifier.
    const firstWord = upper.split(/\s+/)[0];
    const KEYWORDS = ['JOBS', 'PARTS', 'INVOICE', 'BRIEF', 'STATUS', 'SETTINGS', 'SET', 'UNDO', 'FIX', 'COMMANDS', 'TECHS', 'HISTORY', 'HELP', 'INFO'];
    const isCommand = KEYWORDS.includes(firstWord) || /^(ADD|REMOVE)\s+TECH\b/i.test(smsBody.trim());
    if (isCommand) {
      intent = 'command';
      response = await handleCommand(smsBody, accountPhone, actorName, isOwner);
    } else {
      intent = await classifyIntent(smsBody);
      console.log(`Intent: ${intent}`);
      if (intent === 'job_log') {
        response = await handleJobLog(smsBody, accountPhone, actorName);
      } else if (intent === 'command') {
        response = await handleCommand(smsBody, accountPhone, actorName, isOwner);
      } else if (intent === 'support') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'support');
      } else if (intent === 'feature_request') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'feature_request');
      } else if (intent === 'cancel') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'cancel');
      } else if (intent === 'billing') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'billing');
      } else {
        response = 'Thanks for the message. How can I help? Reply HELP for commands.';
      }
    }
    logSMS(fromNumber, smsBody, intent, response);
    return replyTwiML(res, response);
  } catch (error) {
    console.error('SMS processing error:', error);
    const errorResponse = 'Sorry, I encountered an error. Please try again.';
    logSMS(fromNumber, smsBody, 'error', errorResponse);
    return replyTwiML(res, errorResponse);
  }
});

app.post('/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET || '');
  } catch (error) {
    res.status(400).send(`Webhook Error: ${error.message}`); return;
  }
  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const metadata = session.metadata || {};
    try {
      await airtableCreate(TABLES.SUBSCRIBERS, {
        'Full Name': metadata.name || 'New Subscriber',
        'Phone Number': metadata.phone || '',
        'Status': 'Active',
        'Company Name': metadata.company || '',
        'Trade': metadata.trade || '',
        'Join Date': localDate(),
      });
      if (metadata.phone) {
        sendSMS(metadata.phone, `Welcome to FieldBrief! You're all set. Reply HELP for available commands.`);
      }
      res.json({ received: true });
    } catch (error) {
      console.error('Stripe onboarding error:', error);
      res.status(500).json({ error: 'Failed to create subscriber' });
    }
  } else {
    res.json({ received: true });
  }
});

// ============================================================================
// CRON: MORNING BRIEF AT 6 AM ET
// ============================================================================
cron.schedule('0 6 * * *', async () => {
  console.log('Running morning brief...');
  try {
    const subscribers = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    for (const sub of subscribers) {
      const phone = sub.fields['Phone Number'];
      if (!phone) continue;
      const jobs = await airtableQuery(TABLES.WORK_ORDERS,
        `AND({subscriber_phone} = "${phone}", DATESTR({date}) = "${localDate(-1)}")`);
      const brief = await generateMorningBrief(jobs);
      sendSMS(phone, brief);
      console.log(`Brief sent to ${phone}`);
    }
  } catch (error) { console.error('Morning brief error:', error); }
}, { timezone: 'America/New_York' });

// ============================================================================
// KEEP-ALIVE: Ping self every 4 min to prevent Render free tier spin-down
// ============================================================================
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || 'https://fieldbrief-webhook.onrender.com';
cron.schedule('*/4 * * * *', () => {
  fetch(RENDER_URL)
    .then(res => console.log(`Keep-alive ping: ${res.status}`))
    .catch(err => console.log('Keep-alive ping failed:', err.message));
});

// ============================================================================
// START SERVER
// ============================================================================
app.listen(PORT, () => {
  console.log(`FieldBrief webhook running on port ${PORT}`);
});
