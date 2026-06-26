import express from 'express';
import { Anthropic } from '@anthropic-ai/sdk';
import twilio from 'twilio';
import Stripe from 'stripe';
import cron from 'node-cron';
import crypto from 'crypto';

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

// Secrets come from the environment ONLY — never hardcode a fallback token.
// Set AIRTABLE_TOKEN in Render → Environment.
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appbcR8hJtuXwpEI8';
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER || '+18559835461';
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.RENDER_EXTERNAL_URL || 'https://fieldbrief-webhook.onrender.com';

if (!AIRTABLE_TOKEN) {
  console.error('FATAL: AIRTABLE_TOKEN is not set. Add it in Render → Environment. Airtable reads/writes will fail until it is.');
}

// ----------------------------------------------------------------------------
// PER-RECORD ACCESS TOKENS — every /dashboard and /invoice link carries a
// token derived from the record id + a server secret (DASH_SECRET). Without a
// valid token the page 404s, so record ids can't be guessed/enumerated (closes
// the IDOR) and the dashboard authenticates as ITS OWN account without leaking
// a master /sms bypass.
// ----------------------------------------------------------------------------
const DASH_SECRET = process.env.DASH_SECRET || '';
if (!DASH_SECRET) {
  console.error('WARNING: DASH_SECRET is not set — dashboard/invoice links will not validate. Set it in Render → Environment.');
}
function recToken(id) {
  return crypto.createHmac('sha256', DASH_SECRET).update(String(id)).digest('base64url').slice(0, 27);
}
function validToken(id, t) {
  if (!DASH_SECRET || !t) return false;
  const a = Buffer.from(recToken(id));
  const b = Buffer.from(String(t));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
// Query-string suffix for building links, e.g. `${BASE_URL}/dashboard/${id}${tq(id)}`
function tq(id) { return `?t=${recToken(id)}`; }

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
  FEATURES: 'tblC0012pvnfs08oK',
  SCHEDULE: 'tblErt1aDGhtbe9pJ',
  PROPOSALS: process.env.PROPOSALS_TABLE_ID || 'tblkQ9fXP13KVvI7c',
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
// Direct Anthropic REST call — avoids the old bundled SDK not reading newer
// models' responses. Returns the concatenated text, or throws on API error.
const CLAUDE_MODEL = 'claude-sonnet-4-6';
async function claudeText({ system, content, max_tokens = 400 }) {
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify({ model: CLAUDE_MODEL, max_tokens, system, messages: [{ role: 'user', content }] }),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data?.error?.message || JSON.stringify(data));
  return (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
}

// Natural-language router: understand a plain-English text and turn it into an
// action (+ normalized command) so users never have to memorize commands.
async function routeIntent(smsBody) {
  try {
    const text = await claudeText({
      max_tokens: 200,
      content: smsBody,
      system: `You route a contractor's plain-English text in a field-service tool ("run your business by text"). Pick the action and rewrite it into a normalized command. Return ONLY JSON: {"action":"...","command":"..."}.

action="log" — they're reporting work done / a job to record (a customer or property, address, service, parts, or hours). command = the original text.

action="command" — they want one of these; set command to the EXACT normalized form:
- today's jobs -> "JOBS"
- parts used -> "PARTS"
- past work at a place -> "HISTORY <address or customer>"
- create/send an invoice (for work already done) -> "INVOICE <customer or address>"
- create/send a quote/estimate/proposal (for work NOT yet done, with prices) -> "PROPOSAL <customer>: <scope + prices verbatim>"
- mark an invoice paid -> "PAID <customer or invoice#>"
- who owes me / outstanding -> "UNPAID"
- remind/resend an invoice -> "RESEND <customer>"
- save a note about a customer -> "NOTE <customer>: <the note>"
- add a worker/tech -> "ADD TECH <phone digits> <name>"
- remove a tech -> "REMOVE TECH <phone>"
- list techs -> "TECHS"
- assign today's jobs to techs -> "SCHEDULE <the assignments, verbatim>"
- send the crew their schedule -> "DISPATCH"
- change a setting -> "SET <KEY> <value>" (KEY one of RATE, MARKUP, COMPANY, EMAIL, LICENSE, PAY, TECHINVOICE)
- view settings -> "SETTINGS"
- account/dashboard -> "STATUS"
- undo last entry -> "UNDO"
- fix last entry -> "FIX <HOURS|CUSTOMER|JOB|PART> <value>"
- morning summary -> "BRIEF"
- help / what can you do -> "HELP"

action="support" — a question, problem, or complaint (command = original).
action="cancel" — wants to cancel/unsubscribe (command = original).
action="general" — ONLY greetings/thanks/unclear with no work content (command = original).

For SET, always normalize to "SET <KEY> <value>" with the bare KEY (drop filler like "my"/"to"/"is"): "set my rate to 195"->{"action":"command","command":"SET RATE 195"}; "my markup is 30%"->{"action":"command","command":"SET MARKUP 30"}; "change my company name to Smith Plumbing"->{"action":"command","command":"SET COMPANY Smith Plumbing"}.
If unsure between log and general, choose log when there's any work or customer content. Examples: "smith paid up"->{"action":"command","command":"PAID Smith"}; "add my guy mike 805 555 1234"->{"action":"command","command":"ADD TECH 8055551234 Mike"}; "who owes me"->{"action":"command","command":"UNPAID"}; "what'd we do at 412 state"->{"action":"command","command":"HISTORY 412 State"}; "gate code at smith is 1234"->{"action":"command","command":"NOTE Smith: gate code 1234"}; "send smith their bill"->{"action":"command","command":"INVOICE Smith"}; "quote the jones job to replace their water heater, 6hr labor $900, heater and parts $1100"->{"action":"command","command":"PROPOSAL Jones: replace water heater, 6hr labor $900, heater and parts $1100"}; "did the henderson boiler 2hr replaced igniter $40"->{"action":"log","command":"did the henderson boiler 2hr replaced igniter $40"}. Return ONLY the JSON.`,
    });
    const m = text.match(/\{[\s\S]*\}/);
    const r = JSON.parse(m ? m[0] : text);
    return { action: (r.action || 'general').toLowerCase().trim(), command: r.command || smsBody };
  } catch (error) { console.error('routeIntent error:', error); return { action: 'general', command: smsBody }; }
}

async function parseJobLog(smsBody, subscriberName) {
  try {
    const responseText = await claudeText({
      max_tokens: 1500,
      content: smsBody,
      system: `You are a job log parser for field service contractors. Extract structured data from casual SMS messages.
Ignore any leading filler like "add job", "job for", "log", "did", "completed".
The contractor ${subscriberName} is reporting work they completed today.
Parse the text into this JSON structure (include only fields that are present):
{
  "customer": { "name": "", "first_name": "", "last_name": "", "address": "", "city": "", "state": "" },
  "equipment": { "category": "", "manufacturer": "", "model": "", "serial_number": "", "fuel_type": "" },
  "work_order": { "job_type": "", "description": "", "labor_hours": 0, "status": "Completed" },
  "parts": [{ "name": "", "supplier": "", "cost": 0, "quantity": 1, "category": "" }]
}
Common abbreviations: WM=Weil-McLain, circ=circulator pump, EWT=electric water tank, ASHP=air-source heat pump, RTU=rooftop unit.
If the customer is an individual person, set first_name and last_name (and "name" = the full name). If it's a business/property, put it in "name" and leave first_name/last_name blank.
Handle incomplete info gracefully. Multiple jobs in one text are OK. Respond with ONLY valid JSON.`,
    });
    const fenced = responseText.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
    const raw = fenced ? fenced[1] : (responseText.match(/(\[[\s\S]*\]|\{[\s\S]*\})/)?.[1] || responseText);
    const parsed = JSON.parse(raw);
    // The model may return a single object or an array of jobs — normalize to one.
    return Array.isArray(parsed) ? (parsed[0] || null) : parsed;
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
    const text = await claudeText({ max_tokens: 100, content: smsBody, system: prompts[ticketType] || prompts.support });
    return text || 'Thanks for reaching out.';
  } catch (error) { return 'Thanks for reaching out. We\'ll review this and get back to you.'; }
}

async function generateMorningBrief(yesterdayJobs) {
  try {
    const jobSummary = yesterdayJobs.map(j =>
      `- ${j.fields.customer_name || 'Unknown'}: ${j.fields.job_type || 'Service'} (${j.fields.labor_hours || 0}h)`
    ).join('\n');
    const text = await claudeText({ max_tokens: 200, content: `Yesterday's jobs:\n${jobSummary || 'No jobs logged'}`, system: 'Create a short motivational morning summary of yesterday\'s work for a field service contractor. 2-3 sentences.' });
    return text || 'Good morning! Have a productive day.';
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
          first_name: parsedData.customer.first_name || '',
          last_name: parsedData.customer.last_name || '',
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
  const view = `Your settings:\nCompany: ${s.company || '—'}\nRate: $${s.rate || 0}/hr\nMarkup: ${s.markup || 0}%\nLicense: ${s.license || '—'}\nEmail: ${s.email || '—'}\nPay note: ${s.payNote || '—'}\nTech invoicing: ${s.allowTechInvoicing ? 'on' : 'off (owner only)'}\n\nChange: SET RATE 215 · SET MARKUP 30 · SET COMPANY name · SET EMAIL you@co.com · SET PAY note · SET TECHINVOICE on/off`;
  const cmd = command.trim();
  if (/^settings?$/i.test(cmd) || /^set$/i.test(cmd)) return view;
  const body = cmd.replace(/^\s*set\s+/i, '').trim();
  // Find the setting keyword anywhere (tolerates filler like "my", "to", "is").
  const km = body.match(/\b(RATE|MARKUP|COMPANY|LICENSE|EMAIL|PAY|TECHINVOICE)\b/i);
  if (!km) return view;
  const key = km[1].toUpperCase();
  const map = { RATE: 'Hourly Rate', MARKUP: 'Markup Pct', COMPANY: 'Company', LICENSE: 'License', EMAIL: 'Contractor Email', PAY: 'Pay Note', TECHINVOICE: 'Allow Tech Invoicing' };
  const field = map[key];
  const val = body.slice(km.index + km[1].length).replace(/^[\s:]*(to|is|=|:)?\s+/i, '').trim();
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

// HISTORY [address or customer] — every past job at a property, regardless of
// which owner/name was on each visit. Matches address OR name; leads with address.
async function handleHistory(command, accountPhone) {
  const term = command.replace(/^\s*HISTORY\s*/i, '').trim();
  if (!term) return 'Usage: HISTORY [address or customer]. Example: HISTORY 412 State St';
  const esc = term.replace(/"/g, '\\"').toLowerCase();
  const jobs = (await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${accountPhone}", OR(FIND("${esc}", LOWER({customer_address})), FIND("${esc}", LOWER({customer_name}))))`))
    .sort((a, b) => (b.fields.date || '').localeCompare(a.fields.date || ''));
  if (jobs.length === 0) return `No history found for "${term}". Try the street address.`;
  const addr = jobs.find(j => j.fields.customer_address)?.fields.customer_address;
  const names = [...new Set(jobs.map(j => j.fields.customer_name).filter(Boolean))];
  const name = addr ? `${addr}${names.length ? ' (' + names.join(', ') + ')' : ''}` : (names[0] || term);
  const totalHours = jobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  const lines = jobs.slice(0, 6).map(j => {
    const who = j.fields.tech_name ? ` · ${j.fields.tech_name}` : '';
    return `${j.fields.date || '?'}: ${j.fields.job_type || 'service'} (${j.fields.labor_hours || 0}h)${who}`;
  }).join('\n');
  const more = jobs.length > 6 ? `\n+${jobs.length - 6} older` : '';
  // Surface any customer note for this property.
  let noteLine = '';
  const custs = await airtableQuery(TABLES.CUSTOMERS,
    `AND({subscriber_phone} = "${accountPhone}", OR(FIND("${esc}", LOWER({customer_name})), FIND("${esc}", LOWER({address}))))`);
  const note = custs.find(c => (c.fields.notes || '').trim())?.fields.notes;
  if (note) noteLine = `\n📝 ${note.split('\n').slice(-3).join(' · ')}`;
  return `${name} — ${jobs.length} job(s), ${totalHours}h total:\n${lines}${more}${noteLine}`;
}

// NOTE [customer]: [text] — append a saved note to a customer.
async function handleNote(command, accountPhone) {
  const rest = command.replace(/^\s*NOTE\s*/i, '');
  const ci = rest.indexOf(':');
  if (ci < 0) return 'Usage: NOTE [customer]: [note]. Example: NOTE Smith: gate code 1234, dog in yard';
  const customer = rest.slice(0, ci).trim();
  const note = rest.slice(ci + 1).trim();
  if (!customer || !note) return 'Usage: NOTE [customer]: [note].';
  const esc = customer.replace(/"/g, '\\"').toLowerCase();
  const recs = await airtableQuery(TABLES.CUSTOMERS,
    `AND({subscriber_phone} = "${accountPhone}", OR(FIND("${esc}", LOWER({customer_name})), FIND("${esc}", LOWER({address}))))`);
  if (recs.length === 0) return `No customer found for "${customer}". Log a job for them first.`;
  const rec = recs[0];
  const prior = rec.fields.notes || '';
  const updated = prior ? `${prior}\n${localDate()}: ${note}` : `${localDate()}: ${note}`;
  const ok = await airtableUpdate(TABLES.CUSTOMERS, rec.id, { notes: updated });
  return ok ? `✓ Note saved to ${rec.fields.customer_name}.` : 'Could not save the note. Try again.';
}

// ----------------------------------------------------------------------------
// GET-PAID LOOP — track outstanding invoices, mark paid, nudge.
// ----------------------------------------------------------------------------
function daysSince(dateStr) {
  if (!dateStr) return null;
  const ms = Date.parse(localDate()) - Date.parse(dateStr);
  return ms >= 0 ? Math.floor(ms / 86400000) : 0;
}

async function handleUnpaid(accountPhone) {
  const invs = (await airtableQuery(TABLES.INVOICES,
    `AND({subscriber_phone} = "${accountPhone}", {status} = "Sent")`))
    .sort((a, b) => (a.fields.sent_date || '').localeCompare(b.fields.sent_date || ''));
  if (invs.length === 0) return "You're all caught up — no outstanding invoices.";
  const total = invs.reduce((s, i) => s + (i.fields.amount || 0), 0);
  const lines = invs.slice(0, 6).map(i => {
    const d = daysSince(i.fields.sent_date);
    const age = d == null ? 'sent' : `${d}d`;
    const flag = (d != null && d >= 14) ? ' ⚠ overdue' : '';
    return `- ${i.fields.customer_name}: ${money(i.fields.amount || 0)} (${age})${flag}`;
  }).join('\n');
  const more = invs.length > 6 ? `\n+${invs.length - 6} more` : '';
  return `Outstanding: ${invs.length} invoice(s), ${money(total)}\n${lines}${more}\n\nReply PAID [customer] when one clears · RESEND [customer] to nudge.`;
}

async function handlePaid(command, accountPhone) {
  const term = command.replace(/^\s*PAID\s*/i, '').trim();
  if (!term) return 'Usage: PAID [customer or invoice #]. Example: PAID Smith';
  const esc = term.replace(/"/g, '\\"');
  const invs = (await airtableQuery(TABLES.INVOICES,
    `AND({subscriber_phone} = "${accountPhone}", {status} = "Sent", OR(FIND("${esc.toLowerCase()}", LOWER({customer_name})), {invoice_label} = "${esc}"))`))
    .sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  if (invs.length === 0) return `No outstanding invoice found for "${term}".`;
  const inv = invs[0];
  await airtableUpdate(TABLES.INVOICES, inv.id, { status: 'Paid', paid_date: localDate() });
  const others = invs.length - 1;
  return `✓ Marked ${inv.fields.invoice_label} — ${inv.fields.customer_name}, ${money(inv.fields.amount || 0)} — PAID.${others > 0 ? ` (${others} more outstanding for that name — reply UNPAID to see them.)` : ''}`;
}

async function handleResend(command, accountPhone) {
  const term = command.replace(/^\s*RESEND\s*/i, '').trim();
  if (!term) return 'Usage: RESEND [customer]';
  const esc = term.replace(/"/g, '\\"');
  const invs = (await airtableQuery(TABLES.INVOICES,
    `AND({subscriber_phone} = "${accountPhone}", OR(FIND("${esc.toLowerCase()}", LOWER({customer_name})), {invoice_label} = "${esc}"))`))
    .sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  if (invs.length === 0) return `No invoice found for "${term}".`;
  const inv = invs[0];
  let snap = {}; try { snap = JSON.parse(inv.fields.notes || '{}'); } catch { snap = {}; }
  if (!snap.customerEmail) return `That invoice hasn't been emailed yet. Open it to send: ${BASE_URL}/invoice/${inv.id}`;
  // Replies go to the contractor, never FieldBrief.
  let replyTo = snap.replyTo || '';
  if (!replyTo) { const s = await getSubscriberSettings(accountPhone); replyTo = s.email || ''; }
  if (!replyTo) return `Set your email first so replies reach you: SET EMAIL you@yourco.com`;
  const viewUrl = `${BASE_URL}/invoice/${inv.id}/view`;
  const html = `<!doctype html><html><head><meta charset="utf-8"><style>${INV_CSS}</style></head><body>
<p style="max-width:640px;margin:0 auto 12px;font:14px sans-serif;color:#6b6256">Friendly reminder — this invoice is still open:</p>
${renderInvoiceBody(snap)}
<p style="max-width:640px;margin:16px auto;color:#6b6256;font:13px sans-serif;text-align:center">View online: <a href="${viewUrl}">${viewUrl}</a></p></body></html>`;
  const result = await sendInvoiceEmail({
    to: snap.customerEmail, replyTo, fromName: snap.company || 'FieldBrief',
    subject: `Reminder: Invoice ${snap.invNum} from ${snap.company || 'your service provider'}`, html,
  });
  if (!result.ok) return `Couldn't resend: ${result.error}`;
  return `✓ Reminder for ${inv.fields.invoice_label} resent to ${snap.customerEmail}.`;
}

// ----------------------------------------------------------------------------
// MORNING DISPATCH — owner assigns the day's jobs to techs, then sends each
// tech their list. SCHEDULE to enter/review, DISPATCH to text the crew.
// ----------------------------------------------------------------------------
async function techPhoneByName(accountPhone, name) {
  const techs = await airtableQuery(TABLES.TECHS, `AND({Account Phone} = "${accountPhone}", {Active} = 1)`);
  const lower = (name || '').toLowerCase().trim();
  const hit = techs.find(t => (t.fields.Name || '').toLowerCase().trim() === lower)
    || techs.find(t => (t.fields.Name || '').toLowerCase().includes(lower) && lower);
  return hit ? { phone: hit.fields.Phone || '', name: hit.fields.Name || name } : null;
}

async function handleSchedule(command, accountPhone, isOwner) {
  if (!isOwner) return 'Only the account owner can build the schedule.';
  const rest = command.replace(/^\s*SCHEDULE\s*/i, '').trim();
  const today = localDate();

  if (!rest) {
    // Review today's schedule grouped by tech.
    const rows = await airtableQuery(TABLES.SCHEDULE, `AND({Account Phone} = "${accountPhone}", DATESTR({Date}) = "${today}")`);
    if (rows.length === 0) return 'No schedule for today yet. Add jobs: SCHEDULE Mike 8a Harbor Inn boiler, 11a Smith no-heat';
    const byTech = {};
    rows.forEach(r => { const t = r.fields['Tech Name'] || '—'; (byTech[t] = byTech[t] || []).push(r.fields); });
    const blocks = Object.entries(byTech).map(([t, js]) =>
      `${t}:\n` + js.map(j => `  ${j.Time || ''} ${j.Customer || j.Job || ''}`.trim()).join('\n')).join('\n');
    return `Today's schedule:\n${blocks}\n\nReply DISPATCH to text it to the crew.`;
  }

  const parsed = await claudeText({
    max_tokens: 1200,
    content: rest,
    system: `Parse a dispatcher's note assigning jobs to technicians for today. Return ONLY a JSON array, one object per job:
[{"tech":"","time":"","customer":"","address":"","job":""}]
"tech" = the technician's first name the job is for. Multiple techs and multiple jobs per tech are common. Keep times like "8a","1p". If no tech is named, use "" for tech.`,
  }).catch(() => '[]');
  let jobs;
  try {
    const m = parsed.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
    const raw = m ? m[1] : (parsed.match(/(\[[\s\S]*\])/)?.[1] || parsed);
    jobs = JSON.parse(raw);
    if (!Array.isArray(jobs)) jobs = [jobs];
  } catch { return 'Could not read that. Try: SCHEDULE Mike 8a Harbor Inn boiler, 11a Smith no-heat'; }
  if (!jobs.length) return 'No jobs found. Try: SCHEDULE Mike 8a Harbor Inn boiler, 11a Smith no-heat';

  const counts = {}; const noNumber = new Set();
  for (const j of jobs) {
    const t = await techPhoneByName(accountPhone, j.tech || '');
    const techName = t?.name || (j.tech || 'Unassigned');
    if (j.tech && !t) noNumber.add(j.tech);
    await airtableCreate(TABLES.SCHEDULE, {
      Label: `${techName} - ${j.customer || j.job || 'job'} - ${today}`,
      Date: today, 'Tech Name': techName, 'Tech Phone': t?.phone || '',
      Time: j.time || '', Customer: j.customer || '', Address: j.address || '', Job: j.job || '',
      'Account Phone': accountPhone, Status: 'Scheduled',
    });
    counts[techName] = (counts[techName] || 0) + 1;
  }
  const summary = Object.entries(counts).map(([t, n]) => `${t} (${n})`).join(', ');
  let msg = `✓ Scheduled today: ${summary}. Reply DISPATCH to text the crew, or SCHEDULE to review.`;
  if (noNumber.size) msg += `\n⚠ No saved number for: ${[...noNumber].join(', ')} — ADD TECH [phone] [name] so they get texted.`;
  return msg;
}

async function handleDispatch(accountPhone, isOwner) {
  if (!isOwner) return 'Only the account owner can dispatch the crew.';
  const today = localDate();
  const rows = await airtableQuery(TABLES.SCHEDULE, `AND({Account Phone} = "${accountPhone}", DATESTR({Date}) = "${today}")`);
  if (rows.length === 0) return 'Nothing scheduled today. Add jobs first: SCHEDULE Mike 8a Harbor Inn boiler';
  const byTech = {};
  rows.forEach(r => { const key = r.fields['Tech Phone'] || ''; (byTech[key] = byTech[key] || { name: r.fields['Tech Name'], jobs: [] }).jobs.push(r.fields); });
  const sent = []; const skipped = [];
  for (const [phone, grp] of Object.entries(byTech)) {
    if (!phone) { skipped.push(grp.name || 'unassigned'); continue; }
    const list = grp.jobs.map(j => `${j.Time ? j.Time + ' ' : ''}${j.Customer || j.Job}${j.Address ? ' (' + j.Address + ')' : ''}${j.Customer && j.Job ? ' - ' + j.Job : ''}`).join('\n');
    await sendSMS(phone, `Good morning ${grp.name || ''}! Today's jobs:\n${list}\n— text back what you did after each.`);
    for (const j of grp.jobs) { const id = rows.find(r => r.fields === j)?.id; if (id) await airtableUpdate(TABLES.SCHEDULE, id, { Status: 'Sent' }); }
    sent.push(`${grp.name} (${grp.jobs.length})`);
  }
  let msg = sent.length ? `✓ Sent to: ${sent.join(', ')}.` : 'Nothing sent.';
  if (skipped.length) msg += ` No number for: ${[...new Set(skipped)].join(', ')} (ADD TECH to fix).`;
  return msg;
}

// ----------------------------------------------------------------------------
// ONBOARD — platform-admin command to set up a new contractor account in
// seconds and auto-welcome them. Restricted to admin phone(s).
// ----------------------------------------------------------------------------
const ADMIN_PHONES = [process.env.PLATFORM_ADMIN, '+18054527511'].filter(Boolean);

async function handleOnboard(command) {
  const rest = command.replace(/^\s*ONBOARD\s*/i, '').trim();
  const parts = rest.split(',').map(p => p.trim()).filter(Boolean);
  if (parts.length < 3) return 'Usage: ONBOARD name, company, cell, rate [, email]\nEx: ONBOARD Mike Smith, Smith Plumbing, 8055551234, 195';
  const [name, company, cellRaw, rateRaw, email] = parts;
  const cell = normalizePhone(cellRaw);
  if (!cell || cell.replace(/[^0-9]/g, '').length < 11) return `That cell looks off: "${cellRaw}". Use 10 digits.`;
  const rate = parseFloat((rateRaw || '').replace(/[^0-9.]/g, '')) || 0;
  const existing = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${cell}"`);
  if (existing.length) return `${cell} is already set up (${existing[0].fields['Company'] || existing[0].fields['Full Name'] || 'existing account'}).`;
  const fields = { 'Full Name': name, 'Company': company, 'Phone Number': cell, 'Hourly Rate': rate, 'Status': 'Active' };
  if (email) fields['Contractor Email'] = email;
  const id = await airtableCreate(TABLES.SUBSCRIBERS, fields);
  if (!id) return 'Could not create the account. Try again.';
  await sendSMS(cell, `Welcome to FieldBrief, ${name.split(' ')[0]}! Text this number what you did after each job — e.g. "Smith 12 Main St, boiler tune-up 2hr, $45 filter" — and it logs + invoices for you. You can also ask "who owes me" or say "send Smith's invoice". Reply HELP anytime. Quick guide: ${BASE_URL}/how`);
  return `✓ Onboarded ${company} — ${name} (${cell}), $${rate}/hr. Welcome text sent to them.`;
}

// ============================================================================
// COMMAND HANDLERS
// Returns reply string. Does NOT call sendSMS.
// ============================================================================
async function handleCommand(command, subscriberPhone, subscriberName, isOwner = false) {
  const cmd = command.toUpperCase().trim();
  const word = cmd.split(/\s+/)[0];
  if (['HELP', 'COMMANDS', 'INFO'].includes(word)) {
    return 'Just text a job to log it. Commands: JOBS · PARTS · INVOICE [customer] · PROPOSAL [customer]: [scope] · HISTORY [address] · SCHEDULE [tech jobs] · DISPATCH · UNPAID · PAID [customer] · RESEND · STATUS · SETTINGS · TECHS · ADD TECH · UNDO · FIX · HELP';
  }
  if (word === 'SETTINGS' || word === 'SET') {
    return await handleSettings(command, subscriberPhone);
  }
  if (word === 'UNDO') {
    return await handleUndo(subscriberPhone);
  }
  if (word === 'FIX') {
    return await handleFix(command, subscriberPhone);
  }
  if (word === 'HISTORY') {
    return await handleHistory(command, subscriberPhone);
  }
  if (word === 'TECHS') {
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
  if (word === 'UNPAID' || word === 'OUTSTANDING') {
    return await handleUnpaid(subscriberPhone);
  }
  if (word === 'PAID') {
    return await handlePaid(command, subscriberPhone);
  }
  if (word === 'RESEND') {
    if (!isOwner) return 'Only the account owner can resend invoices.';
    return await handleResend(command, subscriberPhone);
  }
  if (word === 'SCHEDULE') {
    return await handleSchedule(command, subscriberPhone, isOwner);
  }
  if (word === 'DISPATCH') {
    return await handleDispatch(subscriberPhone, isOwner);
  }
  if (word === 'NOTE') {
    return await handleNote(command, subscriberPhone);
  }
  if (word === 'ONBOARD') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Onboarding new businesses is admin-only.';
    return await handleOnboard(command);
  }
  if (word === 'JOBS') {
    const today = localDate();
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${today}")`);
    if (jobs.length === 0) return 'No jobs logged today yet.';
    const jobList = jobs.slice(0, 3).map(j =>
      `- ${j.fields.customer_name}: ${j.fields.job_type} (${j.fields.labor_hours || 0}h)`).join('\n');
    return `Today's jobs:\n${jobList}${jobs.length > 3 ? `\n+${jobs.length - 3} more` : ''}`;
  }
  if (word === 'PARTS') {
    const today = localDate();
    const parts = await airtableQuery(TABLES.PARTS_USED,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${today}")`);
    if (parts.length === 0) return 'No parts logged today.';
    const partList = parts.slice(0, 5).map(p =>
      `- ${p.fields.part_name} x${p.fields.quantity || 1} ($${(p.fields.cost || 0).toFixed(2)})`).join('\n');
    const total = parts.reduce((sum, p) => sum + (p.fields.cost || 0), 0);
    return `Today's parts:\n${partList}\nTotal: $${total.toFixed(2)}`;
  }
  if (word === 'INVOICE') {
    return await handleInvoiceCommand(command, subscriberPhone, subscriberName, isOwner);
  }
  if (word === 'PROPOSAL' || word === 'QUOTE' || word === 'ESTIMATE') {
    return await handleProposeCommand(command, subscriberPhone, subscriberName, isOwner);
  }
  if (word === 'BRIEF') {
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", DATESTR({date}) = "${localDate(-1)}")`);
    return await generateMorningBrief(jobs);
  }
  if (word === 'STATUS') {
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
  // Match on address OR customer name — the property is the constant; owner names change.
  const jobs = await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${subscriberPhone}", OR(FIND("${esc}", LOWER({customer_address})), FIND("${esc}", LOWER({customer_name}))))`);
  if (jobs.length === 0) return `No jobs found for "${customer}". Try the street address or the customer name.`;
  jobs.sort((a, b) => (b.fields.date || '').localeCompare(a.fields.date || ''));

  if (!rate) rate = jobs.find(j => j.fields.labor_rate)?.fields.labor_rate || 0;
  const laborHours = jobs.reduce((s, j) => s + (j.fields.labor_hours || 0), 0);
  if (laborHours > 0 && !rate) {
    return `Found ${jobs.length} job(s), ${laborHours}h labor for ${customer}. Add your hourly rate to price labor: INVOICE ${customer} 215`;
  }

  // Gather parts for exactly the matched jobs (by their wo_labels) so address-based
  // lookups still capture parts even when owner names differ across visits.
  const woLabels = [...new Set(jobs.map(j => j.fields.wo_label).filter(Boolean))];
  const parts = woLabels.length
    ? await airtableQuery(TABLES.PARTS_USED,
        `AND({subscriber_phone} = "${subscriberPhone}", OR(${woLabels.map(l => `{wo_label} = "${l.replace(/"/g, '\\"')}"`).join(', ')}))`)
    : [];

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
    customer: customerName, address, date: localDate(), woLabels,
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

// Invoices send from a shared FieldBrief address with the contractor's company
// as the display name (e.g. "Wick Boiler" <hello@fieldbrief.ai>). Replies are
// always directed to the contractor via reply-to — never to FieldBrief.
async function sendInvoiceEmail({ to, replyTo, fromName, subject, html }) {
  const key = process.env.RESEND_API_KEY;
  if (!key) return { ok: false, error: 'RESEND_API_KEY not set — add it in Render env after verifying fieldbrief.ai in Resend.' };
  const from = `${fromName || 'FieldBrief'} <${process.env.INVOICE_FROM || 'hello@fieldbrief.ai'}>`;
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
// PROPOSALS / QUOTES — win the job up front. Unlike invoices (built from logged
// work), a proposal is dictated by the contractor for work NOT yet done:
//   PROPOSAL <customer>: <scope + pricing>
// AI parses the scope into line items, saves a Draft, returns a review/send
// link. The customer gets an emailed quote with an ACCEPT button; on accept the
// contractor is texted. Stored in its own PROPOSALS table so it never pollutes
// the invoice / "who owes me" lists.
// ============================================================================
async function parseProposal(scope) {
  try {
    const text = await claudeText({
      max_tokens: 500,
      content: scope,
      system: `Parse a contractor's quote/estimate for work NOT yet done into clear line items with dollar amounts. Return ONLY JSON: {"items":[{"desc":"...","amount":number}],"total":number}. Split labor and materials into separate, customer-readable line items (e.g. "Labor — install 50-gal water heater", "Materials — water heater + fittings"). Use the contractor's stated prices; if an item has no stated price, include it with amount 0. "total" must equal the sum of item amounts. Return ONLY the JSON.`,
    });
    const m = text.match(/\{[\s\S]*\}/);
    const r = JSON.parse(m ? m[0] : text);
    const items = (Array.isArray(r.items) ? r.items : [])
      .map(i => ({ desc: String(i.desc || 'Work').slice(0, 200), amount: Number(i.amount) || 0 }))
      .filter(i => i.desc);
    const total = items.reduce((s, i) => s + i.amount, 0);
    return { items, total };
  } catch (error) {
    console.error('parseProposal error:', error);
    return { items: [], total: 0 };
  }
}

async function handleProposeCommand(command, subscriberPhone, subscriberName, isOwner = true) {
  if (!isOwner) return 'Only the account owner can send proposals.';
  let rest = command.replace(/^\s*(PROPOSAL|QUOTE|ESTIMATE)\s*/i, '').trim();
  // "<customer> [@ address]: <scope>" — everything before the first colon is who/where.
  const colon = rest.indexOf(':');
  if (colon === -1) {
    return 'Usage: PROPOSAL [customer]: [what you\'ll do + prices]. Example: PROPOSAL Smith: replace 50gal water heater, 6hr labor $900, heater + fittings $1100';
  }
  let who = rest.slice(0, colon).trim();
  const scope = rest.slice(colon + 1).trim();
  if (!who) return 'Add who the proposal is for: PROPOSAL [customer]: [scope]';
  if (!scope) return 'Add what the work is: PROPOSAL [customer]: [scope + prices]';
  let address = '';
  const at = who.match(/\s+@\s+(.+)$/) || who.match(/\s+at\s+(.+)$/i);
  if (at) { address = at[1].trim(); who = who.slice(0, at.index).trim(); }

  const { items, total } = await parseProposal(scope);
  if (!items.length) return `Couldn't read line items from that. Try: PROPOSAL ${who}: replace water heater, 6hr labor $900, unit + parts $1100`;

  const acct = await getSubscriberSettings(subscriberPhone);
  const company = acct.company || subscriberName || 'My Company';
  const propNum = `PROP-${localDate().replace(/-/g, '')}-${Math.floor(1000 + Math.random() * 9000)}`;
  const snapshot = {
    propNum, company, contractor: subscriberName, subscriberPhone,
    customer: who, address, date: localDate(), items, total, status: 'Draft',
  };
  const recId = await airtableCreate(TABLES.PROPOSALS, {
    proposal_label: propNum, customer_name: who, amount: total,
    status: 'Draft', subscriber_phone: subscriberPhone, notes: JSON.stringify(snapshot),
  });
  if (!recId) return 'Could not create the proposal. Please try again.';
  const lines = items.map(i => `· ${i.desc} — ${money(i.amount)}`).join('\n');
  return `Proposal ${propNum} for ${who}: ${money(total)}\n${lines}\n\nReview & send -> ${BASE_URL}/proposal/${recId}`;
}

// Renders the proposal card (shared by the customer view and the email body).
function renderProposalBody(s) {
  const rows = (s.items || []).map(i =>
    `<tr><td>${escapeHTML(i.desc)}</td><td class="r">${money(i.amount)}</td></tr>`).join('');
  const validity = s.validUntil ? `<div class="mut">Valid until ${escapeHTML(s.validUntil)}</div>` : '';
  const msg = s.message ? `<div class="pay"><div class="lbl">Note</div><div class="note">${escapeHTML(s.message)}</div></div>` : '';
  return `<div class="inv">
    <div class="top"><div><div class="co">${escapeHTML(s.company)}</div><div class="mut">${escapeHTML(s.contractor || '')}</div></div>
      <div class="r"><div class="co">PROPOSAL</div><div class="mut">${escapeHTML(s.propNum)}</div><div class="mut">${escapeHTML(s.date)}</div>${validity}</div></div>
    <div class="billto"><span class="lbl">Prepared for</span> ${escapeHTML(s.customer)}${s.address ? ' · ' + escapeHTML(s.address) : ''}</div>
    <table><thead><tr><th>Scope of work</th><th class="r">Amount</th></tr></thead>
      <tbody>${rows || ''}</tbody></table>
    <div class="tot"><div class="grand">Total ${money(s.total)}</div></div>
    ${msg}
  </div>`;
}

async function getProposal(recId) {
  const rec = await airtableRequest('GET', TABLES.PROPOSALS, null, recId);
  if (!rec || !rec.fields) return null;
  let snap = {};
  try { snap = JSON.parse(rec.fields.notes || '{}'); } catch { snap = {}; }
  return { rec, snap };
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
  const token = process.env.TEST_PANEL_TOKEN;
  if (!token || req.query.key !== token) {
    return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Not found.</p>');
  }
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
 try{const r=await fetch('/sms',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded','x-fieldbrief-test':'${token}'},body:new URLSearchParams({From:from.value.trim(),To:'+18053104809',Body:body})});
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
  const acct = await getSubscriberSettings(snap.subscriberPhone);
  const replyToPrefill = snap.replyTo || acct.email || '';
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
  <label>Your email (so their reply reaches you, not FieldBrief) *</label><input type="email" name="reply_to" required placeholder="you@yourcompany.com" value="${escapeHTML(replyToPrefill)}">
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
  const methods = [].concat(req.body.methods || []);
  const payNote = (req.body.pay_note || '').trim();
  if (!customerEmail) return res.status(400).send('Customer email required.');

  // Replies must reach the contractor who sent it — never FieldBrief. Use the
  // entered email, else their saved account email; never the from-address.
  // Persist it so it's captured once and every future invoice has a reply-to.
  const s = await getSubscriberSettings(snap.subscriberPhone);
  const replyTo = (req.body.reply_to || '').trim() || (s.email || '').trim();
  if (!replyTo) {
    return res.status(400).type('html').send('<div style="max-width:520px;margin:40px auto;font:15px/1.6 sans-serif;padding:22px;border:1px solid #e4b4b4;background:#fbeaea;border-radius:12px;color:#8a2b2b"><b>Add your email first.</b><br>So your customer\'s reply goes to <i>you</i> (not FieldBrief), enter your email above (or text <b>SET EMAIL you@yourco.com</b>), then send again.</div>');
  }
  if (s.recId && (s.email || '').trim().toLowerCase() !== replyTo.toLowerCase()) {
    await airtableUpdate(TABLES.SUBSCRIBERS, s.recId, { 'Contractor Email': replyTo });
  }

  // Double-check: re-pull the same matched jobs/parts and confirm the invoice still
  // matches. Blocks sending if a job/part was edited or removed after the draft.
  const labels = (snap.woLabels && snap.woLabels.length) ? snap.woLabels : [snap.customer];
  const labelOr = labels.map(l => `{wo_label} = "${String(l).replace(/"/g, '\\"')}"`).join(', ');
  const curJobs = await airtableQuery(TABLES.WORK_ORDERS,
    `AND({subscriber_phone} = "${snap.subscriberPhone}", OR(${labelOr}))`);
  const curParts = await airtableQuery(TABLES.PARTS_USED,
    `AND({subscriber_phone} = "${snap.subscriberPhone}", OR(${labelOr}))`);
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

// ----------------------------------------------------------------------------
// PROPOSAL pages — contractor review/send, customer view + ACCEPT, accept handler.
// ----------------------------------------------------------------------------
app.get('/proposal/:id', async (req, res) => {
  const p = await getProposal(req.params.id);
  if (!p) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Proposal not found.</p>');
  const { rec, snap } = p;
  const id = req.params.id;
  const status = rec.fields.status || 'Draft';
  const sent = status !== 'Draft';
  const acct = await getSubscriberSettings(snap.subscriberPhone);
  const replyToPrefill = snap.replyTo || acct.email || '';
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(snap.propNum || 'Proposal')} — review & send</title><style>${INV_CSS}
.bar{max-width:640px;margin:0 auto 14px;display:flex;justify-content:space-between;align-items:center}
.bar h1{font-size:1.1rem;margin:0}.badge{font-size:.72rem;padding:3px 9px;border-radius:20px;background:#efe9dd;color:#6b6256}
.send{max-width:640px;margin:16px auto 0;background:#fff;border:1px solid #e4ddcf;border-radius:14px;padding:20px}
.send h2{font-size:1rem;margin:0 0 12px}label{display:block;font-size:.8rem;color:#6b6256;margin:12px 0 4px}
input[type=email],input[type=text],textarea{width:100%;padding:10px;border:1px solid #d8cfbd;border-radius:9px;font:inherit;box-sizing:border-box}
button{margin-top:16px;width:100%;padding:13px;border:0;border-radius:10px;background:#c0532b;color:#fff;font-weight:700;font-size:1rem;cursor:pointer}
.ok{max-width:640px;margin:0 auto 14px;background:#e7f3e7;border:1px solid #bcd9bc;color:#2c6b2c;border-radius:10px;padding:12px 14px;font-size:.9rem}
.printbtn{background:#1a1a1a}</style></head><body>
<div class="bar"><h1>Review & send proposal</h1><span class="badge">${escapeHTML(status)}</span></div>
${sent ? `<div class="ok">✓ ${status === 'Accepted' ? 'Accepted by the customer' : 'Sent to ' + escapeHTML(snap.customerEmail || 'customer')}${rec.fields.sent_date ? ' on ' + escapeHTML(rec.fields.sent_date) : ''}. You can resend with new details below.</div>` : ''}
${renderProposalBody(snap)}
<form class="send" method="POST" action="/proposal/${id}/send" onsubmit="return confirm('Send this proposal to your customer now?')">
  <h2>Email this proposal to your customer</h2>
  <label>Customer email *</label><input type="email" name="customer_email" required placeholder="customer@email.com" value="${escapeHTML(snap.customerEmail || '')}">
  <label>Your email (so their reply reaches you, not FieldBrief) *</label><input type="email" name="reply_to" required placeholder="you@yourcompany.com" value="${escapeHTML(replyToPrefill)}">
  <label>Message to the customer (optional)</label><textarea name="message" rows="2" placeholder="e.g. Happy to answer questions — this quote is good for 30 days.">${escapeHTML(snap.message || '')}</textarea>
  <label style="display:flex;align-items:flex-start;gap:8px;margin-top:16px;color:#1a1a1a;font-size:.9rem"><input type="checkbox" name="confirm_ok" value="1" required style="margin-top:3px;flex:none">I've checked this scope and pricing — it's correct.</label>
  <button type="submit">Send proposal to customer</button>
</form>
<form class="send" method="GET" action="/proposal/${id}/view" target="_blank" style="background:none;border:0;padding:8px 0">
  <button class="printbtn" type="submit">Preview what the customer sees</button>
</form>
</body></html>`);
});

app.post('/proposal/:id/send', async (req, res) => {
  const id = req.params.id;
  const p = await getProposal(id);
  if (!p) return res.status(404).send('Proposal not found.');
  const { snap } = p;
  const customerEmail = (req.body.customer_email || '').trim();
  const message = (req.body.message || '').trim();
  if (!customerEmail) return res.status(400).send('Customer email required.');

  const s = await getSubscriberSettings(snap.subscriberPhone);
  const replyTo = (req.body.reply_to || '').trim() || (s.email || '').trim();
  if (!replyTo) {
    return res.status(400).type('html').send('<div style="max-width:520px;margin:40px auto;font:15px/1.6 sans-serif;padding:22px;border:1px solid #e4b4b4;background:#fbeaea;border-radius:12px;color:#8a2b2b"><b>Add your email first.</b><br>So your customer\'s reply goes to <i>you</i> (not FieldBrief), enter your email above (or text <b>SET EMAIL you@yourco.com</b>), then send again.</div>');
  }
  if (s.recId && (s.email || '').trim().toLowerCase() !== replyTo.toLowerCase()) {
    await airtableUpdate(TABLES.SUBSCRIBERS, s.recId, { 'Contractor Email': replyTo });
  }

  snap.customerEmail = customerEmail;
  snap.replyTo = replyTo;
  snap.message = message;
  snap.status = 'Sent';

  const acceptUrl = `${BASE_URL}/proposal/${id}/view`;
  const emailHtml = `<!doctype html><html><head><meta charset="utf-8"><style>${INV_CSS}
.cta{display:block;max-width:640px;margin:18px auto;text-align:center}
.cta a{display:inline-block;background:#c0532b;color:#fff;text-decoration:none;font-weight:700;padding:13px 26px;border-radius:10px}</style></head><body>
${renderProposalBody(snap)}
<div class="cta"><a href="${acceptUrl}">Review & accept this proposal →</a></div>
<p style="max-width:640px;margin:10px auto;color:#6b6256;font:13px sans-serif;text-align:center">
Or view it online: <a href="${acceptUrl}">${acceptUrl}</a></p></body></html>`;

  const result = await sendInvoiceEmail({
    to: customerEmail, replyTo: replyTo || undefined, fromName: snap.company || 'FieldBrief',
    subject: `Proposal ${snap.propNum} from ${snap.company || 'your service provider'}`, html: emailHtml,
  });

  snap.lastSend = result.ok ? { ok: true, id: result.id } : { ok: false, error: result.error };
  await airtableUpdate(TABLES.PROPOSALS, id, {
    status: result.ok ? 'Sent' : 'Draft',
    sent_date: result.ok ? localDate() : undefined,
    notes: JSON.stringify(snap),
  });

  if (!result.ok) {
    return res.status(200).type('html').send(`<div style="max-width:560px;margin:40px auto;font:15px/1.5 sans-serif;padding:24px;border:1px solid #e4b4b4;background:#fbeaea;border-radius:12px;color:#8a2b2b">
      <h2 style="margin:0 0 8px">Couldn't send yet</h2>
      <p>${escapeHTML(result.error)}</p>
      <p style="color:#6b6256">The proposal is saved. Reopen the link and hit send again.</p>
      <p><a href="/proposal/${id}">← back to proposal</a></p></div>`);
  }
  res.type('html').send(`<div style="max-width:560px;margin:40px auto;font:15px/1.5 sans-serif;padding:24px;border:1px solid #bcd9bc;background:#e7f3e7;border-radius:12px;color:#2c6b2c">
    <h2 style="margin:0 0 8px">✓ Proposal sent</h2>
    <p>${escapeHTML(snap.propNum)} emailed to <b>${escapeHTML(customerEmail)}</b> for ${money(snap.total)}. You'll get a text the moment they accept.</p>
    <p><a href="/proposal/${id}/view" target="_blank">View what the customer received →</a></p></div>`);
});

// Customer-facing view — what the email links to, with the ACCEPT button.
app.get('/proposal/:id/view', async (req, res) => {
  const p = await getProposal(req.params.id);
  if (!p) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Proposal not found.</p>');
  const { rec, snap } = p;
  const id = req.params.id;
  const status = rec.fields.status || 'Draft';
  const accepted = status === 'Accepted';
  const declined = status === 'Declined';
  const action = accepted
    ? `<div class="ok">✓ You accepted this proposal${rec.fields.accepted_date ? ' on ' + escapeHTML(rec.fields.accepted_date) : ''}. ${escapeHTML(snap.company || 'Your contractor')} has been notified and will be in touch.</div>`
    : declined
    ? `<div class="dec">You let ${escapeHTML(snap.company || 'the contractor')} know this isn't moving forward. Changed your mind? Just reply to the email.</div>`
    : `<form method="POST" action="/proposal/${id}/accept" class="acc" onsubmit="return confirm('Accept this proposal?')">
        <button type="submit" name="decision" value="accept" class="accept">Accept this proposal</button>
        <button type="submit" name="decision" value="decline" class="decline">Not right now</button>
        <p class="mut" style="text-align:center;margin-top:10px">Questions? Just reply to the email and it goes straight to ${escapeHTML(snap.company || 'us')}.</p>
       </form>`;
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(snap.propNum || 'Proposal')}</title><style>${INV_CSS}
.ok{max-width:640px;margin:14px auto 0;background:#e7f3e7;border:1px solid #bcd9bc;color:#2c6b2c;border-radius:10px;padding:14px 16px;font-size:.95rem;text-align:center}
.dec{max-width:640px;margin:14px auto 0;background:#f7f2e8;border:1px solid #e4ddcf;color:#6b6256;border-radius:10px;padding:14px 16px;font-size:.95rem;text-align:center}
.acc{max-width:640px;margin:16px auto 0;display:flex;flex-direction:column;gap:10px}
.acc button{border:0;border-radius:10px;padding:14px;font-weight:700;font-size:1rem;cursor:pointer}
.accept{background:#2c8a3d;color:#fff}.decline{background:#fff;color:#6b6256;border:1px solid #d8cfbd}</style></head><body>
${renderProposalBody(snap)}
${action}
</body></html>`);
});

app.post('/proposal/:id/accept', async (req, res) => {
  const id = req.params.id;
  const p = await getProposal(id);
  if (!p) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Proposal not found.</p>');
  const { rec, snap } = p;
  const status = rec.fields.status || 'Draft';
  if (status === 'Draft') {
    return res.status(400).type('html').send('<p style="font:16px sans-serif;padding:30px">This proposal hasn\'t been sent yet.</p>');
  }
  const decline = (req.body.decision || '') === 'decline';
  if (status === 'Accepted' && !decline) {
    return res.type('html').send(`<div style="max-width:520px;margin:40px auto;font:15px/1.6 sans-serif;padding:24px;border:1px solid #bcd9bc;background:#e7f3e7;border-radius:12px;color:#2c6b2c"><h2 style="margin:0 0 6px">Already accepted ✓</h2><p>${escapeHTML(snap.company || 'Your contractor')} has been notified.</p></div>`);
  }
  const newStatus = decline ? 'Declined' : 'Accepted';
  snap.status = newStatus;
  await airtableUpdate(TABLES.PROPOSALS, id, {
    status: newStatus,
    accepted_date: decline ? undefined : localDate(),
    notes: JSON.stringify(snap),
  });
  // Notify the contractor by text.
  if (snap.subscriberPhone) {
    const note = decline
      ? `✗ ${snap.customer} declined proposal ${snap.propNum} (${money(snap.total)}).`
      : `✓ ${snap.customer} ACCEPTED your proposal ${snap.propNum} — ${money(snap.total)}! Go do the work, then text the job to log it and INVOICE ${snap.customer} when you're done.`;
    try { await sendSMS(snap.subscriberPhone, note); } catch (e) { console.error('accept notify failed:', e.message); }
  }
  if (decline) {
    return res.type('html').send(`<div style="max-width:520px;margin:40px auto;font:15px/1.6 sans-serif;padding:24px;border:1px solid #e4ddcf;background:#f7f2e8;border-radius:12px;color:#6b6256"><h2 style="margin:0 0 6px">Thanks for letting us know</h2><p>We've told ${escapeHTML(snap.company || 'the contractor')}. Changed your mind? Just reply to the email.</p></div>`);
  }
  res.type('html').send(`<div style="max-width:520px;margin:40px auto;font:15px/1.6 sans-serif;padding:24px;border:1px solid #bcd9bc;background:#e7f3e7;border-radius:12px;color:#2c6b2c"><h2 style="margin:0 0 6px">✓ Accepted — thank you!</h2><p>${escapeHTML(snap.company || 'Your contractor')} has been notified and will reach out to schedule the work.</p></div>`);
});

// Read-only subscriber dashboard (link surfaced via STATUS)
app.get('/dashboard/:id', async (req, res) => {
  const sub = await airtableRequest('GET', TABLES.SUBSCRIBERS, null, req.params.id);
  if (!sub || !sub.fields) return res.status(404).type('html').send('<p style="font:16px sans-serif;padding:30px">Not found.</p>');
  // Lets this console's quick-command buttons keep working when /sms Twilio
  // signature verification is enabled (same exempt header as the /test panel).
  const token = process.env.TEST_PANEL_TOKEN || '';
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
    `<tr><td>${escapeHTML(j.fields.date || '')}</td><td>${escapeHTML(j.fields.customer_name || '')}</td><td>${escapeHTML(j.fields.customer_address || '')}</td><td>${escapeHTML(j.fields.job_type || '')}</td><td class="r">${j.fields.labor_hours || 0}h</td></tr>`).join('') || '<tr><td colspan="5" class="mut">No jobs yet.</td></tr>';
  const outstanding = invoices.filter(i => i.fields.status === 'Sent');
  const outTotal = outstanding.reduce((s, i) => s + (i.fields.amount || 0), 0);
  const paid = invoices.filter(i => i.fields.status === 'Paid');
  const collectedTotal = paid.reduce((s, i) => s + (i.fields.amount || 0), 0);
  const invRow = (i, withAge) => {
    const open = i.fields.status === 'Sent';
    const d = open ? daysSince(i.fields.sent_date) : null;
    const overdue = d != null && d >= 14;
    const extra = (withAge && open && d != null) ? ` · ${d}d${overdue ? ' ⚠' : ''}` : (i.fields.paid_date ? ` · ${i.fields.paid_date}` : '');
    return `<tr${overdue ? ' style="background:#fbeaea"' : ''}><td>${escapeHTML(i.fields.invoice_label || '')}</td><td>${escapeHTML(i.fields.customer_name || '')}</td><td class="r">${money(i.fields.amount || 0)}</td><td>${escapeHTML(i.fields.status || '')}${extra}</td><td><a href="/invoice/${i.id}">open</a></td></tr>`;
  };
  const outRows = outstanding.map(i => invRow(i, true)).join('') || '<tr><td colspan="5" class="mut">Nothing outstanding — nice.</td></tr>';
  const paidRows = paid.slice(0, 15).map(i => invRow(i, false)).join('') || '<tr><td colspan="5" class="mut">No paid invoices yet.</td></tr>';
  const proposals = (await airtableQuery(TABLES.PROPOSALS, `{subscriber_phone} = "${phone}"`))
    .sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  const propBadge = st => st === 'Accepted' ? ' style="background:#e7f3e7"' : st === 'Declined' ? ' style="color:#999"' : '';
  const propRows = proposals.slice(0, 12).map(p =>
    `<tr${propBadge(p.fields.status)}><td>${escapeHTML(p.fields.proposal_label || '')}</td><td>${escapeHTML(p.fields.customer_name || '')}</td><td class="r">${money(p.fields.amount || 0)}</td><td>${escapeHTML(p.fields.status || 'Draft')}</td><td><a href="/proposal/${p.id}">open</a></td></tr>`).join('') || '<tr><td colspan="5" class="mut">No proposals yet.</td></tr>';
  const features = (await airtableQuery(TABLES.FEATURES, `OR({Status} = "New", {Status} = "Reviewing")`))
    .sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));
  const featRows = features.slice(0, 10).map(f =>
    `<tr><td>${escapeHTML(f.fields.Date || '')}</td><td>${escapeHTML(f.fields.Request || f.fields.Details || '')}</td><td>${escapeHTML(f.fields.Status || 'New')}</td></tr>`).join('') || '<tr><td colspan="3" class="mut">No open requests.</td></tr>';
  const sched = await airtableQuery(TABLES.SCHEDULE, `AND({Account Phone} = "${phone}", DATESTR({Date}) = "${localDate()}")`);
  const schedByTech = {};
  sched.forEach(r => { const t = r.fields['Tech Name'] || '—'; (schedByTech[t] = schedByTech[t] || []).push(r.fields); });
  const schedRows = Object.entries(schedByTech).map(([t, js]) =>
    `<tr><td>${escapeHTML(t)}</td><td>${js.map(j => escapeHTML(`${j.Time || ''} ${j.Customer || j.Job || ''}`.trim())).join('<br>')}</td><td>${js.some(j => j.Status === 'Sent') ? 'Sent ✓' : 'Scheduled'}</td></tr>`).join('') || '<tr><td colspan="3" class="mut">Nothing scheduled today.</td></tr>';
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHTML(company)} — FieldBrief</title><style>${INV_CSS}
.dash{max-width:720px;margin:0 auto;padding:20px}
.cards{display:flex;gap:10px;margin:14px 0}.card{flex:1;background:#fff;border:1px solid #e4ddcf;border-radius:12px;padding:14px;text-align:center}
.card .n{font-size:1.5rem;font-weight:800}.card .l{color:#6b6256;font-size:.78rem}
h2{font-size:1rem;margin:22px 0 6px}.sec{background:#fff;border:1px solid #e4ddcf;border-radius:12px;padding:4px 14px}
a{color:#c0532b;text-decoration:none}
.actions{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:14px 0}
.act{background:#fff;border:1px solid #e4ddcf;border-radius:12px;padding:12px}
.al{font-size:.82rem;color:#1a1a1a;font-weight:500;margin-bottom:6px}
.act textarea,.act input{width:100%;box-sizing:border-box;border:1px solid #d8cfbd;border-radius:8px;padding:9px;font:inherit;background:#fff}
.act button{margin-top:8px;width:100%;background:#c0532b;color:#fff;border:0;border-radius:8px;padding:10px;font-weight:600;cursor:pointer}
.result{grid-column:1/-1;background:#f7f2e8;border:1px solid #e4ddcf;border-radius:8px;padding:10px 12px;font-size:.9rem;color:#1a1a1a;white-space:pre-wrap}
.result:empty{display:none}
@media(max-width:560px){.actions,.cards{grid-template-columns:1fr;display:grid}}</style></head><body><div class="dash">
<div style="border-bottom:2px solid #c0532b;padding-bottom:12px;display:flex;justify-content:space-between;align-items:flex-start">
  <div class="co" style="font-size:1.3rem">${escapeHTML(company)}</div>
  <div class="mut r">$${rate}/hr · ${markup}% markup</div></div>
<div class="cards">
  <div class="card"><div class="n">${weekJobs.length}</div><div class="l">jobs this week</div></div>
  <div class="card"><div class="n">${weekHours}h</div><div class="l">hours this week</div></div>
  <div class="card"><div class="n">${money(outTotal)}</div><div class="l">outstanding (${outstanding.length})</div></div>
  <div class="card"><div class="n">${money(collectedTotal)}</div><div class="l">collected (${paid.length})</div></div>
</div>
<div class="actions">
  <div class="act">
    <div class="al">Log a job</div>
    <textarea id="jobtext" rows="2" placeholder="e.g. Smith 12 Main St, boiler tune-up 2hr, $45 filter"></textarea>
    <button onclick="logJob()">Log it</button>
  </div>
  <div class="act">
    <div class="al">Send an invoice</div>
    <input id="invcust" type="text" placeholder="customer name or address">
    <button onclick="makeInvoice()">Build invoice →</button>
  </div>
  <div class="act">
    <div class="al">Mark an invoice paid</div>
    <input id="paidref" type="text" placeholder="invoice # or customer">
    <button onclick="markPaid()">Mark paid</button>
  </div>
  <div id="result" class="result"></div>
</div>
<div class="act" style="margin:0 0 6px">
  <div class="al">Send a proposal / quote</div>
  <textarea id="proptext" rows="2" placeholder="Smith: replace 50gal water heater, 6hr labor $900, heater + fittings $1100"></textarea>
  <button onclick="makeProposal()">Build proposal →</button>
</div>
<div class="act" style="margin:0 0 6px">
  <div class="al">Dispatch the crew (today)</div>
  <textarea id="plantext" rows="2" placeholder="Mike 8a Harbor Inn boiler, 11a Smith no-heat. Dana 9a Mesa backflow"></textarea>
  <button onclick="scheduleDay()" style="background:#1a1a1a">Add to schedule</button>
  <button onclick="dispatchCrew()">Send today's schedule to crew →</button>
</div>
<h2>Today's schedule</h2><div class="sec"><table><thead><tr><th>Tech</th><th>Jobs</th><th>Status</th></tr></thead><tbody>${schedRows}</tbody></table></div>
<h2>Recent jobs</h2><div class="sec"><table><thead><tr><th>Date</th><th>Customer</th><th>Address</th><th>Type</th><th class="r">Hrs</th></tr></thead><tbody>${jobRows}</tbody></table></div>
<h2>Outstanding invoices</h2><div class="sec"><table><thead><tr><th>#</th><th>Customer</th><th class="r">Amount</th><th>Status</th><th></th></tr></thead><tbody>${outRows}</tbody></table></div>
<h2>Paid invoices</h2><div class="sec"><table><thead><tr><th>#</th><th>Customer</th><th class="r">Amount</th><th>Paid</th><th></th></tr></thead><tbody>${paidRows}</tbody></table></div>
<h2>Proposals</h2><div class="sec"><table><thead><tr><th>#</th><th>Customer</th><th class="r">Amount</th><th>Status</th><th></th></tr></thead><tbody>${propRows}</tbody></table></div>
<h2>Feature requests <span class="mut" style="font-weight:400;font-size:.8rem">— from fieldbrief.ai/features</span></h2><div class="sec"><table><thead><tr><th>Date</th><th>Request</th><th>Status</th></tr></thead><tbody>${featRows}</tbody></table></div>
<p class="mut" style="margin-top:20px;font-size:.8rem">Your private console · also works by text from the field</p>
</div>
<script>
const ACCOUNT=${JSON.stringify(phone)};
async function sendCmd(body){
  const r=await fetch('/sms',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded','x-fieldbrief-test':'${token}'},body:new URLSearchParams({From:ACCOUNT,To:'+18053104809',Body:body})});
  const x=await r.text();const m=x.match(/<Message>([\\s\\S]*?)<\\/Message>/);
  return m?m[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&apos;/g,"'").replace(/&quot;/g,'"'):'(no reply)';
}
async function logJob(){const t=document.getElementById('jobtext').value.trim();if(!t)return;const res=document.getElementById('result');res.textContent='Logging…';res.textContent=await sendCmd(t);document.getElementById('jobtext').value='';setTimeout(()=>location.reload(),1400);}
async function makeInvoice(){const c=document.getElementById('invcust').value.trim();if(!c)return;const res=document.getElementById('result');res.textContent='Building…';const reply=await sendCmd('INVOICE '+c);res.textContent=reply;const lm=reply.match(/(https?:\\/\\/\\S+\\/invoice\\/\\S+)/);if(lm)window.open(lm[1],'_blank');}
async function makeProposal(){const t=document.getElementById('proptext').value.trim();if(!t)return;const res=document.getElementById('result');res.textContent='Building…';const reply=await sendCmd('PROPOSAL '+t);res.textContent=reply;const lm=reply.match(/(https?:\\/\\/\\S+\\/proposal\\/\\S+)/);if(lm)window.open(lm[1],'_blank');document.getElementById('proptext').value='';}
async function markPaid(){const v=document.getElementById('paidref').value.trim();if(!v)return;const res=document.getElementById('result');res.textContent='Marking paid…';res.textContent=await sendCmd('PAID '+v);document.getElementById('paidref').value='';setTimeout(()=>location.reload(),1400);}
async function scheduleDay(){const t=document.getElementById('plantext').value.trim();if(!t)return;const res=document.getElementById('result');res.textContent='Scheduling…';res.textContent=await sendCmd('SCHEDULE '+t);document.getElementById('plantext').value='';setTimeout(()=>location.reload(),1400);}
async function dispatchCrew(){if(!confirm("Text today's schedule to the crew now?"))return;const res=document.getElementById('result');res.textContent='Sending…';res.textContent=await sendCmd('DISPATCH');setTimeout(()=>location.reload(),1600);}
</script>
</body></html>`);
});

// ----------------------------------------------------------------------------
// FEATURE REQUESTS — public form at /features, stored for the owner to review.
// ----------------------------------------------------------------------------
app.get('/features', (req, res) => {
  const sent = req.query.sent === '1';
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Request a feature — FieldBrief</title><style>
body{font:16px/1.6 -apple-system,system-ui,sans-serif;color:#1a1a1a;background:#f4f0e8;margin:0;padding:28px 18px}
.wrap{max-width:520px;margin:0 auto}.co{font-size:1.4rem;font-weight:800}.co b{color:#c0532b}
h1{font-size:1.3rem;margin:18px 0 4px}.sub{color:#6b6256;margin:0 0 18px}
label{display:block;font-size:.82rem;color:#6b6256;margin:14px 0 4px}
textarea,input{width:100%;box-sizing:border-box;border:1px solid #d8cfbd;border-radius:10px;padding:12px;font:inherit;background:#fff}
button{margin-top:16px;width:100%;background:#c0532b;color:#fff;border:0;border-radius:10px;padding:13px;font-weight:600;font-size:1rem;cursor:pointer}
.ok{background:#e7f3e7;border:1px solid #bcd9bc;color:#2c6b2c;border-radius:12px;padding:18px;text-align:center}</style></head>
<body><div class="wrap"><div class="co">Field<b>Brief</b></div>
${sent ? `<div class="ok" style="margin-top:20px"><div style="font-size:1.1rem;font-weight:500">✓ Got it — thank you!</div><div style="margin-top:6px">We read every request. <a href="/features" style="color:#c0532b">Send another</a></div></div>`
: `<h1>Request a feature</h1><p class="sub">What would make FieldBrief work better for your business? Tell us — we read every one.</p>
<form method="POST" action="/features">
  <label>Your idea or request *</label>
  <textarea name="request" rows="5" required placeholder="e.g. Let me attach a photo of the job to the invoice"></textarea>
  <label>Name or number (optional, so we can follow up)</label>
  <input type="text" name="contact" placeholder="optional">
  <button type="submit">Send request</button>
</form>`}
</div></body></html>`);
});

app.post('/features', async (req, res) => {
  const request = (req.body.request || '').trim();
  const contact = (req.body.contact || '').trim();
  if (!request) return res.redirect('/features');
  await airtableCreate(TABLES.FEATURES, {
    Request: request.slice(0, 250),
    Details: request,
    Contact: contact,
    Status: 'New',
    Source: 'Web',
    Date: localDate(),
  });
  res.redirect('/features?sent=1');
});

// Verify inbound webhooks really came from Twilio (HMAC over the URL + params).
// OFF by default so it can't silently break live SMS. To turn on: set
// VERIFY_TWILIO_SIGNATURE=true in Render, send yourself a test text, confirm it
// still works. The /test panel is exempted via the shared TEST_PANEL_TOKEN header.
function verifyTwilioSignature(req, res, next) {
  if (process.env.VERIFY_TWILIO_SIGNATURE !== 'true') return next();
  if (process.env.TEST_PANEL_TOKEN &&
      req.headers['x-fieldbrief-test'] === process.env.TEST_PANEL_TOKEN) return next();
  const sig = req.headers['x-twilio-signature'];
  const url = (process.env.RENDER_EXTERNAL_URL || BASE_URL) + req.originalUrl;
  const ok = twilio.validateRequest(process.env.TWILIO_AUTH_TOKEN || '', sig, url, req.body);
  if (!ok) {
    console.warn(`Rejected /sms: bad Twilio signature (from ${req.body.From || '?'})`);
    return res.status(403).type('text/xml').send('<Response/>');
  }
  next();
}

// Public one-pager to hand a new contractor.
app.get('/how', (req, res) => {
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>How to use FieldBrief</title><style>
body{font:16px/1.6 -apple-system,system-ui,sans-serif;color:#1a1a1a;background:#f4f0e8;margin:0;padding:30px 18px}
.wrap{max-width:560px;margin:0 auto}.co{font-size:1.5rem;font-weight:800}.co b{color:#c0532b}
h1{font-size:1.25rem;margin:16px 0 4px}.sub{color:#6b6256;margin:0 0 20px}
.card{background:#fff;border:1px solid #e4ddcf;border-radius:14px;padding:16px 18px;margin:12px 0}
.q{color:#6b6256;font-size:.8rem;text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px}
.ex{background:#f7f2e8;border-radius:8px;padding:8px 11px;margin:5px 0;font-size:.95rem}
.big{font-size:1.05rem;font-weight:500}</style></head><body><div class="wrap">
<div class="co">Field<b>Brief</b></div>
<h1>Run your business by text.</h1>
<p class="sub">No app. No logins. Just text your number like you'd text a person — it figures out the rest.</p>
<div class="card"><div class="q">Log a job (after each one)</div>
<div class="ex">Smith 12 Main St, boiler tune-up 2hr, $45 filter from Ferguson</div>
<div class="ex">did the Henderson place, no-heat call, replaced igniter, 1.5 hrs</div></div>
<div class="card"><div class="q">Get paid</div>
<div class="ex">send Smith their invoice</div>
<div class="ex">who owes me money</div>
<div class="ex">Smith paid</div></div>
<div class="card"><div class="q">Look things up</div>
<div class="ex">what have we done at 12 Main St</div>
<div class="ex">gate code at Smith is 4421, big dog</div></div>
<div class="card"><div class="q">Your crew</div>
<div class="ex">add my guy Mike, cell 805 555 1234</div>
<div class="ex">Mike's got Harbor Inn at 8, Smith at 11 — then: send the crew their schedule</div></div>
<div class="card"><div class="q">Set up once</div>
<div class="ex">set my rate to 195</div>
<div class="ex">set my markup to 30</div>
<div class="ex">my email is you@yourco.com</div></div>
<p class="sub big">That's it. Text a job. We handle the paperwork.</p>
</div></body></html>`);
});

// ----------------------------------------------------------------------------
// WEB SIGNUP — the fieldbrief.ai signup form posts here so an ad/landing-page
// lead is INSTANTLY provisioned (trial account + welcome text) instead of
// sitting silent in a lead list. Also texts the owner that someone signed up.
// ----------------------------------------------------------------------------
const SIGNUP_ALLOWED_ORIGINS = ['https://fieldbrief.ai', 'https://www.fieldbrief.ai'];
function signupCors(req, res) {
  const o = req.headers.origin || '';
  if (SIGNUP_ALLOWED_ORIGINS.includes(o)) res.set('Access-Control-Allow-Origin', o);
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
}
app.options('/signup', (req, res) => { signupCors(req, res); res.sendStatus(204); });
app.post('/signup', async (req, res) => {
  signupCors(req, res);
  try {
    // Honeypot: real users leave the hidden bot-field empty.
    if ((req.body['bot-field'] || '').trim()) return res.json({ ok: true });
    const first = (req.body.first_name || '').trim();
    const last = (req.body.last_name || '').trim();
    const name = `${first} ${last}`.trim() || first || 'there';
    const cell = normalizePhone(req.body.phone || '');
    if (!cell || cell.replace(/[^0-9]/g, '').length < 11) {
      return res.status(400).json({ ok: false, error: 'valid phone required' });
    }
    // Dedupe: never double-create or re-text a number already on file.
    const existing = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${cell}"`);
    if (existing.length) return res.json({ ok: true, already: true });
    const id = await airtableCreate(TABLES.SUBSCRIBERS, {
      'Full Name': name, 'Phone Number': cell, 'Status': 'Active',
    });
    if (!id) return res.status(500).json({ ok: false });
    // Welcome text — they gave SMS consent on the form (consent line under it).
    try {
      await sendSMS(cell, `Welcome to FieldBrief, ${(first || name).split(' ')[0]}! This is your line. After a job, just text what you did — e.g. "Smith 12 Main St, boiler tune-up 2hr, $45 filter" — and it logs + builds the invoice. Ask "who owes me", say "send Smith's invoice", or "PROPOSAL Smith: ..." to quote a job. Reply HELP anytime · guide: ${BASE_URL}/how · reply STOP to opt out.`);
    } catch (e) { console.error('signup welcome SMS failed:', e.message); }
    // Tell the owner a lead just came in (so you don't have to watch email).
    try { for (const a of ADMIN_PHONES) await sendSMS(a, `🎉 New FieldBrief signup: ${name} (${cell}). They got a welcome text.`); } catch (e) {}
    return res.json({ ok: true });
  } catch (e) {
    console.error('signup error:', e.message);
    return res.status(500).json({ ok: false });
  }
});

app.post('/sms', verifyTwilioSignature, async (req, res) => {
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
    const KEYWORDS = ['JOBS', 'PARTS', 'INVOICE', 'PROPOSAL', 'QUOTE', 'ESTIMATE', 'BRIEF', 'STATUS', 'SETTINGS', 'SET', 'UNDO', 'FIX', 'COMMANDS', 'TECHS', 'HISTORY', 'HELP', 'INFO', 'UNPAID', 'OUTSTANDING', 'PAID', 'RESEND', 'SCHEDULE', 'DISPATCH', 'NOTE', 'ONBOARD'];
    const isCommand = KEYWORDS.includes(firstWord) || /^(ADD|REMOVE)\s+TECH\b/i.test(smsBody.trim());
    if (isCommand) {
      intent = 'command';
      response = await handleCommand(smsBody, accountPhone, actorName, isOwner);
    } else {
      // No explicit keyword — let the AI understand plain English and route it.
      const r = await routeIntent(smsBody);
      intent = r.action;
      console.log(`Routed: ${r.action} -> ${r.command}`);
      if (r.action === 'log') {
        response = await handleJobLog(smsBody, accountPhone, actorName);
      } else if (r.action === 'command') {
        response = await handleCommand(r.command || smsBody, accountPhone, actorName, isOwner);
      } else if (r.action === 'support' || r.action === 'billing') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'support');
      } else if (r.action === 'cancel') {
        response = await handleSupportTicket(smsBody, accountPhone, actorName, 'cancel');
      } else {
        // general/unsure — substantive text is almost always a job; otherwise nudge.
        if (smsBody.trim().split(/\s+/).length >= 3) {
          intent = 'log';
          response = await handleJobLog(smsBody, accountPhone, actorName);
        } else {
          response = "Hey! Just tell me what you did — e.g. \"Smith 12 Main St, boiler tune-up 2hr, $45 filter\" — or ask things like \"who owes me\" or \"send Smith's invoice\".";
        }
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
