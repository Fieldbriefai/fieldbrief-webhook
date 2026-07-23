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
// Stripe needs the RAW body to verify webhook signatures — must run BEFORE the
// JSON/urlencoded parsers, or they consume the body and signature checks fail.
app.use('/stripe', express.raw({ type: '*/*' }));
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
// Dedicated number for all end-customer-facing texts (dispatch confirmations, opt-in
// asks, follow-ups, check-ins) — kept separate from the subscriber/tech toll-free line
// above so customer replies never get mixed up with owner commands, and a customer's
// account is identified by which number they texted, not by guessing from their phone.
const TWILIO_CUSTOMER_NUMBER = process.env.TWILIO_CUSTOMER_NUMBER || '+18053104809';
// After-hours booking line — a THIRD, separate number, dedicated to a single
// pilot account's own after-hours calls/texts (dogfood before this becomes a
// generic per-subscriber feature). Kept apart from TWILIO_CUSTOMER_NUMBER
// above because that one is shared across every subscriber's customers; a
// public "text us to schedule" line needs to belong to exactly one business.
// Empty by default so the new voice/SMS branches below are a no-op — and
// therefore safe to ship — until all three vars are actually configured.
const TWILIO_BOOKING_NUMBER = process.env.TWILIO_BOOKING_NUMBER || '';
// Rung in PARALLEL (whoever picks up first wins) — e.g. a work number and a
// personal cell — rather than one at a time, to give the missed-call fallback
// the best real shot at actually reaching someone before it gives up.
// BOOKING_FORWARD_TO_2 is optional; leave it unset to ring just one number.
const BOOKING_FORWARD_TO = process.env.BOOKING_FORWARD_TO || ''; // E.164
const BOOKING_FORWARD_TO_2 = process.env.BOOKING_FORWARD_TO_2 || ''; // E.164, optional
const BOOKING_ACCOUNT_PHONE = process.env.BOOKING_ACCOUNT_PHONE || ''; // that owner's SUBSCRIBERS 'Phone Number'
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
  // One-off crew reminders ("remind me at 4pm to grab the pump") — see the
  // CREW REMINDERS section. Fields: 'Phone', 'Account Phone', 'Requested By',
  // 'Text', 'Due At' (UTC ISO), 'Status' (Pending/Sent/Cancelled).
  REMINDERS: process.env.REMINDERS_TABLE_ID || 'tbltSRagt8QB0fv33',
  // Crew ops (see CREW OPS section): supply requests, end-of-day one-liners,
  // and time-off requests — all fold into the owner's morning digest.
  PARTS_REQUESTS: process.env.PARTS_REQUESTS_TABLE_ID || 'tblO2JPxJ5g04zElw',
  EOD_REPORTS: process.env.EOD_REPORTS_TABLE_ID || 'tblBePT3rZrgqchW5',
  TIME_OFF: process.env.TIME_OFF_TABLE_ID || 'tblRnk0zp8oucujDg',
  // Field-taught fault-code reference the tech helper treats as authoritative
  // ("TEACH HTP F13 = fan speed error"). Shared across accounts on purpose —
  // an HTP F13 means the same thing in anyone's boiler room.
  FAULT_CODES: process.env.FAULT_CODES_TABLE_ID || 'tblM9oZ5c5ne0dJ1n',
  // Created manually in Airtable for the after-hours booking pilot — see
  // BOOKINGS_TABLE_ID in Render → Environment. Fields: 'Customer Phone',
  // 'Status' (Open/Confirmed/Cancelled/Escalated), 'Name', 'Address', 'Issue',
  // 'Day', 'Booking Date', 'Proposed Slots', 'Chosen Slot', 'Schedule Record ID'.
  BOOKINGS: process.env.BOOKINGS_TABLE_ID || '',
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

// Every text that reaches an end customer (never a subscriber or tech) goes out from
// TWILIO_CUSTOMER_NUMBER instead — see handleApproveCommand, the only caller.
async function sendCustomerSMS(toNumber, message) {
  try {
    const response = await twilioClient.messages.create({ body: message, from: TWILIO_CUSTOMER_NUMBER, to: toNumber });
    console.log(`Customer SMS sent to ${toNumber}:`, response.sid);
    return response.sid;
  } catch (error) { console.error(`Failed to send customer SMS to ${toNumber}:`, error); return null; }
}

// Proactive text to a caller who just hit the after-hours voice fallback (see
// /booking-voice-status) — sent from TWILIO_BOOKING_NUMBER so it's the same
// number they just called and the one they'll reply to.
async function sendBookingSMS(toNumber, message) {
  try {
    const response = await twilioClient.messages.create({ body: message, from: TWILIO_BOOKING_NUMBER, to: toNumber });
    console.log(`Booking SMS sent to ${toNumber}:`, response.sid);
    return response.sid;
  } catch (error) { console.error(`Failed to send booking SMS to ${toNumber}:`, error); return null; }
}

// A genuine emergency deserves more than a text buried in notifications — an
// actual ringing call is far more likely to wake someone at 2am. Used only
// for the "urgent" flag in handleBookingSMS, alongside (not instead of) the
// usual sendSMS alert to ADMIN_PHONES.
async function alertUrgentByCall(toNumber) {
  if (!toNumber) return;
  try {
    await twilioClient.calls.create({
      to: toNumber,
      from: TWILIO_BOOKING_NUMBER,
      twiml: '<Response><Say>Urgent after-hours booking alert. Check your texts now.</Say></Response>',
    });
  } catch (error) { console.error(`Urgent call alert to ${toNumber} failed:`, error.message); }
}

function createTwiMLResponse(message) {
  return `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${escapeXML(message)}</Message></Response>`;
}

function escapeXML(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

// ----------------------------------------------------------------------------
// LANGUAGE MIRRORING — replies match the language the customer texts in.
// Detection is per-message (no stored preference needed): accented characters
// or common Spanish words in the inbound SMS flip the reply to Spanish. The
// /sms handler sets res.locals.wantsSpanish; replyTwiML localizes on the way
// out, so every reply path (onboarding, commands, job logs, errors) is covered
// by this single seam.
// ----------------------------------------------------------------------------
const SPANISH_RE = /[¿¡ñ]|[áéíóú]{1}|\b(hola|gracias|trabajo|trabajos|cliente|cuanto|cuánto|hoy|ayer|hice|terminé|termine|instalé|instale|cambié|cambie|reparé|repare|arreglé|arregle|casa|calle|bomba|caldera|horas|factura|facturas|cotización|cotizacion|presupuesto|dónde|donde está|qué|cómo|por favor|listo|mañana|ayuda|pesos)\b/i;
const isSpanish = (t) => SPANISH_RE.test(String(t || ''));

async function localizeReply(message) {
  try {
    const t = await claudeText({
      max_tokens: 600,
      content: message,
      system: 'Translate this SMS from an AI assistant for field-service contractors into natural, friendly Latin American Spanish. Keep ALL numbers, currency amounts, dates, URLs, email addresses, proper names, and command keywords (HELP, JOBS, UNPAID, PAID, PROPOSAL, INVOICE, BRIEF, STATUS, HISTORY, RESEND, SKIP, STOP, DEMO, BILLING) EXACTLY as they are. Match the original\'s length and tone. Return ONLY the translated text, nothing else.',
    });
    return t || message;
  } catch (e) { console.error('localize failed:', e.message); return message; }
}

async function replyTwiML(res, message) {
  let msg = message || 'Message received.';
  if (res.locals && res.locals.wantsSpanish) msg = await localizeReply(msg);
  res.type('text/xml').send(createTwiMLResponse(msg));
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
// Optional `webSearch: true` lets the model run real web searches (used by the
// tech helper for manufacturer fault codes it has no reference for). If the
// API rejects the tool for any reason, the caller's catch handles it — pass
// webSearch only where a retry-without-tools fallback exists.
async function claudeText({ system, content, max_tokens = 400, webSearch = false }) {
  const body = { model: CLAUDE_MODEL, max_tokens, system, messages: [{ role: 'user', content }] };
  if (webSearch) body.tools = [{ type: 'web_search_20250305', name: 'web_search', max_uses: 3 }];
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
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
      system: `You route a contractor's text (English OR Spanish) in a field-service tool ("run your business by text"). Pick the action and rewrite it into a normalized command. Command keywords are always English regardless of input language; free-text parts of the command (customer, scope, prices) stay in the original language. Return ONLY JSON: {"action":"...","command":"..."}.

action="log" — they're reporting work done / a job to record (a customer or property, address, service, parts, or hours). command = the original text.

action="command" — they want one of these; set command to the EXACT normalized form:
- today's jobs -> "JOBS"
- parts used -> "PARTS"
- past work at a place -> "HISTORY <address or customer>"
- create/send an invoice (for work already done) -> "INVOICE <customer or address>"
- create/send a quote/estimate/proposal (for work NOT yet done, with prices) -> "PROPOSAL <customer>: <scope + prices verbatim>"
- cancel their paid subscription / stop being billed / "cancel my plan" / "I don't want to pay anymore" -> "CANCEL SUBSCRIPTION"
- manage billing, update card, change plan, see invoices -> "BILLING"
- get their referral/share link, refer a friend, "invite a buddy" -> "REFER"
- mark an invoice paid -> "PAID <customer or invoice#>"
- who owes me / outstanding -> "UNPAID"
- remind/resend an invoice -> "RESEND <customer>"
- save a note about a customer -> "NOTE <customer>: <the note>"
- add a worker/tech -> "ADD TECH <phone digits> <name>"
- remove a tech -> "REMOVE TECH <phone>"
- list techs -> "TECHS"
- relay/send a message to a tech or the crew ("text Mike ...", "tell Darin to ...", "send them a welcome text", "let the guys know ...") -> "TEXT TECH <name or ALL>: <the message>" (use ALL when it's everyone/them/the crew)
- assign today's jobs to techs -> "SCHEDULE <the assignments, verbatim>"
- send the crew their schedule -> "DISPATCH"
- change a setting -> "SET <KEY> <value>" (KEY one of RATE, MARKUP, COMPANY, EMAIL, LICENSE, PAY, TECHINVOICE)
- view settings -> "SETTINGS"
- account/dashboard -> "STATUS"
- undo last entry -> "UNDO"
- fix last entry -> "FIX <HOURS|CUSTOMER|JOB|PART> <value>"
- morning summary -> "BRIEF"
- help / what can you do -> "HELP"

action="remind" — they want a one-off reminder text sent later, to themselves ("remind me at 4 to grab the pump", "ping me tomorrow morning about the permit") OR to a crew member by name ("remind Jaylen at 4pm to grab the pump"). Catch misspellings of remind too ("irmeind me a 4pm..."). NOT for relaying a plain message right now (that's TEXT TECH). command = the original text.

action="techhelp" — a TECHNICAL trade question about equipment, diagnostics, error/fault codes, specs, parts, or install/service procedure ("what's error 110 on a weil-mclain ultra", "min gas pressure for a raypak 406", "how do I purge air out of a zone"). NOT questions about FieldBrief itself (that's support). command = the original question.

action="parts_request" — they NEED materials bought/picked up for the FUTURE ("need a 3/4 ball valve for Montecito", "we're out of 1in copper", "order me a taco 007"). Distinct from log: log reports parts already USED on finished work; parts_request asks for parts they don't have yet. command = original text.

action="timeoff" — they're requesting time off / a day off / saying they won't be in ("I need Friday off", "can I take the 28th off", "not coming in tomorrow, kid's sick"). command = original text.

action="support" — a question, problem, or complaint about FieldBrief/the service itself (command = original).
action="cancel" — wants to cancel/unsubscribe (command = original).
action="general" — ONLY greetings/thanks/unclear with no work content (command = original).

For SET, always normalize to "SET <KEY> <value>" with the bare KEY (drop filler like "my"/"to"/"is"): "set my rate to 195"->{"action":"command","command":"SET RATE 195"}; "my markup is 30%"->{"action":"command","command":"SET MARKUP 30"}; "change my company name to Smith Plumbing"->{"action":"command","command":"SET COMPANY Smith Plumbing"}.
If unsure between log and general, choose log when there's any work or customer content. Examples: "smith paid up"->{"action":"command","command":"PAID Smith"}; "add my guy mike 805 555 1234"->{"action":"command","command":"ADD TECH 8055551234 Mike"}; "who owes me"->{"action":"command","command":"UNPAID"}; "what'd we do at 412 state"->{"action":"command","command":"HISTORY 412 State"}; "gate code at smith is 1234"->{"action":"command","command":"NOTE Smith: gate code 1234"}; "tell mike to grab the pump before the harbor job"->{"action":"command","command":"TEXT TECH Mike: grab the pump before the harbor job"}; "send them a welcome text and tell them to save this number"->{"action":"command","command":"TEXT TECH ALL: Welcome to FieldBrief! Save this number — your jobs get texted here and you can text back what you did."}; "send smith their bill"->{"action":"command","command":"INVOICE Smith"}; "quote the jones job to replace their water heater, 6hr labor $900, heater and parts $1100"->{"action":"command","command":"PROPOSAL Jones: replace water heater, 6hr labor $900, heater and parts $1100"}; "did the henderson boiler 2hr replaced igniter $40"->{"action":"log","command":"did the henderson boiler 2hr replaced igniter $40"}. Return ONLY the JSON.`,
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
      system: `You are a job log parser for field service contractors. Extract structured data from casual SMS messages, which may be in English or Spanish.
Ignore any leading filler like "add job", "job for", "log", "did", "completed" (or Spanish equivalents like "trabajo", "hice", "terminé").
The contractor ${subscriberName} is reporting work they completed today.
Parse the text into this JSON structure (include only fields that are present):
{
  "customer": { "name": "", "first_name": "", "last_name": "", "phone": "", "address": "", "city": "", "state": "" },
  "equipment": { "category": "", "manufacturer": "", "model": "", "serial_number": "", "fuel_type": "" },
  "work_order": { "job_type": "", "description": "", "labor_hours": 0, "status": "Completed" },
  "parts": [{ "name": "", "supplier": "", "cost": 0, "quantity": 1, "category": "" }]
}
Common abbreviations: WM=Weil-McLain, circ=circulator pump, EWT=electric water tank, ASHP=air-source heat pump, RTU=rooftop unit.
If the customer is an individual person, set first_name and last_name (and "name" = the full name). If it's a business/property, put it in "name" and leave first_name/last_name blank.
If a phone number appears anywhere near the customer's name or address, extract it into customer.phone exactly as written (don't invent one if none is present).
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
      support: `You are FieldBrief's support assistant, replying inside the product's own SMS thread. FieldBrief runs a contractor's business 100% by text — there is NO app, NO website login for techs. It absolutely CAN send texts (you are one). What it does: techs text in jobs; SCHEDULE assigns the day's jobs; DISPATCH texts each tech their job list; TEXT TECH [name]: [msg] relays any message to a tech (ALL = whole crew); ADD TECH [phone] [name] adds a tech and texts them a welcome; INVOICE/PROPOSAL/UNPAID/PAID handle billing; "remind me at 4pm to ..." sets reminders. NEVER say you lack messaging/email access, and NEVER invent features or apps. Answer the question by pointing to the exact command that does it (HELP lists all). Keep response to 1-2 sentences, max 160 chars.`,
      feature_request: 'Thank the contractor for their feature suggestion. Keep to 1-2 sentences, max 160 chars.',
      billing: 'Address the billing question helpfully. Keep to 1-2 sentences, max 160 chars.',
      cancel: 'Acknowledge their cancellation request. Keep to 1-2 sentences, max 160 chars.',
    };
    const text = await claudeText({ max_tokens: 100, content: smsBody, system: prompts[ticketType] || prompts.support });
    return text || 'Thanks for reaching out.';
  } catch (error) { return 'Thanks for reaching out. We\'ll review this and get back to you.'; }
}

// Data-driven daily brief — yesterday's work, money outstanding, proposals
// waiting. This is the product's daily touchpoint: it must show THEIR numbers,
// not motivational filler, or trial users write it off as a toy.
async function buildDailyBrief(phone) {
  const [yJobs, unpaid, drafts] = await Promise.all([
    airtableQuery(TABLES.WORK_ORDERS, `AND({subscriber_phone} = "${phone}", DATESTR({date}) = "${localDate(-1)}")`),
    airtableQuery(TABLES.INVOICES, `AND({subscriber_phone} = "${phone}", {status} = "Sent")`),
    airtableQuery(TABLES.PROPOSALS, `AND({subscriber_phone} = "${phone}", {status} = "Draft")`),
  ]);
  const day = new Date().toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'America/Los_Angeles' });
  // Nothing on file at all — a brand-new account. One clear next action.
  if (!yJobs.length && !unpaid.length && !drafts.length) {
    return `YOUR BRIEF — ${day}\n\nNothing on file yet — let's fix that today. Text me a job when you wrap it up, e.g. "Smith 12 Main St, tune-up 2hr, $45 filter", and tomorrow's brief will have your real numbers.\n\nCommands: JOBS · UNPAID · PROPOSAL · HELP`;
  }
  const parts = [`YOUR BRIEF — ${day}`];
  if (yJobs.length) {
    parts.push(`YESTERDAY (${yJobs.length} job${yJobs.length > 1 ? 's' : ''})\n` + yJobs.slice(0, 5).map(j =>
      `· ${j.fields.customer_name || 'Unknown'} — ${j.fields.job_type || 'Service'}${j.fields.labor_hours ? ` (${j.fields.labor_hours}h)` : ''}`).join('\n'));
  }
  if (unpaid.length) {
    const total = unpaid.reduce((s, i) => s + (i.fields.amount || 0), 0);
    parts.push(`OUTSTANDING (${money(total)})\n` + unpaid.slice(0, 4).map(i => {
      const d = daysSince(i.fields.sent_date);
      return `· ${i.fields.customer_name} — ${money(i.fields.amount || 0)}${d != null ? ` (${d}d)` : ''}${(d != null && d >= 14) ? ' ⚠' : ''}`;
    }).join('\n') + (unpaid.length > 4 ? `\n+${unpaid.length - 4} more (text UNPAID)` : ''));
  } else {
    parts.push('OUTSTANDING\n· All caught up — nothing unpaid. 💪');
  }
  if (drafts.length) {
    parts.push(`PROPOSALS WAITING TO SEND (${drafts.length})\n` + drafts.slice(0, 3).map(p =>
      `· ${p.fields.customer_name} — ${money(p.fields.amount || 0)}`).join('\n'));
  }
  parts.push('Text me today\'s jobs and I\'ll handle the paperwork.');
  return parts.join('\n\n');
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
    const customerPhone = parsedData.customer?.phone ? normalizePhone(parsedData.customer.phone) : '';
    if (parsedData.customer?.name) {
      const existing = await airtableQuery(TABLES.CUSTOMERS,
        `AND({customer_name} = "${customerName}", {subscriber_phone} = "${subscriberPhone}")`);
      if (existing.length === 0) {
        await airtableCreate(TABLES.CUSTOMERS, {
          customer_name: customerName,
          first_name: parsedData.customer.first_name || '',
          last_name: parsedData.customer.last_name || '',
          phone: customerPhone,
          address: parsedData.customer.address || '',
          city: parsedData.customer.city || '',
          state: parsedData.customer.state || '',
          subscriber_phone: subscriberPhone,
        });
      } else if (customerPhone && !existing[0].fields.phone) {
        // Backfill: record already existed (e.g. from before this shipped) but had no phone on file.
        await airtableUpdate(TABLES.CUSTOMERS, existing[0].id, { phone: customerPhone });
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

// Conversational onboarding — walks a brand-new subscriber through company -> rate
// -> email by text so the owner never has to set anyone up by hand. Returns the
// next prompt to send, or null when finished (caller falls through to normal flow).
async function handleOnboardingReply(body, rec) {
  const step = (rec.fields['Onboard Step'] || '').toLowerCase();
  const text = (body || '').trim();
  const skip = /^(skip|next|later|skip it|omitir|saltar|luego|despu[eé]s)$/i.test(text);
  if (step === 'name') {
    // DEMO-provisioned accounts start here — the form collects a name, a text doesn't.
    const patch = { 'Onboard Step': 'company' };
    if (!skip && text) patch['Full Name'] = text;
    await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, patch);
    const first = (!skip && text) ? text.split(/\s+/)[0] : 'there';
    return `Nice to meet you, ${first}! What's your company name? (Reply SKIP to set it later.)`;
  }
  if (step === 'company') {
    const patch = { 'Onboard Step': 'rate' };
    if (!skip && text) patch['Company'] = text;
    await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, patch);
    const who = (!skip && text) ? text : 'your company';
    return `Got it — ${who}. What's your hourly labor rate? Just the number, e.g. 195. (Reply SKIP to set it later.)`;
  }
  if (step === 'rate') {
    const patch = { 'Onboard Step': 'email' };
    if (!skip) { const r = parseFloat(text.replace(/[^0-9.]/g, '')); if (r > 0) patch['Hourly Rate'] = r; }
    await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, patch);
    return `Perfect. Last thing — what email should your customers' invoice replies go to? (Reply SKIP to set it later.)`;
  }
  if (step === 'email') {
    const patch = { 'Onboard Step': 'done' };
    if (!skip) { const m = text.match(/\S+@\S+\.\S+/); if (m) patch['Contractor Email'] = m[0]; }
    await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, patch);
    return `🎉 You're all set! Now just text me what you did after a job — e.g. "Smith 12 Main St, boiler tune-up 2hr, $45 filter" — and I'll log it and build the invoice. You can also send a quote ("PROPOSAL Smith: ...") or ask "who owes me". Reply HELP anytime to see everything I can do.`;
  }
  if (rec.id) await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, { 'Onboard Step': 'done' });
  return null;
}

// ============================================================================
// BILLING SELF-SERVE — cancel / manage a paid Stripe subscription by text.
// ============================================================================
async function handleCancelSubscription(phone) {
  const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${phone}"`);
  const rec = subs[0];
  const custId = rec?.fields['Stripe Customer'] || '';
  if (!custId) {
    return "I don't see a paid plan on your account — if you're on a free website trial there's nothing to cancel. (Reply STOP to stop texts.) Need help? Just reply and we'll sort it out.";
  }
  try {
    const list = await stripe.subscriptions.list({ customer: custId, status: 'all', limit: 10 });
    const active = list.data.filter(su => ['active', 'trialing', 'past_due'].includes(su.status));
    if (!active.length) return "Your subscription is already canceled — you won't be charged again. Reply STOP to also stop texts.";
    for (const su of active) await stripe.subscriptions.update(su.id, { cancel_at_period_end: true });
    const ts = active[0].current_period_end || active[0].trial_end;
    const when = ts ? new Date(ts * 1000).toISOString().slice(0, 10) : 'the end of your current period';
    for (const a of ADMIN_PHONES) { if (a !== phone) sendSMS(a, `⚠️ ${rec.fields['Full Name'] || phone} CANCELED their FieldBrief subscription (ends ${when}).`); }
    return `Done — your FieldBrief subscription is canceled. You won't be charged again, and you keep access until ${when}. Changed your mind? Reply BILLING to reactivate, or visit fieldbrief.ai. Thanks for giving it a shot!`;
  } catch (e) {
    console.error('cancel sub error:', e.message);
    for (const a of ADMIN_PHONES) { if (a !== phone) sendSMS(a, `⚠️ ${phone} tried to CANCEL but it errored (${e.message}). Handle manually so they aren't charged.`); }
    return "I hit a snag canceling automatically — but I've flagged it for the team and we'll take care of it right away. You won't be charged while we sort it out.";
  }
}

// Trial users have no Stripe customer — they pay by tapping a payment link.
// Checkout matches them back to this account by phone number, hence the
// "use this same number" instruction.
const PLAN_LINKS_MSG = `Founding offer: 50% off your first 3 months, first 20 shops only — already applied to these links. Takes 2 minutes; use this same phone number at checkout so your account links up:
• Solo $39 → $19.50/mo: https://buy.stripe.com/8x200k8Gyd9q4dY7OIbsc07?prefilled_promo_code=FOUNDING50
• Crew $89 → $44.50/mo (unlimited techs): https://buy.stripe.com/fZucN6aOGc5m5i2ed6bsc08?prefilled_promo_code=FOUNDING50
• Command $149 → $74.50/mo (auto-chases overdue invoices): https://buy.stripe.com/fZu7sMbSK0mEh0Kb0Ubsc09?prefilled_promo_code=FOUNDING50
Questions? Just reply.`;

async function handleBillingPortal(phone) {
  const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${phone}"`);
  const custId = subs[0]?.fields['Stripe Customer'] || '';
  if (!custId) {
    return PLAN_LINKS_MSG;
  }
  try {
    const session = await stripe.billingPortal.sessions.create({ customer: custId, return_url: 'https://fieldbrief.ai' });
    return `Manage your FieldBrief billing — update your card, change your plan, or cancel — here: ${session.url}`;
  } catch (e) {
    console.error('billing portal error:', e.message);
    return "Couldn't open your billing page just now. To cancel, reply CANCEL SUBSCRIPTION. For anything else, just reply and we'll help.";
  }
}

// ============================================================================
// TRIAL NUDGES — keep a 15-day trial converting: a day-2 "log your first job"
// nudge (only if they haven't), and a day-12 "trial's ending" nudge. Runs daily.
// Deduped via the Trial Nudges field; windows (not exact days) survive a missed run.
// ============================================================================
async function runTrialNudges() {
  let n1 = 0, n2 = 0;
  try {
    const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    const today = Date.parse(localDate());
    for (const sub of subs) {
      const f = sub.fields; const phone = f['Phone Number'];
      if (!phone) continue;
      const step = (f['Onboard Step'] || '').toLowerCase();
      if (step && step !== 'done') continue; // still onboarding — leave them be
      const start = f['Signed Up'] || (sub.createdTime ? sub.createdTime.slice(0, 10) : null);
      if (!start) continue;
      const days = Math.floor((today - Date.parse(String(start).slice(0, 10))) / 86400000);
      const sent = f['Trial Nudges'] || '';
      // Day 2–5: haven't logged a single job yet → nudge to try the core loop.
      if (days >= 2 && days <= 5 && !sent.includes('n1')) {
        const jobs = await airtableQuery(TABLES.WORK_ORDERS, `{subscriber_phone} = "${phone}"`);
        if (jobs.length === 0) {
          await sendSMS(phone, `It's FieldBrief 👋 Haven't seen a job from you yet! Next time you wrap one up, just text me what you did — e.g. "Smith 12 Main St, water heater install 4hr, $1200 unit" — and I'll build the invoice for you. Give it a shot!`);
          await airtableUpdate(TABLES.SUBSCRIBERS, sub.id, { 'Trial Nudges': (sent ? sent + ',' : '') + 'n1' });
          n1++;
        }
      }
      // Day 12–14: trial ending soon → convert / reassure.
      if (days >= 12 && days <= 14 && !sent.includes('n2')) {
        const msg = f['Stripe Customer']
          ? `Heads up — your FieldBrief free trial ends in a few days. If it's saving you time, do nothing and you'll roll right into your plan. Want to change it or cancel? Reply BILLING. Thanks for giving it a run!`
          : `You've been running your shop on FieldBrief for ~2 weeks 🙌 Ready to lock it in? ${PLAN_LINKS_MSG}`;
        await sendSMS(phone, msg);
        await airtableUpdate(TABLES.SUBSCRIBERS, sub.id, { 'Trial Nudges': (sent ? sent + ',' : '') + 'n2' });
        n2++;
      }
    }
  } catch (e) { console.error('trial nudges error:', e.message); }
  return { n1, n2 };
}

// ============================================================================
// LEAD FOLLOW-UP — a proposal sent 3+ days ago with no reply gets a drafted
// nudge to the customer. Promotional, so it's gated on sms_opt_in_status: a
// customer who's never been asked gets the opt-in text drafted instead of
// marketing content; nothing here calls sendSMS to a customer directly — the
// owner reviews and sends via APPROVE (see handleApproveCommand).
// ============================================================================
async function runLeadFollowups() {
  let drafted = 0, notified = 0;
  try {
    const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    for (const sub of subs) {
      const phone = sub.fields['Phone Number'];
      if (!phone) continue;
      const props = await airtableQuery(TABLES.PROPOSALS, `AND({subscriber_phone} = "${phone}", {status} = "Sent")`);
      if (!props.length) continue;
      const acct = await getSubscriberSettings(phone);
      const company = acct.company || sub.fields['Full Name'] || 'your contractor';
      const newDrafts = [];
      for (const p of props) {
        const f = p.fields;
        if (!f.customer_phone) continue;
        if ((f.followup_sent || '').includes('f1')) continue;
        const d = daysSince(f.sent_date);
        if (d == null || d < 3) continue;
        const custRows = await airtableQuery(TABLES.CUSTOMERS, `{phone} = "${f.customer_phone}"`);
        const optStatus = custRows.length ? (custRows[0].fields.sms_opt_in_status || 'Not Asked') : 'Not Asked';
        if (optStatus === 'Opted Out' || optStatus === 'Pending') continue; // declined, or already asked and awaiting reply
        const text = optStatus === 'Opted In'
          ? `Hi, this is ${company} — just following up on the proposal for ${f.customer_name || 'your project'}. Any questions, or ready to move forward? Reply STOP to opt out.`
          : `This is ${company} via FieldBrief — reply YES for occasional service updates, or STOP to opt out.`;
        await airtableUpdate(TABLES.PROPOSALS, p.id, {
          'Followup Msg Status': 'Pending Approval',
          'Followup Msg Text': text,
          followup_sent: (f.followup_sent ? f.followup_sent + ',' : '') + 'f1',
        });
        newDrafts.push({ label: f.customer_name || 'customer', text });
        drafted++;
      }
      if (newDrafts.length) {
        await sendSMS(phone, `🔔 ${newDrafts.length} lead follow-up${newDrafts.length > 1 ? 's' : ''} ready to review:\n` +
          newDrafts.map((d, i) => `[${i + 1}] ${d.label}: "${d.text}"`).join('\n') +
          `\nReply APPROVE to send, or SKIP to discard.`);
        notified++;
      }
    }
  } catch (e) { console.error('lead followups error:', e.message); }
  return { drafted, notified };
}

// ============================================================================
// MAINTENANCE CHECK-INS — a customer whose last logged job is 120+ days old
// gets a drafted re-engagement text. Same opt-in gate and owner-review rule
// as lead follow-up. maintenance_sent stores the last-job-date this customer
// was already drafted for, so a new job (moving that date forward) is what
// re-arms the next cycle rather than a one-time flag.
// ============================================================================
async function runMaintenanceCheckins() {
  let drafted = 0, notified = 0;
  try {
    const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    for (const sub of subs) {
      const phone = sub.fields['Phone Number'];
      if (!phone) continue;
      const customers = await airtableQuery(TABLES.CUSTOMERS, `AND({subscriber_phone} = "${phone}", {phone} != "")`);
      if (!customers.length) continue;
      const acct = await getSubscriberSettings(phone);
      const company = acct.company || sub.fields['Full Name'] || 'your contractor';
      const newDrafts = [];
      for (const c of customers) {
        const cf = c.fields;
        if (!cf.phone) continue;
        const optStatus = cf.sms_opt_in_status || 'Not Asked';
        if (optStatus === 'Opted Out' || optStatus === 'Pending') continue;
        const esc = (cf.customer_name || '').replace(/"/g, '\\"');
        const jobs = await airtableQuery(TABLES.WORK_ORDERS, `AND({subscriber_phone} = "${phone}", {customer_name} = "${esc}")`);
        if (!jobs.length) continue;
        const lastJobDate = jobs.map(j => j.fields.date).filter(Boolean).sort().pop();
        const d = daysSince(lastJobDate);
        if (d == null || d < 120 || cf.maintenance_sent === lastJobDate) continue;
        const text = optStatus === 'Opted In'
          ? `Hi, this is ${company} — it's been a while since your last service. Want to get on the schedule for a checkup? Reply STOP to opt out.`
          : `This is ${company} via FieldBrief — reply YES for occasional service reminders, or STOP to opt out.`;
        await airtableUpdate(TABLES.CUSTOMERS, c.id, {
          'Maintenance Msg Status': 'Pending Approval', 'Maintenance Msg Text': text, maintenance_sent: lastJobDate,
        });
        newDrafts.push({ label: cf.customer_name || 'customer', text });
        drafted++;
      }
      if (newDrafts.length) {
        await sendSMS(phone, `🔧 ${newDrafts.length} maintenance check-in${newDrafts.length > 1 ? 's' : ''} ready to review:\n` +
          newDrafts.map((d, i) => `[${i + 1}] ${d.label}: "${d.text}"`).join('\n') +
          `\nReply APPROVE to send, or SKIP to discard.`);
        notified++;
      }
    }
  } catch (e) { console.error('maintenance checkins error:', e.message); }
  return { drafted, notified };
}

// ============================================================================
// REFERRALS — every customer can bring more. Text REFER for a share link;
// when a new shop signs up with it, the referrer gets a free month (Stripe
// coupon applied if they're paid; banked as a credit otherwise).
// ============================================================================
function genRefCode() {
  const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  let c = ''; for (let i = 0; i < 6; i++) c += chars[Math.floor(Math.random() * chars.length)];
  return c;
}
async function handleReferral(phone) {
  const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${phone}"`);
  const rec = subs[0];
  if (!rec) return 'Get set up first, then text REFER for your share link.';
  let code = rec.fields['Referral Code'];
  if (!code) { code = genRefCode(); await airtableUpdate(TABLES.SUBSCRIBERS, rec.id, { 'Referral Code': code }); }
  const credits = rec.fields['Referral Credits'] || 0;
  return `Share FieldBrief, get a FREE MONTH for every shop that joins 🤝\nYour link: https://fieldbrief.ai?ref=${code}\nThey get a deal too. Send it to a buddy in the trades.${credits ? `\n(You've earned ${credits} free month${credits > 1 ? 's' : ''} so far!)` : ''}`;
}
async function creditReferrer(refCode) {
  if (!refCode) return;
  const refs = await airtableQuery(TABLES.SUBSCRIBERS, `{Referral Code} = "${String(refCode).replace(/[^A-Za-z0-9]/g, '')}"`);
  if (!refs.length) return;
  const r = refs[0];
  const credits = (r.fields['Referral Credits'] || 0) + 1;
  await airtableUpdate(TABLES.SUBSCRIBERS, r.id, { 'Referral Credits': credits });
  let applied = false;
  const cust = r.fields['Stripe Customer'];
  if (cust) {
    try {
      const list = await stripe.subscriptions.list({ customer: cust, status: 'all', limit: 5 });
      const active = list.data.find(s => ['active', 'trialing', 'past_due'].includes(s.status));
      if (active) { await stripe.subscriptions.update(active.id, { coupon: 'REFERRAL_FREE_MONTH' }); applied = true; }
    } catch (e) { console.error('referral coupon error:', e.message); }
  }
  const ph = r.fields['Phone Number'];
  if (ph) sendSMS(ph, applied
    ? `🎉 Someone signed up with your FieldBrief link — you just earned a FREE MONTH, applied to your account! Keep sharing: text REFER.`
    : `🎉 Someone signed up with your FieldBrief link — you've earned a free month! It applies automatically to your plan. Keep sharing: text REFER.`);
  for (const a of ADMIN_PHONES) { if (a !== ph) sendSMS(a, `🔗 Referral: ${r.fields['Full Name'] || ph} referred a new signup (#${credits}).`); }
}

// ============================================================================
// OWNER PULSE — weekly text to the owner so the business stays hands-off but
// transparent: active/paid counts, ~MRR, new signups, churn.
// ============================================================================
async function ownerPulse() {
  try {
    const active = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    const cancelled = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Cancelled"`);
    const paid = active.filter(s => s.fields['Stripe Customer']);
    const today = Date.parse(localDate());
    const newWeek = active.filter(s => { const su = s.fields['Signed Up']; return su && (today - Date.parse(String(su).slice(0, 10))) <= 7 * 86400000; });
    const price = { solo: 39, crew: 89, command: 149 };
    const mrr = paid.reduce((sum, s) => sum + (price[(s.fields['Subscription Plan'] || '').toLowerCase()] || 0), 0);
    const msg = `📊 FieldBrief weekly pulse\n• Active accounts: ${active.length}\n• Paying: ${paid.length} (~$${mrr}/mo)\n• New this week: ${newWeek.length}\n• Cancelled (total): ${cancelled.length}\nFeed the ad + referrals to grow. Text REFER to share.`;
    for (const a of ADMIN_PHONES) await sendSMS(a, msg);
    return msg;
  } catch (e) { console.error('owner pulse error:', e.message); return 'pulse error'; }
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

// Welcome text sent to a tech when the owner adds them. Techs never see the
// signup flow, so this is their only onboarding — it must say what THEY can do.
async function sendTechWelcome(techPhone, techName, accountPhone) {
  const acct = await getSubscriberSettings(accountPhone).catch(() => ({}));
  const from = acct.company ? ` — ${acct.company} added you` : '';
  try {
    await sendSMS(techPhone, `Hi ${(techName || '').split(' ')[0]}, welcome to FieldBrief${from}! Save this number. Your day's jobs get texted here, and you text back what you did after each one to log it. You can also say "remind me at 4pm to grab the pump", or ASK any technical question (error codes, specs, diagnostics). Reply HELP anytime.`);
    return true;
  } catch (e) { console.error('tech welcome SMS failed:', e.message); return false; }
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
    const w = await sendTechWelcome(phone, name, accountPhone);
    return `✓ Updated ${name} (${phone}). They can text jobs into your account.${w ? ' Welcome text sent to them.' : ''}`;
  }
  const id = await airtableCreate(TABLES.TECHS, { Phone: phone, Name: name, 'Account Phone': accountPhone, Active: true });
  if (!id) return 'Could not add that tech. Try again.';
  const w = await sendTechWelcome(phone, name, accountPhone);
  return `✓ Added ${name} (${phone}). They can now text jobs in — each tagged as theirs. No billing access.${w ? ' Welcome text sent to them.' : ''}`;
}

// TEXT TECH — owner relays a message straight to a tech (or ALL techs). This is
// what "tell Mike to grab the pump" / "send them a welcome text" resolves to.
async function handleTextTech(command, accountPhone, isOwner) {
  if (!isOwner) return 'Only the account owner can text the crew.';
  const m = command.match(/^\s*TEXT\s+TECH\s+([^:]+):\s*([\s\S]+)$/i);
  if (!m) return 'Usage: TEXT TECH [name]: [message] — or TEXT TECH ALL: [message] for the whole crew.';
  const who = m[1].trim();
  const msg = m[2].trim();
  const acct = await getSubscriberSettings(accountPhone).catch(() => ({}));
  const prefix = acct.company ? `${acct.company}: ` : 'From the office: ';
  let targets = [];
  if (/^(ALL|CREW|EVERYONE|TODOS)$/i.test(who)) {
    const techs = await airtableQuery(TABLES.TECHS, `AND({Account Phone} = "${accountPhone}", {Active} = 1)`);
    targets = techs.filter(t => t.fields.Phone).map(t => ({ phone: t.fields.Phone, name: t.fields.Name || 'Tech' }));
    if (!targets.length) return 'No techs with numbers on file. ADD TECH [phone] [name] first.';
  } else {
    // Accept "Mike", "Mike and Darin", "Mike, Darin"
    const names = who.split(/\s*(?:,|&|\band\b|\by\b)\s*/i).filter(Boolean);
    const missing = [];
    for (const n of names) {
      const t = await techPhoneByName(accountPhone, n);
      if (t && t.phone) targets.push(t); else missing.push(n);
    }
    if (missing.length) return `No number on file for: ${missing.join(', ')}. TECHS lists your crew; ADD TECH [phone] [name] to add.`;
  }
  for (const t of targets) await sendSMS(t.phone, `${prefix}${msg}`);
  return `✓ Texted ${targets.map(t => t.name).join(', ')}: "${msg.slice(0, 80)}${msg.length > 80 ? '…' : ''}"`;
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
<p style="max-width:640px;margin:16px auto;color:#6b6256;font:13px sans-serif;text-align:center">View online: <a href="${viewUrl}">${viewUrl}</a></p>${FB_EMAIL_FOOTER}</body></html>`;
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
[{"tech":"","time":"","customer":"","customerPhone":"","address":"","job":""}]
"tech" = the technician's first name the job is for. Multiple techs and multiple jobs per tech are common. Keep times like "8a","1p". If no tech is named, use "" for tech. If a customer phone number appears, put it in "customerPhone" exactly as written; otherwise leave it "".`,
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
      Time: j.time || '', Customer: j.customer || '', 'Customer Phone': j.customerPhone ? normalizePhone(j.customerPhone) : '',
      Address: j.address || '', Job: j.job || '',
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
  // Draft a same-day confirmation for any row with a customer phone on file. Nothing
  // texts the customer here — the owner must reply APPROVE first (see handleApproveCommand).
  const acct = await getSubscriberSettings(accountPhone);
  const company = acct.company || 'your contractor';
  const drafts = [];
  for (const r of rows) {
    const custPhone = r.fields['Customer Phone'];
    if (!custPhone || r.fields['Customer Msg Status']) continue;
    const when = r.fields.Time ? ` around ${r.fields.Time}` : '';
    const job = r.fields.Job || 'your service visit';
    const text = `Hi, this is ${company} — you're on today's schedule for ${job}${when}. Reply STOP to opt out of texts.`;
    await airtableUpdate(TABLES.SCHEDULE, r.id, { 'Customer Msg Status': 'Pending Approval', 'Customer Msg Text': text });
    drafts.push({ label: r.fields.Customer || job, text });
  }
  let msg = sent.length ? `✓ Sent to: ${sent.join(', ')}.` : 'Nothing sent.';
  if (skipped.length) msg += ` No number for: ${[...new Set(skipped)].join(', ')} (ADD TECH to fix).`;
  if (drafts.length) {
    msg += `\n\n${drafts.length} customer confirmation${drafts.length > 1 ? 's' : ''} ready to review:\n` +
      drafts.map((d, i) => `[${i + 1}] ${d.label}: "${d.text}"`).join('\n') +
      `\nReply APPROVE to send ${drafts.length > 1 ? 'all' : 'it'}, or SKIP to discard.`;
  }
  return msg;
}

// ============================================================================
// APPROVE / SKIP — every customer-facing text (dispatch confirmation, opt-in
// ask, lead follow-up, maintenance check-in) is drafted first with a
// "Pending Approval" status; nothing reaches a customer's phone until the
// owner reviews the exact wording and replies APPROVE. This is the ONLY path
// that actually sends any of those drafted messages.
// ============================================================================
async function handleApproveCommand(command, subscriberPhone, approve, isOwner) {
  if (!isOwner) return 'Only the account owner can approve or skip customer messages.';
  const nMatch = command.trim().match(/(\d+)\s*$/);
  const onlyIndex = nMatch ? parseInt(nMatch[1], 10) : null;

  const [schedRows, propRows, custRows] = await Promise.all([
    airtableQuery(TABLES.SCHEDULE, `AND({Account Phone} = "${subscriberPhone}", {Customer Msg Status} = "Pending Approval")`),
    airtableQuery(TABLES.PROPOSALS, `AND({subscriber_phone} = "${subscriberPhone}", {Followup Msg Status} = "Pending Approval")`),
    airtableQuery(TABLES.CUSTOMERS, `AND({subscriber_phone} = "${subscriberPhone}", {Maintenance Msg Status} = "Pending Approval")`),
  ]);
  const pending = [
    ...schedRows.map(r => ({ table: TABLES.SCHEDULE, id: r.id, phone: r.fields['Customer Phone'], text: r.fields['Customer Msg Text'], statusField: 'Customer Msg Status', label: r.fields.Customer || r.fields.Job || 'customer' })),
    ...propRows.map(r => ({ table: TABLES.PROPOSALS, id: r.id, phone: r.fields.customer_phone, text: r.fields['Followup Msg Text'], statusField: 'Followup Msg Status', label: r.fields.customer_name || 'customer' })),
    ...custRows.map(r => ({ table: TABLES.CUSTOMERS, id: r.id, phone: r.fields.phone, text: r.fields['Maintenance Msg Text'], statusField: 'Maintenance Msg Status', label: r.fields.customer_name || 'customer' })),
  ];
  if (pending.length === 0) return 'Nothing pending review right now.';

  const targets = onlyIndex ? [pending[onlyIndex - 1]].filter(Boolean) : pending;
  if (onlyIndex && targets.length === 0) return `No pending item #${onlyIndex}. Reply APPROVE or SKIP with no number to act on all ${pending.length}.`;

  let count = 0;
  for (const item of targets) {
    if (!item.phone || !item.text) continue;
    if (approve) {
      await sendCustomerSMS(item.phone, item.text);
      // If this was an opt-in ask (customer still "Not Asked"), move them to "Pending"
      // now that it's actually gone out — not when it was only drafted.
      if (item.table !== TABLES.SCHEDULE) {
        const cust = await airtableQuery(TABLES.CUSTOMERS, `{phone} = "${item.phone}"`);
        if (cust.length && (cust[0].fields.sms_opt_in_status || 'Not Asked') === 'Not Asked') {
          await airtableUpdate(TABLES.CUSTOMERS, cust[0].id, { sms_opt_in_status: 'Pending', opt_in_asked_date: localDate() });
        }
      }
    }
    await airtableUpdate(item.table, item.id, { [item.statusField]: approve ? 'Sent' : 'Skipped' });
    count++;
  }
  return approve
    ? `✓ Sent ${count} customer message${count === 1 ? '' : 's'}.`
    : `Discarded ${count} pending message${count === 1 ? '' : 's'}.`;
}

// ============================================================================
// CREW OPS — three crew-facing flows on the 855 line, all rolled into one
// morning digest text to the account owner (see sendOwnerDigest):
//   • Parts requests — "need a 3/4 ball valve and 2 unions for Montecito"
//   • End-of-day one-liners — "EOD: Montecito PM done, started 801 C St"
//   • Time off — "I need Friday off"
// Owner also gets an immediate ping for parts and time-off (those are
// actionable now); EOD lines just accumulate for the morning.
// ============================================================================

async function handlePartsRequest(smsBody, phone, accountPhone, actorName, isOwner) {
  let parsed;
  try {
    const text = await claudeText({
      max_tokens: 250,
      content: smsBody,
      system: 'A field tech is texting in a SUPPLY REQUEST (parts/materials they need bought or picked up — not parts already used on a job). Return ONLY JSON: {"items":["qty + item", ...], "job": "customer/property/job name or null"}. Keep item wording close to the original, one array entry per distinct item. If no actual request for materials is present, return {"error":"none"}.',
    });
    const m = text.match(/\{[\s\S]*\}/);
    parsed = JSON.parse(m ? m[0] : text);
  } catch (e) { console.error('parts parse failed:', e.message); parsed = { error: 'none' }; }
  if (!parsed || parsed.error || !Array.isArray(parsed.items) || !parsed.items.length) {
    return 'What do you need? List it like: "need a 3/4 ball valve and 2 unions for Montecito".';
  }
  const items = parsed.items.join('\n');
  await airtableCreate(TABLES.PARTS_REQUESTS, {
    Phone: phone, 'Requested By': actorName, 'Account Phone': accountPhone,
    Items: items, Job: parsed.job || '', Status: 'New', 'Created At': new Date().toISOString(),
  });
  if (!isOwner) {
    await sendSMS(accountPhone, `🛒 Parts request from ${actorName.split(' ')[0]}${parsed.job ? ` (${parsed.job})` : ''}:\n${items}`);
  }
  return `🛒 Got it — sent to the boss${parsed.job ? ` for ${parsed.job}` : ''}:\n${items}`;
}

async function handleEodReport(report, phone, accountPhone, actorName) {
  const clean = report.trim();
  if (!clean) return 'Add the recap after EOD — like: "EOD: Montecito PM done, started the 801 C St repipe, need parts Friday".';
  await airtableCreate(TABLES.EOD_REPORTS, {
    Phone: phone, Name: actorName, 'Account Phone': accountPhone,
    Date: localDate(), Report: clean,
  });
  return '✓ Logged — the boss sees it in the morning brief. Have a good night.';
}

async function handleTimeOffRequest(smsBody, phone, accountPhone, actorName, isOwner) {
  const nowLocal = new Date().toLocaleString('en-US', { timeZone: BUSINESS_TZ, weekday: 'long', year: 'numeric', month: '2-digit', day: '2-digit' });
  let parsed;
  try {
    const text = await claudeText({
      max_tokens: 200,
      content: smsBody,
      system: `A field tech is requesting time off. Today is ${nowLocal}. Return ONLY JSON: {"dates":"human-readable date(s), e.g. Fri Jul 25 or Jul 28-30","note":"reason if given, else empty string"}. If no time-off request is present, return {"error":"none"}.`,
    });
    const m = text.match(/\{[\s\S]*\}/);
    parsed = JSON.parse(m ? m[0] : text);
  } catch (e) { console.error('timeoff parse failed:', e.message); parsed = { error: 'none' }; }
  if (!parsed || parsed.error || !parsed.dates) {
    return 'Which day(s) do you need off? Like: "I need Friday off" or "taking the 28th-30th off".';
  }
  await airtableCreate(TABLES.TIME_OFF, {
    Phone: phone, Name: actorName, 'Account Phone': accountPhone,
    Dates: parsed.dates, Note: parsed.note || '', Status: 'Requested', 'Created At': new Date().toISOString(),
  });
  if (!isOwner) {
    await sendSMS(accountPhone, `🏖️ Time-off request from ${actorName.split(' ')[0]}: ${parsed.dates}${parsed.note ? ` — ${parsed.note}` : ''}`);
  }
  return `✓ Sent to the boss: ${parsed.dates} off. He'll get back to you.`;
}

// Morning digest to each opted-in owner (LOG_NUDGE_ACCOUNTS): yesterday's EOD
// one-liners (back through Friday when it's Monday), open parts requests, and
// pending time-off. Skips the text entirely when there's nothing to say.
async function sendOwnerDigest() {
  for (const acct of LOG_NUDGE_ACCOUNTS) {
    try {
      const dow = new Date().toLocaleDateString('en-US', { weekday: 'short', timeZone: BUSINESS_TZ });
      const backTo = dow === 'Mon' ? localDate(-3) : localDate(-1);
      const [eods, parts, timeoff] = await Promise.all([
        airtableQuery(TABLES.EOD_REPORTS, `AND({Account Phone} = "${acct}", {Date} >= "${backTo}")`),
        airtableQuery(TABLES.PARTS_REQUESTS, `AND({Account Phone} = "${acct}", {Status} = "New")`),
        airtableQuery(TABLES.TIME_OFF, `AND({Account Phone} = "${acct}", {Status} = "Requested")`),
      ]);
      const secs = [];
      if (eods.length) secs.push('Crew EOD:\n' + eods.map(r => `• ${(r.fields.Name || '?').split(' ')[0]}: ${r.fields.Report}`).join('\n'));
      if (parts.length) secs.push('🛒 Open parts requests:\n' + parts.map(r => `• ${(r.fields['Requested By'] || '?').split(' ')[0]}${r.fields.Job ? ` (${r.fields.Job})` : ''}: ${String(r.fields.Items || '').replace(/\n/g, ', ')}`).join('\n'));
      if (timeoff.length) secs.push('🏖️ Pending time off:\n' + timeoff.map(r => `• ${(r.fields.Name || '?').split(' ')[0]}: ${r.fields.Dates}${r.fields.Note ? ` — ${r.fields.Note}` : ''}`).join('\n'));
      if (!secs.length) continue;
      await sendSMS(acct, `☀️ Crew brief:\n\n${secs.join('\n\n')}`);
      console.log(`Owner digest sent to ${acct}`);
    } catch (e) { console.error(`owner digest failed for ${acct}:`, e.message); }
  }
}

// ============================================================================
// TECH HELPER — a field tech texts a technical question ("ASK what's error
// 110 on a Weil-McLain Ultra", or just asks naturally and the classifier
// routes it) and gets a concise expert answer back. Single-shot Q&A with a
// short follow-up window: the tech's last few helper questions (from SMS_LOG)
// ride along so "it's the 155 model" works as a follow-up.
// ============================================================================
async function handleTechHelp(question, phone, actorName) {
  let context = '';
  try {
    const cutoff = new Date(Date.now() - 30 * 60000).toISOString();
    const prior = await airtableQuery(TABLES.SMS_LOG, `AND({from_number} = "${phone}", {parsed_intent} = "techhelp", {timestamp} > "${cutoff}")`);
    const lines = prior.map(r => r.fields).sort((a, b) => (a.timestamp || '').localeCompare(b.timestamp || '')).slice(-3).map(f => f.body);
    if (lines.length) context = `\n\n(Earlier questions from this tech in the last 30 min, oldest first: ${lines.join(' | ')})`;
  } catch (e) { /* context is a nice-to-have, never block the answer */ }
  // Field-taught reference: if the question mentions any code we've been
  // TAUGHT, inject those entries as ground truth the model must not override.
  // Match on code tokens (F13, E02, err 110...) so brand misspellings still hit.
  let reference = '';
  try {
    const codes = [...new Set((question.match(/\b(?:[A-Za-z]{1,3}[- ]?\d{1,4}|\d{2,4})\b/g) || []).map(c => c.replace(/[- ]/g, '').toUpperCase()))];
    if (codes.length) {
      const all = await airtableQuery(TABLES.FAULT_CODES, 'TRUE()');
      const hits = all.filter(r => codes.includes(String(r.fields.Code || '').replace(/[- ]/g, '').toUpperCase()));
      if (hits.length) reference = '\n\nAUTHORITATIVE FIELD REFERENCE (taught by our own techs — trust this over anything else, including web results):\n' + hits.map(r => `${r.fields.Brand} ${r.fields.Code} = ${r.fields.Meaning}`).join('\n');
    }
  } catch (e) { console.error('fault code lookup failed:', e.message); }
  const system = 'You are a master boiler/hydronics/HVAC technician answering a field tech\'s question over SMS. DEFAULT TO SEARCHING THE WEB: for ANY question involving specific equipment, specs, codes, parts, procedures, or facts, run a web search FIRST and base your answer on what you verify — your memory is only for framing, not facts. Exception: if an AUTHORITATIVE FIELD REFERENCE is provided, it outranks both memory and web results. Be direct and practical: most-likely answer or diagnostic steps first, numbered when there are several. HARD LIMIT ~450 characters of final answer — no preamble, no sign-off, no citations. If you can\'t verify a manufacturer-specific fault code, say plainly "I can\'t verify what that code means on that unit — check the fault table in the manual" and give generic next steps; NEVER guess. When you give a spec, setting, or pressure/clearance value, end with "(verify against manual/nameplate)". If the question involves a suspected gas leak or CO exposure, lead with: evacuate, shut gas at the meter only if safe, call the gas utility. If it isn\'t a technical question, answer helpfully in one sentence anyway. Answer in English.';
  try {
    let answer;
    try {
      answer = await claudeText({ max_tokens: 2000, webSearch: true, content: question + reference + context, system });
    } catch (e) {
      // Web-search tool unavailable/rejected — answer from the reference alone.
      console.error('tech help web search failed, retrying without:', e.message);
      answer = await claudeText({ max_tokens: 350, content: question + reference + context, system });
    }
    return (answer || '').trim() || 'Came up empty on that one — try rewording it, or include the make/model.';
  } catch (e) {
    console.error('tech help failed:', e.message);
    return 'The helper hit a snag — try again in a minute.';
  }
}

// "TEACH HTP F13 = fan speed error" — anyone on the crew can correct or add a
// fault-code meaning; from then on the helper treats it as ground truth.
async function handleTeach(smsBody, phone, accountPhone, actorName) {
  const m = smsBody.trim().match(/^teach\s+(.+?)\s+([A-Za-z]{0,3}[- ]?\d{1,4})\s*[=:—-]\s*(.+)$/i);
  if (!m) return 'Format: TEACH <brand> <code> = <what it means>. Example: TEACH HTP F13 = fan speed error';
  const brand = m[1].trim(), code = m[2].replace(/[- ]/g, '').toUpperCase(), meaning = m[3].trim();
  try {
    const existing = await airtableQuery(TABLES.FAULT_CODES, `AND(UPPER({Brand}) = "${brand.toUpperCase()}", UPPER(SUBSTITUTE({Code}, "-", "")) = "${code}")`);
    if (existing.length) {
      await airtableUpdate(TABLES.FAULT_CODES, existing[0].id, { Meaning: meaning, 'Taught By': actorName });
      return `📖 Updated: ${brand} ${code} = ${meaning}. The helper will use this from now on.`;
    }
    await airtableCreate(TABLES.FAULT_CODES, { Brand: brand, Code: code, Meaning: meaning, 'Taught By': actorName, 'Account Phone': accountPhone });
    return `📖 Learned: ${brand} ${code} = ${meaning}. The helper will use this from now on.`;
  } catch (e) { console.error('teach failed:', e.message); return 'Couldn\'t save that — try again.'; }
}

// ============================================================================
// CREW REMINDERS — any owner or tech can text "remind me at 4pm to grab the
// pump" on the crew line and get a text back at that time. Backed by
// TABLES.REMINDERS + a once-a-minute dispatcher cron. Times are interpreted
// in BUSINESS_TZ and stored as UTC ISO strings so a plain string compare in
// the Airtable filter finds what's due.
// ============================================================================

// "YYYY-MM-DD HH:mm" wall-clock in BUSINESS_TZ → UTC Date. Two-pass fixed-point
// (render the guess back into the zone, correct by the difference) so DST
// transitions land right without a timezone library.
function businessTimeToUtc(ymdHm) {
  const target = new Date(ymdHm.replace(' ', 'T') + ':00Z').getTime();
  let guess = new Date(target);
  for (let i = 0; i < 3; i++) {
    const p = new Intl.DateTimeFormat('en-CA', {
      timeZone: BUSINESS_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
    }).formatToParts(guess).reduce((o, x) => (o[x.type] = x.value, o), {});
    const rendered = Date.parse(`${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:00Z`);
    if (rendered === target) break;
    guess = new Date(guess.getTime() + (target - rendered));
  }
  return guess;
}

function formatReminderTime(utcDate) {
  const now = new Date();
  const dayOf = (d) => new Intl.DateTimeFormat('en-CA', { timeZone: BUSINESS_TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
  const time = utcDate.toLocaleTimeString('en-US', { timeZone: BUSINESS_TZ, hour: 'numeric', minute: '2-digit' });
  if (dayOf(utcDate) === dayOf(now)) return `today at ${time}`;
  if (dayOf(utcDate) === dayOf(new Date(now.getTime() + 86400000))) return `tomorrow at ${time}`;
  return utcDate.toLocaleDateString('en-US', { timeZone: BUSINESS_TZ, weekday: 'short', month: 'short', day: 'numeric' }) + ` at ${time}`;
}

async function handleReminderCreate(smsBody, phone, accountPhone, actorName, isOwner) {
  const nowLocal = new Date().toLocaleString('en-US', {
    timeZone: BUSINESS_TZ, weekday: 'long', year: 'numeric', month: '2-digit',
    day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false,
  });
  // Owners can aim a reminder at a crew member by name ("remind Jaylen at
  // 4pm to grab the pump"); give the extractor the real roster so it never
  // invents a target.
  const techs = isOwner ? await airtableQuery(TABLES.TECHS, `AND({Account Phone} = "${accountPhone}", {Active} = 1)`) : [];
  const roster = techs.map(t => t.fields['Name']).filter(Boolean);
  let parsed;
  try {
    const text = await claudeText({
      max_tokens: 200,
      content: smsBody,
      system: `Extract a reminder from a contractor's text (English or Spanish, tolerate typos like "irmeind"="remind", "a 4pm"="at 4pm"). Current local time: ${nowLocal} (${BUSINESS_TZ}). Return ONLY JSON: {"task":"...","due":"YYYY-MM-DD HH:mm","who":null} — due is LOCAL wall-clock time. Rules: if the stated time-of-day already passed today, use tomorrow; "in 20 min"/"in 2 hours" = offset from now; morning=08:00, lunch=12:00, tonight/evening=18:00, end of day=16:30 when no exact time given. task = short imperative phrase of what to do, in the sender's language, no leading "to". "who": if the reminder is for someone else by name ("remind Jaylen to..."), the name as written; for the sender themselves ("remind me"), null.${roster.length ? ` Known crew names: ${roster.join(', ')}.` : ''} If there is no recognizable reminder request or no way to pick a time, return {"error":"no_time"}.`,
    });
    const m = text.match(/\{[\s\S]*\}/);
    parsed = JSON.parse(m ? m[0] : text);
  } catch (e) { console.error('reminder parse failed:', e.message); parsed = { error: 'no_time' }; }
  if (!parsed || parsed.error || !parsed.task || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(parsed.due || '')) {
    return 'I couldn\'t catch the time on that. Try: "remind me at 4pm to grab the pump" or "remind me tomorrow at 7am to call the supply house".';
  }
  const due = businessTimeToUtc(parsed.due);
  if (due.getTime() < Date.now() + 30000) {
    return 'That time already passed — give me a time coming up, like "remind me at 4pm to grab the pump".';
  }
  // Resolve a named target to a real tech. Non-owners can only remind
  // themselves — a tech naming someone else just gets a self-reminder rule.
  let targetPhone = phone, forName = '';
  if (parsed.who && String(parsed.who).toLowerCase() !== 'me') {
    if (!isOwner) return 'Only the account owner can set reminders for someone else — but "remind me at ..." works for you anytime.';
    const want = String(parsed.who).trim().toLowerCase();
    const hit = techs.find(t => {
      const n = (t.fields['Name'] || '').toLowerCase();
      return n === want || n.split(/\s+/)[0] === want.split(/\s+/)[0];
    });
    if (!hit) return `I don't have "${parsed.who}" on your crew.${roster.length ? ` Active techs: ${roster.join(', ')}.` : ''} Add them first with: ADD TECH <phone> <name>`;
    targetPhone = hit.fields['Phone'];
    forName = hit.fields['Name'] || String(parsed.who);
  }
  await airtableCreate(TABLES.REMINDERS, {
    Phone: targetPhone, 'Account Phone': accountPhone, 'Requested By': actorName,
    For: forName, Text: parsed.task, 'Due At': due.toISOString(), Status: 'Pending',
  });
  return `⏰ Got it — I'll text ${forName ? forName.split(' ')[0] : 'you'} ${formatReminderTime(due)}: "${parsed.task}". Text REMINDERS to see what's pending.`;
}

// The owner sees every pending reminder on the account (labeled by person);
// techs see just their own.
async function handleReminderList(phone, accountPhone, isOwner) {
  const filter = isOwner
    ? `AND({Account Phone} = "${accountPhone}", {Status} = "Pending")`
    : `AND({Phone} = "${phone}", {Status} = "Pending")`;
  const recs = await airtableQuery(TABLES.REMINDERS, filter);
  if (!recs.length) return 'No pending reminders. Set one anytime: "remind me at 4pm to grab the pump".';
  const lines = recs
    .map(r => ({ due: new Date(r.fields['Due At']), task: r.fields['Text'], who: r.fields['For'] || (r.fields['Phone'] === phone ? '' : r.fields['Requested By']) }))
    .sort((a, b) => a.due - b.due)
    .map(x => `• ${x.who ? `[${String(x.who).split(' ')[0]}] ` : ''}${formatReminderTime(x.due)} — ${x.task}`);
  return `Pending reminders:\n${lines.join('\n')}\n\nText CANCEL REMINDERS to clear them.`;
}

// Cancels what you own: your own reminders plus (for anyone) the ones you
// personally set for other people.
async function handleReminderCancel(phone, actorName) {
  const recs = await airtableQuery(TABLES.REMINDERS, `AND({Status} = "Pending", OR({Phone} = "${phone}", AND({Requested By} = "${actorName}", {For} != "")))`);
  for (const r of recs) await airtableUpdate(TABLES.REMINDERS, r.id, { Status: 'Cancelled' });
  return recs.length ? `Cancelled ${recs.length} pending reminder${recs.length === 1 ? '' : 's'}.` : 'Nothing to cancel — you have no pending reminders.';
}

// Runs every minute from the cron block. String compare works because Due At
// is a zero-padded UTC ISO string.
async function dispatchDueReminders() {
  try {
    const due = await airtableQuery(TABLES.REMINDERS, `AND({Status} = "Pending", {Due At} <= "${new Date().toISOString()}")`);
    for (const r of due) {
      // Mark Sent BEFORE sending: if the send throws we drop one reminder,
      // but the reverse order would re-text the crew every minute forever.
      await airtableUpdate(TABLES.REMINDERS, r.id, { Status: 'Sent' });
      // Targeted reminders say who they're from ("Reminder from JJ: ...").
      const from = r.fields['For'] && r.fields['Requested By'] ? ` from ${String(r.fields['Requested By']).split(' ')[0]}` : '';
      await sendSMS(r.fields['Phone'], `⏰ Reminder${from}: ${r.fields['Text']}`);
    }
    if (due.length) console.log(`Dispatched ${due.length} reminder(s)`);
  } catch (e) { console.error('reminder dispatch failed:', e.message); }
}

// End-of-day "did you log everything" nudge, sent to every active tech of the
// accounts opted in via LOG_NUDGE_ACCOUNTS (comma-separated subscriber
// phones). Inert when the env var is unset.
const LOG_NUDGE_ACCOUNTS = (process.env.LOG_NUDGE_ACCOUNTS || '').split(',').map(s => s.trim()).filter(Boolean);
async function sendLogNudges() {
  for (const acct of LOG_NUDGE_ACCOUNTS) {
    try {
      const acctSettings = await getSubscriberSettings(acct);
      const techs = await airtableQuery(TABLES.TECHS, `AND({Account Phone} = "${acct}", {Active} = 1)`);
      for (const t of techs) {
        const name = (t.fields['Name'] || '').split(' ')[0];
        await sendSMS(t.fields['Phone'], `🔧 ${acctSettings.company || 'Shop'} end-of-day check${name ? `, ${name}` : ''}: make sure every call from today is logged — hours, materials, notes. Then reply "EOD: what you did + anything you need" and it goes in the boss's morning brief. Need parts? Text "NEED ..." anytime. Reminders: "remind me at 7pm to ...".`);
      }
      if (techs.length) console.log(`Log nudge sent to ${techs.length} tech(s) on ${acct}`);
    } catch (e) { console.error(`log nudge failed for ${acct}:`, e.message); }
  }
}

// ============================================================================
// AFTER-HOURS BOOKING (pilot, single-tenant — see TWILIO_BOOKING_NUMBER)
// A customer texting the dedicated booking number (directly, or handed off
// from a missed call — see /booking-voice) has a slot-filling conversation:
// extract whatever they've given, ask only for what's missing, then offer
// real open slots from the SAME SCHEDULE table SCHEDULE/DISPATCH already
// read/write, so a confirmed booking shows up right where the owner already
// looks for the day's jobs. State lives on a Bookings record (mirrors how
// Onboard Step tracks the onboarding flow) so it survives Twilio's stateless
// webhooks. Replies return through the same replyTwiML seam as everything
// else in /sms, so Spanish localization is automatic — no separate handling
// needed here.
// ============================================================================
const BOOKING_SLOTS = ['8:00 AM', '10:00 AM', '12:00 PM', '2:00 PM', '4:00 PM'];

async function findOpenBookingSlots(accountPhone, dateStr) {
  const rows = await airtableQuery(TABLES.SCHEDULE, `AND({Account Phone} = "${accountPhone}", DATESTR({Date}) = "${dateStr}")`);
  const taken = new Set(rows.map(r => r.fields.Time).filter(Boolean));
  return BOOKING_SLOTS.filter(s => !taken.has(s));
}

// "day" is deliberately loose free text (Claude normalizes obvious dates to
// YYYY-MM-DD, otherwise "tomorrow"/a weekday name) — resolved to a concrete
// date only right before checking availability, so re-clarifying the day
// mid-conversation just re-resolves rather than needing its own state.
function resolveBookingDate(day) {
  if (!day) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(day)) return day;
  const lower = day.toLowerCase().trim();
  if (lower === 'today') return localDate();
  if (lower === 'tomorrow') return localDate(1);
  const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  const idx = days.indexOf(lower);
  if (idx === -1) return null;
  let delta = idx - new Date().getDay();
  if (delta <= 0) delta += 7;
  return localDate(delta);
}

async function parseBookingInfo(smsBody, known) {
  try {
    const responseText = await claudeText({
      max_tokens: 500,
      content: `Known so far: ${JSON.stringify(known)}\nNew message: ${smsBody}`,
      system: `You help a boiler/HVAC service company's after-hours text line collect enough info to book a service appointment. Merge "New message" into "Known so far" and extract: the caller's name, the service address, a short description of the issue, and a preferred day (normalize an obvious specific date to YYYY-MM-DD; otherwise keep loose text like "tomorrow" or "Tuesday"). Also set "urgent": true if the issue sounds like a genuine emergency — no heat, or a leak (matches this company's own definition), plus a gas smell as an added safety flag. Return ONLY JSON: {"name":"","address":"","issue":"","day":"","urgent":false}. Use "" for anything not yet known — never invent a value that wasn't actually stated.`,
    });
    const raw = responseText.match(/\{[\s\S]*\}/)?.[0] || responseText;
    return JSON.parse(raw);
  } catch (e) { console.error('parseBookingInfo error:', e.message); return null; }
}

async function confirmBookingSlot(rec, chosen, fromNumber) {
  const dateStr = rec.fields['Booking Date'];
  // Re-check the slot is still open immediately before writing — closes the
  // window where two customers offered the same slot could both grab it.
  const stillOpen = await findOpenBookingSlots(BOOKING_ACCOUNT_PHONE, dateStr);
  if (!stillOpen.includes(chosen)) {
    if (!stillOpen.length) {
      await airtableUpdate(TABLES.BOOKINGS, rec.id, { Status: 'Escalated' });
      for (const a of ADMIN_PHONES) await sendSMS(a, `⚠️ Booking line: ${fromNumber} wanted ${dateStr} but it just filled up — call them back.`);
      return 'Sorry, that day just filled up — we\'ll call you shortly to find another time.';
    }
    await airtableUpdate(TABLES.BOOKINGS, rec.id, { 'Proposed Slots': stillOpen.join('|') });
    return `Sorry, that slot just got taken. Still open ${dateStr}: ${stillOpen.map((s, i) => `${i + 1}) ${s}`).join(', ')} — reply with a number.`;
  }
  const scheduleId = await airtableCreate(TABLES.SCHEDULE, {
    Label: `${rec.fields.Name || 'Customer'} - ${rec.fields.Issue || 'service call'} - ${dateStr}`,
    Date: dateStr, Time: chosen, Customer: rec.fields.Name || '', 'Customer Phone': fromNumber,
    Address: rec.fields.Address || '', Job: rec.fields.Issue || '',
    'Account Phone': BOOKING_ACCOUNT_PHONE, Status: 'Scheduled',
  });
  await airtableUpdate(TABLES.BOOKINGS, rec.id, { Status: 'Confirmed', 'Chosen Slot': chosen, 'Schedule Record ID': scheduleId || '' });
  for (const a of ADMIN_PHONES) await sendSMS(a, `📅 New after-hours booking: ${rec.fields.Name || fromNumber} — ${rec.fields.Issue || 'service call'} — ${dateStr} ${chosen}. ${rec.fields.Address || ''}`);
  return `You're booked for ${dateStr} at ${chosen}. We'll see you then! Reply if anything changes.`;
}

async function handleBookingSMS(fromNumber, smsBody) {
  if (!BOOKING_ACCOUNT_PHONE) return 'Sorry, this line isn\'t set up yet — please call back during business hours.';
  const upper = smsBody.trim().toUpperCase();
  if (['STOP', 'STOPALL', 'UNSUBSCRIBE', 'CANCEL', 'END', 'QUIT'].includes(upper)) {
    return 'You have been unsubscribed from this line.';
  }

  const existing = await airtableQuery(TABLES.BOOKINGS, `AND({Customer Phone} = "${fromNumber}", {Status} = "Open")`);
  const rec = existing[0] || null;
  const f = rec?.fields || {};

  // Slot-pick shortcut: only fires while slots are actually on offer, and only
  // when the reply clearly points at one (a bare number or the time itself).
  // Anything else — a new day, more detail, a question — falls through to the
  // general extraction path below instead of dead-ending on "try again".
  if (rec && f['Proposed Slots']) {
    const slots = String(f['Proposed Slots']).split('|').filter(Boolean);
    const numMatch = smsBody.trim().match(/^([1-9])\b/);
    const chosen = (numMatch && slots[parseInt(numMatch[1], 10) - 1])
      || slots.find(s => smsBody.toLowerCase().includes(s.toLowerCase().replace(/\s*(am|pm)/i, '').trim()));
    if (chosen) return await confirmBookingSlot(rec, chosen, fromNumber);
  }

  const known = { name: f.Name || '', address: f.Address || '', issue: f.Issue || '', day: f.Day || '' };
  const parsed = await parseBookingInfo(smsBody, known);
  if (!parsed) return 'Sorry, having trouble reading that — could you resend what you need and your address?';
  const merged = {
    name: parsed.name || known.name, address: parsed.address || known.address,
    issue: parsed.issue || known.issue, day: parsed.day || known.day,
  };

  const recId = rec ? rec.id : await airtableCreate(TABLES.BOOKINGS, { 'Customer Phone': fromNumber, Status: 'Open' });
  // Clear any stale slot offer whenever new info comes in — the day may have
  // just changed, and a leftover offer from the old day must not look valid.
  await airtableUpdate(TABLES.BOOKINGS, recId, { Name: merged.name, Address: merged.address, Issue: merged.issue, Day: merged.day, 'Proposed Slots': '' });

  if (parsed.urgent) {
    await airtableUpdate(TABLES.BOOKINGS, recId, { Status: 'Escalated' });
    for (const a of ADMIN_PHONES) await sendSMS(a, `🚨 Urgent after-hours text from ${fromNumber}: "${smsBody.slice(0, 140)}" — call them now.`);
    // Deliberately does NOT tell the customer to "call [the main number]" —
    // that's the same line that's already unanswered after hours, so it'd be
    // circular. Instead this escalates harder on our end: an actual ringing
    // call (not just a text) to whoever's forwarding target(s) are set,
    // since that's far more likely to actually be noticed at 2am.
    await Promise.all([alertUrgentByCall(BOOKING_FORWARD_TO), alertUrgentByCall(BOOKING_FORWARD_TO_2)]);
    return 'That sounds urgent — I\'m paging someone right now and they\'ll call you back shortly. If you smell gas, please leave the property first and call your gas utility or 911.';
  }

  const missing = [];
  if (!merged.name) missing.push('your name');
  if (!merged.address) missing.push('the service address');
  if (!merged.issue) missing.push('what\'s going on');
  if (missing.length) return `Got it. Can you also tell me ${missing.join(' and ')}?`;

  const dateStr = resolveBookingDate(merged.day);
  if (!dateStr) return 'What day works best? (e.g. "tomorrow", "Tuesday", or a date)';

  const open = await findOpenBookingSlots(BOOKING_ACCOUNT_PHONE, dateStr);
  if (!open.length) return `We're fully booked ${dateStr} — want to try a different day?`;

  await airtableUpdate(TABLES.BOOKINGS, recId, { 'Booking Date': dateStr, 'Proposed Slots': open.join('|') });
  return `For ${dateStr}, I've got: ${open.map((s, i) => `${i + 1}) ${s}`).join(', ')} — reply with a number to book it.`;
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
  const fields = { 'Full Name': name, 'Company': company, 'Phone Number': cell, 'Hourly Rate': rate, 'Status': 'Active', 'Onboard Step': 'done' };
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
// Spanish command aliases — resolved to the canonical keyword so Spanish-first
// crews can drive the whole product in their own words. Replies come back in
// Spanish via the localizeReply seam.
const ES_COMMAND_ALIASES = {
  AYUDA: 'HELP', COMANDOS: 'HELP',
  TRABAJOS: 'JOBS', TAREAS: 'JOBS',
  PARTES: 'PARTS', PIEZAS: 'PARTS',
  FACTURA: 'INVOICE', COTIZACION: 'PROPOSAL', COTIZACIÓN: 'PROPOSAL', PRESUPUESTO: 'PROPOSAL',
  HISTORIAL: 'HISTORY', HORARIO: 'SCHEDULE',
  IMPAGADAS: 'UNPAID', PENDIENTES: 'UNPAID', DEUDAS: 'UNPAID',
  PAGADO: 'PAID', PAGADA: 'PAID', REENVIAR: 'RESEND', ESTADO: 'STATUS',
  PAGAR: 'UPGRADE', PLANES: 'UPGRADE', PRECIO: 'UPGRADE', PRECIOS: 'UPGRADE',
};

async function handleCommand(command, subscriberPhone, subscriberName, isOwner = false) {
  let cmd = command.toUpperCase().trim();
  const firstWord = cmd.split(/\s+/)[0].replace(/[.,!]$/, '');
  if (ES_COMMAND_ALIASES[firstWord]) {
    cmd = ES_COMMAND_ALIASES[firstWord] + cmd.slice(firstWord.length);
    command = ES_COMMAND_ALIASES[firstWord] + command.trim().slice(firstWord.length);
  }
  const word = cmd.split(/\s+/)[0];
  if (['HELP', 'COMMANDS', 'INFO'].includes(word)) {
    return 'Just text a job to log it. Commands: JOBS · PARTS · INVOICE [customer] · PROPOSAL [customer]: [scope] · HISTORY [address] · SCHEDULE [tech jobs] · DISPATCH · APPROVE/SKIP [#] (review customer texts) · UNPAID · PAID [customer] · RESEND · STATUS · SETTINGS · TECHS · ADD TECH · TEXT TECH [name]: [msg] · UNDO · FIX · UPGRADE · BILLING · HELP — or "remind me at 4pm to grab the pump" (REMINDERS lists them) — or ASK [any technical question] for the AI tech helper (TEACH [brand] [code] = [meaning] corrects it) — or NEED [parts] (supply request) · EOD: [day recap] · "I need Friday off" (time-off request)';
  }
  if (/^(cancel|end|stop)\s+(subscription|plan|billing|membership)/i.test(command) || /^cancel\s+my\s+(subscription|plan|account|billing)/i.test(command)) {
    return await handleCancelSubscription(subscriberPhone);
  }
  if (word === 'BILLING' || word === 'MANAGE' || word === 'RESUBSCRIBE' || word === 'REACTIVATE' || word === 'UPGRADE' || word === 'PAY' || word === 'SUBSCRIBE' || cmd.startsWith('UPDATE CARD') || cmd.startsWith('UPDATE PAYMENT')) {
    return await handleBillingPortal(subscriberPhone);
  }
  if (word === 'REFER' || word === 'REFERRAL' || word === 'SHARE') {
    return await handleReferral(subscriberPhone);
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
  if (cmd.startsWith('TEXT TECH')) {
    return await handleTextTech(command, subscriberPhone, isOwner);
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
  if (word === 'APPROVE' || word === 'SKIP') {
    return await handleApproveCommand(command, subscriberPhone, word === 'APPROVE', isOwner);
  }
  if (word === 'NOTE') {
    return await handleNote(command, subscriberPhone);
  }
  if (word === 'ONBOARD') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Onboarding new businesses is admin-only.';
    return await handleOnboard(command);
  }
  if (word === 'RUNNUDGES') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Admin only.';
    const r = await runTrialNudges();
    return `Trial nudges run: ${r.n1} engagement, ${r.n2} ending-soon.`;
  }
  if (word === 'RUNFOLLOWUPS') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Admin only.';
    const r = await runLeadFollowups();
    return `Lead follow-ups run: ${r.drafted} drafted, ${r.notified} owner${r.notified === 1 ? '' : 's'} notified.`;
  }
  if (word === 'RUNCHECKINS') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Admin only.';
    const r = await runMaintenanceCheckins();
    return `Maintenance check-ins run: ${r.drafted} drafted, ${r.notified} owner${r.notified === 1 ? '' : 's'} notified.`;
  }
  if (word === 'PULSE') {
    if (!ADMIN_PHONES.includes(subscriberPhone)) return 'Admin only.';
    return await ownerPulse();
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
    return await buildDailyBrief(subscriberPhone);
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
// Viral footer on every customer-facing email — free distribution on each send.
const FB_EMAIL_FOOTER = `<p style="max-width:640px;margin:22px auto 0;text-align:center;font:12px sans-serif;color:#9a9a94">Sent with <a href="https://fieldbrief.ai" style="color:#c0532b;text-decoration:none">FieldBrief</a> — run your trades business by text. No app.</p>`;

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
  // Optional customer phone anywhere in the who segment, e.g. "Smith 555-0101 @ 12 Main St".
  let phone = '';
  const phoneMatch = who.match(/(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);
  if (phoneMatch) {
    phone = normalizePhone(phoneMatch[1]);
    who = (who.slice(0, phoneMatch.index) + who.slice(phoneMatch.index + phoneMatch[0].length)).replace(/\s+/g, ' ').trim();
  }
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
    customer: who, address, phone, date: localDate(), items, total, status: 'Draft',
  };
  const recId = await airtableCreate(TABLES.PROPOSALS, {
    proposal_label: propNum, customer_name: who, amount: total, customer_phone: phone,
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
    // High-priority (cancel/billing) — ping the owner so a customer never sits.
    if (ticketStatus === 'Escalated') {
      for (const a of ADMIN_PHONES) { if (a !== subscriberPhone) sendSMS(a, `⚠️ ${subscriberName || subscriberPhone} needs you (${ticketSubtype}): "${String(smsBody).slice(0, 120)}"`); }
    }
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
View this invoice online: <a href="${viewUrl}">${viewUrl}</a></p>${FB_EMAIL_FOOTER}</body></html>`;

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
Or view it online: <a href="${acceptUrl}">${acceptUrl}</a></p>${FB_EMAIL_FOOTER}</body></html>`;

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
      'Full Name': name, 'Phone Number': cell, 'Status': 'Active', 'Onboard Step': 'company', 'Signed Up': localDate(),
    });
    if (!id) return res.status(500).json({ ok: false });
    // Referral attribution — credit the referrer, tag who sent them.
    const ref = (req.body.ref || '').trim();
    if (ref) { try { await airtableUpdate(TABLES.SUBSCRIBERS, id, { 'Referred By': ref }); await creditReferrer(ref); } catch (e) { console.error('signup referral error:', e.message); } }
    // Welcome text kicks off the AI-guided setup (company -> rate -> email), so the
    // owner never has to onboard anyone by hand. They gave SMS consent on the form.
    // The Spanish landing page posts lang=es; from there the conversation continues
    // in whichever language they reply in (see localizeReply).
    const wantsES = (req.body.lang || '').trim().toLowerCase() === 'es';
    try {
      await sendSMS(cell, wantsES
        ? `¡Bienvenido a FieldBrief, ${(first || name).split(' ')[0]}! 👋 Te configuro en unos 30 segundos — todo por texto. Primero: ¿cómo se llama tu empresa? (Responde STOP para cancelar.)`
        : `Welcome to FieldBrief, ${(first || name).split(' ')[0]}! 👋 I'll get you set up in about 30 seconds — all by text. First: what's your company name? (Reply STOP to opt out.)`);
    } catch (e) { console.error('signup welcome SMS failed:', e.message); }
    // Tell the owner a lead just came in (so you don't have to watch email).
    try { for (const a of ADMIN_PHONES) await sendSMS(a, `🎉 New FieldBrief signup: ${name} (${cell}). They got a welcome text.`); } catch (e) {}
    return res.json({ ok: true });
  } catch (e) {
    console.error('signup error:', e.message);
    return res.status(500).json({ ok: false });
  }
});

// ============================================================================
// AFTER-HOURS BOOKING — VOICE
// Dials the owner's real cell; Twilio's own no-answer/busy/failed timeout
// (not a bare fallthrough) is what routes to the text-handoff message, via
// the action callback below — see /booking-voice-status.
// IMPORTANT (operational, not code): if the owner's cell has its own
// carrier/iOS voicemail enabled and it can pick up faster than `timeout`
// below, Twilio counts that as an ANSWERED call and this fallback never
// fires — silently defeating the whole feature. Disable or drastically
// lengthen that phone's voicemail delay before relying on this.
// ============================================================================
app.post('/booking-voice', async (req, res) => {
  const twiml = new twilio.twiml.VoiceResponse();
  if (BOOKING_FORWARD_TO) {
    // No sequential attribute = simultaneous ring — both numbers ring at
    // once, first pickup wins, rather than trying one then the other.
    const dial = twiml.dial({ timeout: 15, action: '/booking-voice-status' });
    dial.number(BOOKING_FORWARD_TO);
    if (BOOKING_FORWARD_TO_2) dial.number(BOOKING_FORWARD_TO_2);
  } else {
    // No forward number = the owner's own phone is what forwards INTO this
    // line on no-answer (their phone already rang once). Re-dialing it would
    // bounce the call back into this webhook forever, so skip the dial and
    // go straight to the text handoff — same behavior as a failed dial in
    // /booking-voice-status below.
    const caller = req.body.From || '';
    if (caller) {
      const acct = await getSubscriberSettings(BOOKING_ACCOUNT_PHONE);
      const company = acct.company || 'us';
      await sendBookingSMS(caller, `Hi, sorry we missed your call — this is ${company}'s after-hours line. Reply here with what's going on and the service address, and I'll get you scheduled right now. If you smell gas, please leave the property first and call your gas utility or 911.`);
    }
    twiml.say('Sorry we missed you. We just texted this number — reply there and we will get you scheduled right now.');
  }
  res.type('text/xml').send(twiml.toString());
});

app.post('/booking-voice-status', async (req, res) => {
  const twiml = new twilio.twiml.VoiceResponse();
  if (req.body.DialCallStatus === 'completed') {
    twiml.hangup();
  } else {
    // Don't just say a line and hang up — actively text the caller something
    // to reply to right now, instead of hoping they remember to text in on
    // their own after the call ends.
    const caller = req.body.From || '';
    if (caller) {
      const acct = await getSubscriberSettings(BOOKING_ACCOUNT_PHONE);
      const company = acct.company || 'us';
      // No "call [number] instead" line here — this fallback only fires
      // after we already tried ringing every forwarding target and nobody
      // picked up, so pointing back at a phone number would be circular.
      await sendBookingSMS(caller, `Hi, sorry we missed your call — this is ${company}'s after-hours line. Reply here with what's going on and the service address, and I'll get you scheduled right now. If you smell gas, please leave the property first and call your gas utility or 911.`);
    }
    twiml.say('Sorry we missed you. We just texted this number — reply there and we will get you scheduled right now.');
  }
  res.type('text/xml').send(twiml.toString());
});

app.post('/sms', verifyTwilioSignature, async (req, res) => {
  const fromNumber = req.body.From || '';
  const smsBody = req.body.Body || '';
  const upper = smsBody.trim().toUpperCase();
  // Reply in the customer's language (see localizeReply). Per-message, so a
  // bilingual crew can mix languages freely on the same account.
  res.locals.wantsSpanish = isSpanish(smsBody);
  console.log(`SMS from ${fromNumber}: ${smsBody}`);

  // --------------------------------------------------------------------------
  // AFTER-HOURS BOOKING NUMBER — a THIRD number (see TWILIO_BOOKING_NUMBER),
  // separate from the shared crew line and from TWILIO_CUSTOMER_NUMBER below.
  // Checked first and returns immediately: this number belongs to exactly one
  // pilot account, so it must never fall through to DEMO/signup or the
  // shared customer-number branch. No formal SMS_LOG entry (that table
  // hardcodes to_number to the shared crew number, which would misreport).
  // --------------------------------------------------------------------------
  if (TWILIO_BOOKING_NUMBER && normalizePhone(req.body.To || '') === TWILIO_BOOKING_NUMBER) {
    const msg = await handleBookingSMS(fromNumber, smsBody);
    return replyTwiML(res, msg);
  }

  // --------------------------------------------------------------------------
  // DEDICATED CUSTOMER NUMBER — anything arriving on TWILIO_CUSTOMER_NUMBER is
  // always a shop's end customer, never a subscriber or tech (those only ever
  // text the toll-free line below), so it's handled completely separately and
  // never falls through to DEMO/signup or subscriber commands.
  // --------------------------------------------------------------------------
  if (normalizePhone(req.body.To || '') === TWILIO_CUSTOMER_NUMBER) {
    if (upper === 'YES' || upper === 'STOP' || upper === 'STOPALL' || upper === 'UNSUBSCRIBE' ||
        upper === 'CANCEL' || upper === 'END' || upper === 'QUIT') {
      const pendingCustomers = await airtableQuery(TABLES.CUSTOMERS,
        `AND({phone} = "${fromNumber}", {sms_opt_in_status} = "Pending")`);
      if (pendingCustomers.length > 0) {
        const optedIn = upper === 'YES';
        await airtableUpdate(TABLES.CUSTOMERS, pendingCustomers[0].id, { sms_opt_in_status: optedIn ? 'Opted In' : 'Opted Out' });
        const msg = optedIn
          ? 'Got it — you\'re opted in for occasional service reminders. Reply STOP anytime to opt out.'
          : 'You\'ve been opted out and won\'t get further texts like this.';
        logSMS(fromNumber, smsBody, optedIn ? 'customer_opt_in' : 'customer_opt_out', msg);
        return replyTwiML(res, msg);
      }
      if (upper !== 'YES') {
        const msg = 'You have been unsubscribed.';
        logSMS(fromNumber, smsBody, 'customer_stop', msg);
        return replyTwiML(res, msg);
      }
    }
    const knownCustomer = await airtableQuery(TABLES.CUSTOMERS, `{phone} = "${fromNumber}"`);
    const acct = knownCustomer.length ? await getSubscriberSettings(knownCustomer[0].fields.subscriber_phone || '') : {};
    const company = acct.company || 'us';
    const msg = `Thanks for the reply! For anything you need, please contact ${company} directly. Reply STOP to opt out of texts.`;
    logSMS(fromNumber, smsBody, 'customer_ack', msg);
    return replyTwiML(res, msg);
  }

  // --------------------------------------------------------------------------
  // PENDING CUSTOMER OPT-IN (fallback) — the dedicated-number branch above
  // handles this normally. This only fires if a customer's reply somehow
  // lands on the toll-free line instead (e.g. an old thread from before the
  // dedicated number existed) — checked BEFORE the universal STOP/START block
  // below, because that block matches YES/STOP for ANY sender and would
  // otherwise swallow it without recording consent.
  // --------------------------------------------------------------------------
  if (upper === 'YES' || upper === 'STOP' || upper === 'STOPALL' || upper === 'UNSUBSCRIBE' ||
      upper === 'CANCEL' || upper === 'END' || upper === 'QUIT') {
    const pendingCustomers = await airtableQuery(TABLES.CUSTOMERS,
      `AND({phone} = "${fromNumber}", {sms_opt_in_status} = "Pending")`);
    if (pendingCustomers.length > 0) {
      const optedIn = upper === 'YES';
      await airtableUpdate(TABLES.CUSTOMERS, pendingCustomers[0].id, { sms_opt_in_status: optedIn ? 'Opted In' : 'Opted Out' });
      const msg = optedIn
        ? 'Got it — you\'re opted in for occasional service reminders. Reply STOP anytime to opt out.'
        : 'You\'ve been opted out and won\'t get further texts like this.';
      logSMS(fromNumber, smsBody, optedIn ? 'customer_opt_in' : 'customer_opt_out', msg);
      return replyTwiML(res, msg);
    }
  }

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
    // DEMO = instant signup. Texting DEMO provisions a full-access 15-day trial
    // (same record /signup creates) and starts by-text onboarding at the 'name'
    // step. The old look-but-don't-touch sandbox intro dead-ended prospects at
    // the signup wall on their very next text — nobody left the thread to go
    // fill out the website form.
    const existing = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${fromNumber}"`);
    if (existing.length) {
      const msg = (existing[0].fields['Status'] || '') === 'Cancelled'
        ? 'Welcome back! Your FieldBrief subscription has ended — reply BILLING to reactivate, or visit fieldbrief.ai to pick a plan.'
        : 'You already have a FieldBrief account! Just text me what you did after a job — e.g. "Smith 12 Main St, boiler tune-up 2hr, $45 filter" — and I\'ll log it and build the invoice. Reply HELP for everything I can do.';
      logSMS(fromNumber, smsBody, 'demo', msg);
      return replyTwiML(res, msg);
    }
    const id = await airtableCreate(TABLES.SUBSCRIBERS, {
      'Full Name': '', 'Phone Number': fromNumber, 'Status': 'Active', 'Onboard Step': 'name', 'Signed Up': localDate(),
    });
    // Bilingual on purpose: "DEMO" itself carries no language signal, and the
    // Spanish ad campaign points here. Their next text picks the language.
    const msg = id
      ? 'You\'re in! Full access, free for 15 days, no card needed. I\'ll set you up right here — first, what\'s your name?\n—\n¡Listo! Acceso completo, gratis por 15 días, sin tarjeta. Te configuro por aquí — primero, ¿cómo te llamas? (Reply STOP to opt out / Responde STOP para cancelar.)'
      : 'Welcome to FieldBrief! Something hiccuped setting up your trial — text DEMO again in a minute or sign up at fieldbrief.ai.';
    // Alert the owner — a DEMO text is a hot lead (likely straight off an ad).
    try { for (const a of ADMIN_PHONES) { if (a !== fromNumber) await sendSMS(a, id ? `🎉 New FieldBrief signup via DEMO text: ${fromNumber}. Trial provisioned, onboarding started by SMS.` : `⚠️ DEMO text from ${fromNumber} but trial provisioning FAILED (Airtable create). Follow up manually.`); } } catch (e) { console.error('demo lead alert failed:', e.message); }
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
    // Subscription ended — block normal use, but let them reactivate or get help.
    if ((owner[0].fields['Status'] || '') === 'Cancelled' && !/^(billing|manage|resubscribe|reactivate|help)\b/i.test(smsBody.trim())) {
      const msg = 'Your FieldBrief subscription has ended. Reply BILLING to reactivate, or visit fieldbrief.ai to pick a plan. (Reply STOP to opt out of texts.)';
      logSMS(fromNumber, smsBody, 'cancelled', msg);
      return replyTwiML(res, msg);
    }
  } else {
    const tech = await airtableQuery(TABLES.TECHS, `AND({Phone} = "${fromNumber}", {Active} = 1)`);
    if (tech.length > 0) {
      accountPhone = tech[0].fields['Account Phone'] || '';
      actorName = tech[0].fields['Name'] || 'Tech';
    }
  }
  if (!accountPhone) {
    // KNOWN END-CUSTOMER, ORDINARY REPLY — YES/STOP were already handled above.
    // Anything else from a number we recognize as a shop's own customer (e.g.
    // "thanks!", "see you then", a question) must NOT fall into the demo/signup
    // pitch below — that's meant for total strangers, not someone's real customer.
    const knownCustomer = await airtableQuery(TABLES.CUSTOMERS, `{phone} = "${fromNumber}"`);
    if (knownCustomer.length > 0) {
      const acct = await getSubscriberSettings(knownCustomer[0].fields.subscriber_phone || '');
      const company = acct.company || 'us';
      const msg = `Thanks for the reply! For anything you need, please contact ${company} directly. Reply STOP to opt out of texts.`;
      logSMS(fromNumber, smsBody, 'customer_ack', msg);
      return replyTwiML(res, msg);
    }
    // DEMO SANDBOX — the DEMO intro tells prospects to "try texting a job", so
    // a job-shaped text from an unknown number IS the demo. Parse it for real
    // and show the built WO instead of the signup wall (which used to dead-loop
    // DEMO → "text a job" → "you're not set up" → DEMO). No Airtable writes.
    if (/\d\s*(?:hrs?|hours?)\b|\$\s*\d|install|replace|repair|tune|leak|service call/i.test(smsBody) && smsBody.trim().split(/\s+/).length >= 3) {
      try {
        const parsed = await parseJobLog(smsBody, 'Demo');
        if (parsed && parsed.work_order) {
          const DEMO_RATE = 185;
          const hours = parsed.work_order.labor_hours || 0;
          const partsTotal = (Array.isArray(parsed.parts) ? parsed.parts : [])
            .reduce((s, p) => s + ((Number(p.cost) || 0) * (Number(p.quantity) || 1)), 0);
          const total = hours * DEMO_RATE + partsTotal;
          const cust = parsed.customer?.name || 'your customer';
          const msg = `WO built — ${cust}${hours ? `, ${hours}hr labor @ $${DEMO_RATE}` : ''}${partsTotal ? ` + $${partsTotal.toFixed(0)} parts` : ''} = $${total.toFixed(2)}. On a real account that invoice just emailed to ${cust}, and I chase it if it goes unpaid. Reply DEMO and you're live — full access, free 15 days, no card. (Reply STOP to opt out.)`;
          logSMS(fromNumber, smsBody, 'demo_parse', msg);
          try { for (const a of ADMIN_PHONES) { if (a !== fromNumber) await sendSMS(a, `🔥 HOT lead: ${fromNumber} closed out a DEMO job ("${String(smsBody).slice(0, 80)}") and got the WO back. Call them.`); } } catch (e) { console.error('demo lead alert failed:', e.message); }
          return replyTwiML(res, msg);
        }
      } catch (e) { console.error('demo parse failed:', e.message); }
    }
    const signupPrompt = 'Hey! You\'re not set up yet. Reply DEMO to start your free 15-day trial — full access, no card needed.';
    logSMS(fromNumber, smsBody, 'signup_prompt', signupPrompt);
    return replyTwiML(res, signupPrompt);
  }

  try {
    let response = '', intent;
    // Brand-new owner mid-onboarding? The AI walks them through setup first.
    if (isOwner && owner.length) {
      const ostep = (owner[0].fields['Onboard Step'] || '').toLowerCase();
      if (ostep && ostep !== 'done' && upper !== 'HELP' && upper !== 'STOP') {
        const onbReply = await handleOnboardingReply(smsBody, owner[0]);
        if (onbReply) { logSMS(fromNumber, smsBody, 'onboarding', onbReply); return replyTwiML(res, onbReply); }
      }
    }
    // Crew reminders — deterministic fast paths first ("remind me..." plus
    // the REMINDERS list/cancel keywords); typo'd variants ("irmeind me")
    // still land here via the AI classifier's "remind" action below.
    const upperBare = upper.replace(/[.,!?]+$/, '');
    if (upperBare === 'REMINDERS') {
      const msg = await handleReminderList(fromNumber, accountPhone, isOwner);
      logSMS(fromNumber, smsBody, 'reminder_list', msg);
      return replyTwiML(res, msg);
    }
    if (/^(CANCEL|CLEAR)\s+REMINDERS$/.test(upperBare)) {
      const msg = await handleReminderCancel(fromNumber, actorName);
      logSMS(fromNumber, smsBody, 'reminder_cancel', msg);
      return replyTwiML(res, msg);
    }
    // Tech helper, explicit form: "ASK <any technical question>". Natural
    // questions without the keyword reach it via the classifier's "techhelp".
    const askMatch = smsBody.trim().match(/^ask[:,\s]+([\s\S]+)/i);
    if (askMatch) {
      const msg = await handleTechHelp(askMatch[1], fromNumber, actorName);
      logSMS(fromNumber, smsBody, 'techhelp', msg);
      return replyTwiML(res, msg);
    }
    // "TEACH HTP F13 = fan speed error" — crew-taught fault-code reference.
    if (/^teach\b/i.test(smsBody.trim())) {
      const msg = await handleTeach(smsBody, fromNumber, accountPhone, actorName);
      logSMS(fromNumber, smsBody, 'teach', msg);
      return replyTwiML(res, msg);
    }
    // Crew ops, explicit forms. "EOD: ..." logs the end-of-day one-liner;
    // "NEED ..." is a supply request. Natural phrasings ("we're out of 1in
    // copper", "can I get Friday off") arrive via the classifier below.
    const eodMatch = smsBody.trim().match(/^eod\b[:,\s-]*([\s\S]*)/i);
    if (eodMatch) {
      const msg = await handleEodReport(eodMatch[1], fromNumber, accountPhone, actorName);
      logSMS(fromNumber, smsBody, 'eod_report', msg);
      return replyTwiML(res, msg);
    }
    if (/^need\b/i.test(smsBody.trim())) {
      const msg = await handlePartsRequest(smsBody, fromNumber, accountPhone, actorName, isOwner);
      logSMS(fromNumber, smsBody, 'parts_request', msg);
      return replyTwiML(res, msg);
    }
    // Explicit keyword commands route deterministically — skip the AI classifier.
    const firstWord = upper.split(/\s+/)[0];
    const KEYWORDS = ['JOBS', 'PARTS', 'INVOICE', 'PROPOSAL', 'QUOTE', 'ESTIMATE', 'BRIEF', 'STATUS', 'SETTINGS', 'SET', 'UNDO', 'FIX', 'COMMANDS', 'TECHS', 'HISTORY', 'HELP', 'INFO', 'UNPAID', 'OUTSTANDING', 'PAID', 'RESEND', 'SCHEDULE', 'DISPATCH', 'APPROVE', 'SKIP', 'NOTE', 'ONBOARD', 'BILLING', 'MANAGE', 'RESUBSCRIBE', 'REACTIVATE', 'UPGRADE', 'PAY', 'SUBSCRIBE', 'RUNNUDGES', 'RUNFOLLOWUPS', 'RUNCHECKINS', 'REFER', 'REFERRAL', 'SHARE', 'PULSE'];
    const isCommand = KEYWORDS.includes(firstWord) || !!ES_COMMAND_ALIASES[firstWord.replace(/[.,!]$/, '')] || /^(ADD|REMOVE|TEXT)\s+TECH\b/i.test(smsBody.trim())
      || /^(cancel|end|stop)\s+(subscription|plan|billing|membership)/i.test(smsBody.trim())
      || /^cancel\s+my\s+(subscription|plan|account|billing)/i.test(smsBody.trim());
    // Reminder-shaped texts route to reminders — "remind me at 4pm..." from
    // anyone, or the owner aiming one at a tech ("remind Jaylen at 4pm...").
    // Never hijack an explicit command (e.g. TEXT TECH relaying a message
    // that merely MENTIONS reminders).
    if (!isCommand && (/^\s*remind\b/i.test(smsBody) || /\bremind\s*me\b/i.test(smsBody))) {
      const msg = await handleReminderCreate(smsBody, fromNumber, accountPhone, actorName, isOwner);
      logSMS(fromNumber, smsBody, 'reminder_create', msg);
      return replyTwiML(res, msg);
    }
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
      } else if (r.action === 'remind') {
        response = await handleReminderCreate(smsBody, fromNumber, accountPhone, actorName, isOwner);
      } else if (r.action === 'techhelp') {
        response = await handleTechHelp(r.command || smsBody, fromNumber, actorName);
      } else if (r.action === 'parts_request') {
        response = await handlePartsRequest(smsBody, fromNumber, accountPhone, actorName, isOwner);
      } else if (r.action === 'timeoff') {
        response = await handleTimeOffRequest(smsBody, fromNumber, accountPhone, actorName, isOwner);
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
    const cust = session.customer_details || {};
    const md = session.metadata || {};
    const phone = normalizePhone(cust.phone || md.phone || '');
    const name = cust.name || md.name || 'New Subscriber';
    const email = cust.email || '';
    const plan = md.plan || '';
    try {
      // No phone captured at checkout — can't key the SMS account. Alert the owner.
      if (!phone) {
        console.error('Stripe: no phone on checkout session', session.id);
        for (const a of ADMIN_PHONES) sendSMS(a, `💳 PAID FieldBrief signup but NO phone was captured (${name || '?'} / ${email || '?'}). Reach out and set them up manually.`);
        return res.json({ received: true });
      }
      const existing = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${phone}"`);
      if (existing.length) {
        // Already a subscriber (e.g. free trial converting) — activate + tag plan.
        const patch = { 'Status': 'Active', 'Subscription Plan': plan, 'Stripe Customer': session.customer || '' };
        if (email && !existing[0].fields['Contractor Email']) patch['Contractor Email'] = email;
        await airtableUpdate(TABLES.SUBSCRIBERS, existing[0].id, patch);
        sendSMS(phone, `Thanks for subscribing to FieldBrief${plan ? ' (' + plan + ')' : ''}! Your account is active. Reply HELP anytime.`);
      } else {
        // New paid signup — create + kick off the SAME AI-guided onboarding as free.
        await airtableCreate(TABLES.SUBSCRIBERS, {
          'Full Name': name, 'Phone Number': phone, 'Status': 'Active', 'Subscription Plan': plan,
          'Contractor Email': email, 'Stripe Customer': session.customer || '',
          'Onboard Step': 'company', 'Signed Up': localDate(), 'Referred By': session.client_reference_id || '',
        });
        sendSMS(phone, `Welcome to FieldBrief, ${String(name).split(' ')[0]}! 👋 I'll get you set up in 30 seconds — all by text. First: what's your company name? (Reply STOP to opt out.)`);
      }
      for (const a of ADMIN_PHONES) { if (a !== phone) sendSMS(a, `💳 New PAID FieldBrief signup: ${name} (${phone})${plan ? ' — ' + plan : ''}.`); }
      if (session.client_reference_id) { try { await creditReferrer(session.client_reference_id); } catch (e) { console.error('stripe referral error:', e.message); } }
      res.json({ received: true });
    } catch (error) {
      console.error('Stripe onboarding error:', error);
      res.status(500).json({ error: 'Failed to create subscriber' });
    }
  } else if (event.type === 'invoice.payment_failed') {
    // Card declined — keep access during dunning, but warn them + the owner.
    try {
      const inv = event.data.object;
      const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Stripe Customer} = "${inv.customer}"`);
      if (subs.length) {
        const ph = subs[0].fields['Phone Number'];
        await airtableUpdate(TABLES.SUBSCRIBERS, subs[0].id, { 'Status': 'Past Due' });
        if (ph) sendSMS(ph, `Heads up — your FieldBrief payment didn't go through. Update your card to keep your account active: reply BILLING for a secure link.`);
        for (const a of ADMIN_PHONES) { if (a !== ph) sendSMS(a, `💳⚠️ Payment FAILED for ${subs[0].fields['Full Name'] || ph}. They were asked to update their card.`); }
      }
    } catch (e) { console.error('payment_failed handler error:', e.message); }
    res.json({ received: true });
  } else if (event.type === 'customer.subscription.deleted') {
    // Subscription fully ended (canceled or final dunning failure) — deactivate.
    try {
      const sub = event.data.object;
      const subs = await airtableQuery(TABLES.SUBSCRIBERS, `{Stripe Customer} = "${sub.customer}"`);
      if (subs.length) {
        const ph = subs[0].fields['Phone Number'];
        await airtableUpdate(TABLES.SUBSCRIBERS, subs[0].id, { 'Status': 'Cancelled' });
        if (ph) sendSMS(ph, `Your FieldBrief subscription has ended — you won't be charged again. Thanks for trying it! Resubscribe anytime at fieldbrief.ai.`);
        for (const a of ADMIN_PHONES) { if (a !== ph) sendSMS(a, `🚫 FieldBrief subscription ENDED for ${subs[0].fields['Full Name'] || ph}.`); }
      }
    } catch (e) { console.error('subscription.deleted handler error:', e.message); }
    res.json({ received: true });
  } else {
    res.json({ received: true });
  }
});

// ============================================================================
// CRON: MORNING BRIEF AT 6 AM PACIFIC
// All subscriber-facing crons run Pacific — the current customer base is West
// Coast, and ET crons were landing the "morning" brief at 3 AM PT.
// ============================================================================
cron.schedule('0 6 * * *', async () => {
  console.log('Running morning brief...');
  try {
    const subscribers = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);
    for (const sub of subscribers) {
      const phone = sub.fields['Phone Number'];
      if (!phone) continue;
      const brief = await buildDailyBrief(phone);
      sendSMS(phone, brief);
      console.log(`Brief sent to ${phone}`);
    }
  } catch (error) { console.error('Morning brief error:', error); }
}, { timezone: 'America/Los_Angeles' });

// Crew reminders: dispatch anything due, every minute.
cron.schedule('* * * * *', dispatchDueReminders, { timezone: 'America/Los_Angeles' });

// Owner morning digest (crew EOD lines + open parts requests + pending time
// off), weekdays at 6:30 AM PT — before the day starts, after the 6 AM brief.
cron.schedule('30 6 * * 1-5', async () => {
  console.log('Running owner digests...');
  await sendOwnerDigest();
}, { timezone: 'America/Los_Angeles' });

// End-of-day "log your calls" nudge for opted-in accounts (LOG_NUDGE_ACCOUNTS),
// weekdays at 4:30 PM PT.
cron.schedule('30 16 * * 1-5', async () => {
  console.log('Running log nudges...');
  await sendLogNudges();
}, { timezone: 'America/Los_Angeles' });

// Daily trial nudges at 10 AM PT (day-2 engagement + day-12 trial-ending).
cron.schedule('0 10 * * *', async () => {
  console.log('Running trial nudges...');
  const r = await runTrialNudges();
  console.log(`Trial nudges sent: ${r.n1} engagement, ${r.n2} ending-soon`);
}, { timezone: 'America/Los_Angeles' });

// Weekly owner pulse — Mondays 8 AM PT.
cron.schedule('0 8 * * 1', async () => { console.log('Owner pulse...'); await ownerPulse(); }, { timezone: 'America/Los_Angeles' });

// Lead follow-up drafts at 11 AM PT (after trial nudges) — drafts only, owner APPROVEs.
cron.schedule('0 11 * * *', async () => {
  console.log('Running lead follow-ups...');
  const r = await runLeadFollowups();
  console.log(`Lead follow-ups: ${r.drafted} drafted, ${r.notified} owner(s) notified`);
}, { timezone: 'America/Los_Angeles' });

// Maintenance check-in drafts — weekly, Tuesdays 9 AM PT (low volume, doesn't need daily).
cron.schedule('0 9 * * 2', async () => {
  console.log('Running maintenance check-ins...');
  const r = await runMaintenanceCheckins();
  console.log(`Maintenance check-ins: ${r.drafted} drafted, ${r.notified} owner(s) notified`);
}, { timezone: 'America/Los_Angeles' });

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
