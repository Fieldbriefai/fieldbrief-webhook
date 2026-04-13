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
    const jsonMatch = responseText.match(/\`\`\`json\n?([\s\S]*?)\n?\`\`\`/) || responseText.match(/({[\s\S]*})/);
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
// JOB LOG HANDLER (uses actual Airtable field names)
// ============================================================================

async function handleJobLog(smsBody, subscriberPhone, subscriberName) {
  const parsedData = await parseJobLog(smsBody, subscriberName);
  if (!parsedData) {
    sendSMS(subscriberPhone, 'Got your text but had trouble parsing it. I\'ll flag this for review.');
    return 'Parse failed';
  }

  try {
    // Create or find customer
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

    // Create equipment if mentioned
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

    // Create work order
    let woId = null;
    if (parsedData.work_order) {
      woId = await airtableCreate(TABLES.WORK_ORDERS, {
        wo_label: `${customerName} - ${new Date().toISOString().split('T')[0]}`,
        job_type: parsedData.work_order.job_type || 'Service',
        description: parsedData.work_order.description || '',
        labor_hours: parsedData.work_order.labor_hours || 0,
        status: 'Completed',
        date: new Date().toISOString().split('T')[0],
        customer_name: customerName,
        equipment_label: equipmentLabel,
        subscriber_phone: subscriberPhone,
        raw_sms: smsBody,
      });
    }

    // Create parts
    if (parsedData.parts && Array.isArray(parsedData.parts)) {
      for (const part of parsedData.parts) {
        if (part.supplier) {
          const existingSuppliers = await airtableQuery(TABLES.SUPPLIERS, `{supplier_name} = "${part.supplier}"`);
          if (existingSuppliers.length === 0) {
            await airtableCreate(TABLES.SUPPLIERS, { supplier_name: part.supplier });
          }
        }
        await airtableCreate(TABLES.PARTS_USED, {
          part_name: part.name || '',
          supplier_name: part.supplier || '',
          cost: part.cost || 0,
          quantity: part.quantity || 1,
          category: part.category || '',
          wo_label: `${customerName} - ${new Date().toISOString().split('T')[0]}`,
          subscriber_phone: subscriberPhone,
          date: new Date().toISOString().split('T')[0],
        });
      }
    }

    const confirmation = `✓ Logged: ${parsedData.work_order?.description || 'Work'} for ${customerName}. ${parsedData.parts?.length || 0} parts. $${(parsedData.parts || []).reduce((sum, p) => sum + (p.cost || 0), 0).toFixed(2)}`;
    sendSMS(subscriberPhone, confirmation);
    return confirmation;
  } catch (error) {
    console.error('Job log error:', error);
    sendSMS(subscriberPhone, 'Error logging your job. Please try again.');
    return 'Error';
  }
}

// ============================================================================
// COMMAND HANDLERS
// ============================================================================

async function handleCommand(command, subscriberPhone, subscriberName) {
  const cmd = command.toUpperCase().trim();

  if (cmd === 'HELP') {
    const msg = 'Commands: JOBS (today), PARTS (used), INVOICE [customer], BRIEF (on-demand), STATUS (account), HELP';
    sendSMS(subscriberPhone, msg); return msg;
  }

  if (cmd === 'JOBS') {
    const today = new Date().toISOString().split('T')[0];
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", {date} = "${today}")`);
    if (jobs.length === 0) { sendSMS(subscriberPhone, 'No jobs logged today yet.'); return 'No jobs today'; }
    const jobList = jobs.slice(0, 3).map(j =>
      `• ${j.fields.customer_name}: ${j.fields.job_type} (${j.fields.labor_hours || 0}h)`).join('\n');
    const msg = `Today's jobs:\n${jobList}${jobs.length > 3 ? `\n+${jobs.length - 3} more` : ''}`;
    sendSMS(subscriberPhone, msg); return msg;
  }

  if (cmd === 'PARTS') {
    const today = new Date().toISOString().split('T')[0];
    const parts = await airtableQuery(TABLES.PARTS_USED,
      `AND({subscriber_phone} = "${subscriberPhone}", {date} = "${today}")`);
    if (parts.length === 0) { sendSMS(subscriberPhone, 'No parts logged today.'); return 'No parts'; }
    const partList = parts.slice(0, 5).map(p =>
      `• ${p.fields.part_name} x${p.fields.quantity || 1} ($${(p.fields.cost || 0).toFixed(2)})`).join('\n');
    const total = parts.reduce((sum, p) => sum + (p.fields.cost || 0), 0);
    const msg = `Today's parts:\n${partList}\nTotal: $${total.toFixed(2)}`;
    sendSMS(subscriberPhone, msg); return msg;
  }

  if (cmd.startsWith('INVOICE')) {
    const customerName = cmd.replace('INVOICE', '').trim();
    if (!customerName) { sendSMS(subscriberPhone, 'Usage: INVOICE [customer name]'); return 'Usage'; }
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", {customer_name} = "${customerName}")`);
    if (jobs.length === 0) { sendSMS(subscriberPhone, `No jobs found for ${customerName}.`); return 'No jobs'; }
    let totalLabor = 0, totalParts = 0;
    for (const job of jobs) {
      totalLabor += job.fields.labor_hours || 0;
      totalParts += job.fields.total_parts_cost || 0;
    }
    const msg = `Invoice for ${customerName}: ${jobs.length} job(s), ${totalLabor}h labor, $${totalParts.toFixed(2)} parts`;
    sendSMS(subscriberPhone, msg); return msg;
  }

  if (cmd === 'BRIEF') {
    const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
    const jobs = await airtableQuery(TABLES.WORK_ORDERS,
      `AND({subscriber_phone} = "${subscriberPhone}", {date} = "${yesterday.toISOString().split('T')[0]}")`);
    const brief = await generateMorningBrief(jobs);
    sendSMS(subscriberPhone, brief); return brief;
  }

  if (cmd === 'STATUS') {
    const sub = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${subscriberPhone}"`);
    if (sub.length === 0) { sendSMS(subscriberPhone, 'Account not found.'); return 'Not found'; }
    const s = sub[0].fields;
    const msg = `Plan: ${s.Plan || 'Standard'} | Status: ${s.Status || 'Active'} | ${s['Company Name'] || ''}`;
    sendSMS(subscriberPhone, msg); return msg;
  }

  sendSMS(subscriberPhone, 'Unknown command. Reply HELP for available commands.');
  return 'Unknown command';
}

// ============================================================================
// SUPPORT TICKET & SMS LOG
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
      ticket_label: `${ticketSubtype} - ${subscriberName} - ${new Date().toISOString().split('T')[0]}`,
      type: ticketSubtype,
      status: ticketStatus,
      subscriber_phone: subscriberPhone,
      subscriber_name: subscriberName,
      description: smsBody,
      ai_response: aiResponse,
      created_date: new Date().toISOString().split('T')[0],
    });

    sendSMS(subscriberPhone, aiResponse);
    return aiResponse;
  } catch (error) {
    console.error('Support ticket error:', error);
    const fallback = 'Thanks for reaching out. We\'ll review this soon.';
    sendSMS(subscriberPhone, fallback); return fallback;
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

app.post('/sms', async (req, res) => {
  const fromNumber = req.body.From || '';
  const smsBody = req.body.Body || '';
  console.log(`SMS from ${fromNumber}: ${smsBody}`);

  // Look up subscriber by phone (field name: "Phone Number")
  const subscribers = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone Number} = "${fromNumber}"`);

  if (subscribers.length === 0) {
    const signupPrompt = 'Hey! You\'re not signed up yet. Visit fieldbrief.ai to get started. Reply DEMO for a free trial.';
    sendSMS(fromNumber, signupPrompt);
    logSMS(fromNumber, smsBody, 'signup_prompt', signupPrompt);
    res.type('text/xml').send(createTwiMLResponse(signupPrompt));
    return;
  }

  const subscriber = subscribers[0];
  const subscriberName = subscriber.fields['Full Name'] || 'Contractor';

  try {
    const intent = await classifyIntent(smsBody);
    console.log(`Intent: ${intent}`);
    let response = '';

    if (intent === 'job_log') {
      response = await handleJobLog(smsBody, fromNumber, subscriberName);
    } else if (intent === 'command') {
      const commandMatch = smsBody.match(/^(JOBS|PARTS|INVOICE|BRIEF|HELP|STATUS)(?:\s+(.*))?$/i);
      response = await handleCommand(commandMatch ? commandMatch[0] : smsBody, fromNumber, subscriberName);
    } else if (intent === 'support') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberName, 'support');
    } else if (intent === 'feature_request') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberName, 'feature_request');
    } else if (intent === 'cancel') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberName, 'cancel');
    } else if (intent === 'billing') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberName, 'billing');
    } else {
      response = 'Thanks for the message. How can I help? Reply HELP for commands.';
      sendSMS(fromNumber, response);
    }

    logSMS(fromNumber, smsBody, intent, response);
    res.type('text/xml').send(createTwiMLResponse(response || 'Message received.'));
  } catch (error) {
    console.error('SMS processing error:', error);
    const errorResponse = 'Sorry, I encountered an error. Please try again.';
    sendSMS(fromNumber, errorResponse);
    logSMS(fromNumber, smsBody, 'error', errorResponse);
    res.type('text/xml').send(createTwiMLResponse(errorResponse));
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
        'Join Date': new Date().toISOString().split('T')[0],
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
      const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
      const jobs = await airtableQuery(TABLES.WORK_ORDERS,
        `AND({subscriber_phone} = "${phone}", {date} = "${yesterday.toISOString().split('T')[0]}")`);
      const brief = await generateMorningBrief(jobs);
      sendSMS(phone, brief);
      console.log(`Brief sent to ${phone}`);
    }
  } catch (error) { console.error('Morning brief error:', error); }
}, { timezone: 'America/New_York' });

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, () => {
  console.log(`FieldBrief webhook running on port ${PORT}`);
});
