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

// Environment variables
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || 'patm1fGCuyaDhi5RC.0ab7a30ee2453980d68154847713f309a8eb310764f48840e66af79cd9c2cb06';
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appbcR8hJtuXwpEI8';
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER || '+18559835461';
const PORT = process.env.PORT || 3000;

// Airtable table IDs
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
// AIRTABLE HELPER FUNCTIONS
// ============================================================================

/**
 * Make authenticated request to Airtable REST API
 */
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

  if (data) {
    options.body = JSON.stringify(data);
  }

  try {
    const response = await fetch(endpoint, options);
    if (!response.ok) {
      const error = await response.text();
      console.error(`Airtable error (${method} ${tableId}):`, error);
      return null;
    }
    return await response.json();
  } catch (error) {
    console.error(`Airtable request error:`, error);
    return null;
  }
}

/**
 * Query Airtable with filter formula
 */
async function airtableQuery(tableId, filterFormula) {
  const endpoint = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}?filterByFormula=${encodeURIComponent(filterFormula)}`;

  try {
    const response = await fetch(endpoint, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${AIRTABLE_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      console.error('Airtable query error:', await response.text());
      return [];
    }

    const data = await response.json();
    return data.records || [];
  } catch (error) {
    console.error('Airtable query request error:', error);
    return [];
  }
}

/**
 * Create a record in Airtable
 */
async function airtableCreate(tableId, fields) {
  const result = await airtableRequest('POST', tableId, { fields });
  return result?.id || null;
}

/**
 * Update a record in Airtable
 */
async function airtableUpdate(tableId, recordId, fields) {
  const result = await airtableRequest('PATCH', tableId, { fields }, recordId);
  return result?.id || null;
}

// ============================================================================
// SMS HELPER FUNCTIONS
// ============================================================================

/**
 * Send SMS via Twilio
 */
async function sendSMS(toNumber, message) {
  try {
    const response = await twilioClient.messages.create({
      body: message,
      from: TWILIO_PHONE_NUMBER,
      to: toNumber,
    });
    console.log(`SMS sent to ${toNumber}:`, response.sid);
    return response.sid;
  } catch (error) {
    console.error(`Failed to send SMS to ${toNumber}:`, error);
    return null;
  }
}

/**
 * Format Twilio TwiML response
 */
function createTwiMLResponse(message) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Message>${escapeXML(message)}</Message>
</Response>`;
}

/**
 * Escape XML special characters
 */
function escapeXML(str) {
  if (!str) return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

// ============================================================================
// CLAUDE INTENT CLASSIFICATION & PARSING
// ============================================================================

/**
 * Classify SMS intent using Claude
 */
async function classifyIntent(smsBody) {
  try {
    const message = await anthropic.messages.create({
      model: 'claude-3-5-sonnet-20241022',
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
      messages: [
        {
          role: 'user',
          content: smsBody,
        },
      ],
    });

    const intent = message.content[0].type === 'text' ? message.content[0].text.trim().toLowerCase() : 'general';
    return intent;
  } catch (error) {
    console.error('Claude classification error:', error);
    return 'general';
  }
}

/**
 * Parse job log SMS using Claude
 */
async function parseJobLog(smsBody, subscriberName) {
  try {
    const message = await anthropic.messages.create({
      model: 'claude-3-5-sonnet-20241022',
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

Common trades abbreviations:
- WM = Weil-McLain
- circ = circulator pump
- EWT = electric water tank
- ASHP = air-source heat pump
- RTU = rooftop unit
- VAV = variable air volume
- AHU = air handling unit
- UMC = unit mounted controller

Handle incomplete info gracefully. If the text mentions "threw in a new circ pump", that's a part installation.
Multiple jobs in one text are OK - include all. If values are missing, omit them. If you see dollar amounts, include them in parts.cost.

Respond with ONLY valid JSON, no explanation.`,
      messages: [
        {
          role: 'user',
          content: smsBody,
        },
      ],
    });

    let parsed = {};
    try {
      const responseText = message.content[0].type === 'text' ? message.content[0].text : '{}';
      // Extract JSON if it's wrapped in code blocks
      const jsonMatch = responseText.match(/\`\`\`json\n?([\s\S]*?)\n?\`\`\`/) || responseText.match(/({[\s\S]*})/);
      parsed = JSON.parse(jsonMatch ? jsonMatch[1] : responseText);
    } catch (parseError) {
      console.error('JSON parse error in parseJobLog:', parseError);
      return null;
    }

    return parsed;
  } catch (error) {
    console.error('Claude parse error:', error);
    return null;
  }
}

/**
 * Generate AI response for support/feature/billing questions
 */
async function generateAIResponse(smsBody, ticketType) {
  try {
    const systemPrompt = {
      support: `You are a friendly field service support assistant. Respond helpfully to the contractor's issue or question. Keep response to 1-2 sentences, max 160 characters so it fits in an SMS.`,
      feature_request: `You are a product feedback assistant. Thank the contractor for their feature suggestion and let them know it will be reviewed. Keep to 1-2 sentences, max 160 characters.`,
      billing: `You are a billing support assistant. Address the contractor's billing question helpfully. If you can't fully resolve it, offer to escalate. Keep to 1-2 sentences, max 160 characters.`,
      cancel: `You are a cancellation support specialist. Acknowledge their request, express that we're sorry to see them go, and let them know we'll process it. Keep to 1-2 sentences, max 160 characters.`,
    };

    const message = await anthropic.messages.create({
      model: 'claude-3-5-sonnet-20241022',
      max_tokens: 100,
      system: systemPrompt[ticketType] || systemPrompt.support,
      messages: [
        {
          role: 'user',
          content: smsBody,
        },
      ],
    });

    return message.content[0].type === 'text' ? message.content[0].text.trim() : 'Thanks for reaching out. We\'ll look into this.';
  } catch (error) {
    console.error('Claude response generation error:', error);
    return 'Thanks for reaching out. We\'ll review this and get back to you.';
  }
}

/**
 * Generate morning brief using Claude
 */
async function generateMorningBrief(yesterdayJobs) {
  try {
    const jobSummary = yesterdayJobs
      .map(
        (job) =>
          `- ${job.fields['Customer Name'] || 'Unknown'}: ${job.fields['Job Type'] || 'Service'} (${job.fields['Labor Hours'] || 0}h)`
      )
      .join('\n');

    const message = await anthropic.messages.create({
      model: 'claude-3-5-sonnet-20241022',
      max_tokens: 200,
      system: `You are a brief generator for field service contractors. Create a short, motivational morning summary of yesterday's work.
Keep it to 2-3 sentences, friendly tone, acknowledge their productivity.`,
      messages: [
        {
          role: 'user',
          content: `Yesterday's jobs:\n${jobSummary || 'No jobs logged'}`,
        },
      ],
    });

    return message.content[0].type === 'text' ? message.content[0].text.trim() : 'Good morning! Keep up the great work today.';
  } catch (error) {
    console.error('Morning brief generation error:', error);
    return 'Good morning! Have a productive day ahead.';
  }
}

// ============================================================================
// JOB LOG HANDLER
// ============================================================================

async function handleJobLog(smsBody, subscriberPhone, subscriberName, subscriberId) {
  let parsedData = null;

  try {
    parsedData = await parseJobLog(smsBody, subscriberName);
  } catch (error) {
    console.error('Job log parsing failed:', error);
  }

  if (!parsedData) {
    const response = 'Got your text but had trouble parsing it. I\'ll flag this for review.';
    sendSMS(subscriberPhone, response);
    return response;
  }

  let customerId = null;
  let equipmentId = null;
  let workOrderId = null;

  try {
    // Handle customer
    if (parsedData.customer && parsedData.customer.name) {
      const customerName = parsedData.customer.name.trim();
      const existingCustomers = await airtableQuery(
        TABLES.CUSTOMERS,
        `AND({Name} = "${customerName}", {Subscriber ID} = "${subscriberId}")`
      );

      if (existingCustomers.length > 0) {
        customerId = existingCustomers[0].id;
      } else {
        customerId = await airtableCreate(TABLES.CUSTOMERS, {
          Name: customerName,
          Address: parsedData.customer.address || '',
          City: parsedData.customer.city || '',
          State: parsedData.customer.state || '',
          'Subscriber ID': subscriberId,
        });
      }
    }

    // Handle equipment
    if (parsedData.equipment && (parsedData.equipment.model || parsedData.equipment.serial_number)) {
      const filterParts = [];
      if (parsedData.equipment.serial_number) {
        filterParts.push(`{Serial Number} = "${parsedData.equipment.serial_number}"`);
      }
      if (parsedData.equipment.manufacturer && parsedData.equipment.model) {
        filterParts.push(
          `AND({Manufacturer} = "${parsedData.equipment.manufacturer}", {Model} = "${parsedData.equipment.model}")`
        );
      }

      const filterFormula = filterParts.length > 0 ? `OR(${filterParts.join(', ')})` : null;
      const existingEquipment = filterFormula ? await airtableQuery(TABLES.EQUIPMENT, filterFormula) : [];

      if (existingEquipment.length > 0) {
        equipmentId = existingEquipment[0].id;
      } else {
        equipmentId = await airtableCreate(TABLES.EQUIPMENT, {
          Category: parsedData.equipment.category || '',
          Manufacturer: parsedData.equipment.manufacturer || '',
          Model: parsedData.equipment.model || '',
          'Serial Number': parsedData.equipment.serial_number || '',
          'Fuel Type': parsedData.equipment.fuel_type || '',
          'Customer ID': customerId || '',
        });
      }
    }

    // Handle work order
    if (parsedData.work_order) {
      workOrderId = await airtableCreate(TABLES.WORK_ORDERS, {
        'Customer Name': parsedData.customer?.name || 'Unknown',
        'Job Type': parsedData.work_order.job_type || 'Service',
        Description: parsedData.work_order.description || '',
        'Labor Hours': parsedData.work_order.labor_hours || 0,
        Status: 'Completed',
        Date: new Date().toISOString().split('T')[0],
        'Subscriber ID': subscriberId,
        'Customer ID': customerId || '',
        'Equipment ID': equipmentId || '',
      });
    }

    // Handle parts used
    if (parsedData.parts && Array.isArray(parsedData.parts)) {
      for (const part of parsedData.parts) {
        // Create supplier if needed
        let supplierId = null;
        if (part.supplier) {
          const existingSuppliers = await airtableQuery(TABLES.SUPPLIERS, `{Name} = "${part.supplier}"`);
          if (existingSuppliers.length > 0) {
            supplierId = existingSuppliers[0].id;
          } else {
            supplierId = await airtableCreate(TABLES.SUPPLIERS, { Name: part.supplier });
          }
        }

        // Create parts used record
        await airtableCreate(TABLES.PARTS_USED, {
          'Part Name': part.name || '',
          Supplier: supplierId ? [supplierId] : [],
          Cost: part.cost || 0,
          Quantity: part.quantity || 1,
          Category: part.category || '',
          'Work Order ID': workOrderId || '',
          'Subscriber ID': subscriberId,
        });
      }
    }

    // Send confirmation SMS
    const confirmation = `✓ Logged: ${parsedData.work_order?.description || 'Work'} for ${parsedData.customer?.name || 'Customer'}. ${parsedData.parts?.length || 0} parts. $${(parsedData.parts || []).reduce((sum, p) => sum + (p.cost || 0), 0).toFixed(2)}`;
    sendSMS(subscriberPhone, confirmation);
    return confirmation;
  } catch (error) {
    console.error('Job log processing error:', error);
    const errorMsg = 'There was an error logging your job. Please try again or contact support.';
    sendSMS(subscriberPhone, errorMsg);
    return errorMsg;
  }
}

// ============================================================================
// COMMAND HANDLERS
// ============================================================================

async function handleCommand(command, subscriberPhone, subscriberId, subscriberName) {
  const cmd = command.toUpperCase().trim();

  if (cmd === 'HELP') {
    const helpText = `Commands: JOBS (today's), PARTS (used), INVOICE [customer], BRIEF (on-demand), STATUS (account), HELP`;
    sendSMS(subscriberPhone, helpText);
    return helpText;
  }

  if (cmd === 'JOBS') {
    const today = new Date().toISOString().split('T')[0];
    const jobs = await airtableQuery(
      TABLES.WORK_ORDERS,
      `AND({Subscriber ID} = "${subscriberId}", {Date} = "${today}")`
    );

    if (jobs.length === 0) {
      sendSMS(subscriberPhone, 'No jobs logged today yet.');
      return 'No jobs logged today yet.';
    }

    const jobList = jobs
      .map((j) => `• ${j.fields['Customer Name']}: ${j.fields['Job Type']} (${j.fields['Labor Hours'] || 0}h)`)
      .slice(0, 3)
      .join('\n');
    const msg = `Today's jobs:\n${jobList}${jobs.length > 3 ? `\n+${jobs.length - 3} more` : ''}`;
    sendSMS(subscriberPhone, msg);
    return msg;
  }

  if (cmd === 'PARTS') {
    const today = new Date().toISOString().split('T')[0];
    const jobs = await airtableQuery(
      TABLES.WORK_ORDERS,
      `AND({Subscriber ID} = "${subscriberId}", {Date} = "${today}")`
    );
    const jobIds = jobs.map((j) => j.id);

    let parts = [];
    for (const jobId of jobIds) {
      const jobParts = await airtableQuery(
        TABLES.PARTS_USED,
        `{Work Order ID} = "${jobId}"`
      );
      parts = parts.concat(jobParts);
    }

    if (parts.length === 0) {
      sendSMS(subscriberPhone, 'No parts logged today.');
      return 'No parts logged today.';
    }

    const partList = parts
      .map((p) => `• ${p.fields['Part Name']} x${p.fields.Quantity || 1} ($${(p.fields.Cost || 0).toFixed(2)})`)
      .slice(0, 5)
      .join('\n');
    const total = parts.reduce((sum, p) => sum + (p.fields.Cost || 0), 0);
    const msg = `Today's parts:\n${partList}\nTotal: $${total.toFixed(2)}`;
    sendSMS(subscriberPhone, msg);
    return msg;
  }

  if (cmd.startsWith('INVOICE')) {
    const customerName = cmd.replace('INVOICE', '').trim();
    if (!customerName) {
      sendSMS(subscriberPhone, 'Usage: INVOICE [customer name]');
      return 'Usage: INVOICE [customer name]';
    }

    const jobs = await airtableQuery(
      TABLES.WORK_ORDERS,
      `AND({Subscriber ID} = "${subscriberId}", {Customer Name} = "${customerName}")`
    );

    if (jobs.length === 0) {
      sendSMS(subscriberPhone, `No jobs found for ${customerName}.`);
      return `No jobs found for ${customerName}.`;
    }

    let totalLabor = 0;
    let totalParts = 0;
    for (const job of jobs) {
      totalLabor += job.fields['Labor Hours'] || 0;
      const parts = await airtableQuery(TABLES.PARTS_USED, `{Work Order ID} = "${job.id}"`);
      totalParts += parts.reduce((sum, p) => sum + (p.fields.Cost || 0), 0);
    }

    const msg = `Invoice for ${customerName}: ${jobs.length} job(s), ${totalLabor}h labor, $${totalParts.toFixed(2)} parts`;
    sendSMS(subscriberPhone, msg);
    return msg;
  }

  if (cmd === 'BRIEF') {
    // Send on-demand brief (same as morning brief)
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];

    const jobs = await airtableQuery(
      TABLES.WORK_ORDERS,
      `AND({Subscriber ID} = "${subscriberId}", {Date} = "${yesterdayStr}")`
    );

    const brief = await generateMorningBrief(jobs);
    sendSMS(subscriberPhone, brief);
    return brief;
  }

  if (cmd === 'STATUS') {
    const subscriber = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone} = "${subscriberPhone}"`);
    if (subscriber.length === 0) {
      sendSMS(subscriberPhone, 'Account not found.');
      return 'Account not found.';
    }

    const sub = subscriber[0].fields;
    const plan = sub.Plan || 'Unknown';
    const status = sub.Status || 'Active';
    const msg = `Plan: ${plan} | Status: ${status} | Email: ${sub.Email || 'N/A'}`;
    sendSMS(subscriberPhone, msg);
    return msg;
  }

  const defaultMsg = 'Unknown command. Reply HELP for available commands.';
  sendSMS(subscriberPhone, defaultMsg);
  return defaultMsg;
}

// ============================================================================
// SUPPORT/TICKET HANDLER
// ============================================================================

async function handleSupportTicket(smsBody, subscriberPhone, subscriberId, ticketType) {
  try {
    // Generate AI response
    const aiResponse = await generateAIResponse(smsBody, ticketType);

    // Determine ticket status and priority
    let ticketStatus = 'Open';
    let ticketSubtype = 'Support Request';

    if (ticketType === 'cancel') {
      ticketStatus = 'Escalated to JJ';
      ticketSubtype = 'Cancellation';
    } else if (ticketType === 'support') {
      // Simple issues might be AI resolved, complex ones escalated
      ticketStatus = aiResponse.length < 100 ? 'AI Resolved' : 'Escalated to JJ';
    } else if (ticketType === 'billing') {
      ticketStatus = 'Escalated to JJ';
      ticketSubtype = 'Billing Question';
    } else if (ticketType === 'feature_request') {
      ticketStatus = 'Open';
      ticketSubtype = 'Feature Request';
    }

    // Create support ticket record
    await airtableCreate(TABLES.SUPPORT_TICKETS, {
      'Subscriber ID': subscriberId,
      Phone: subscriberPhone,
      Message: smsBody,
      Type: ticketSubtype,
      Status: ticketStatus,
      'AI Response': aiResponse,
      'Created At': new Date().toISOString(),
    });

    // Send response SMS
    sendSMS(subscriberPhone, aiResponse);
    return aiResponse;
  } catch (error) {
    console.error('Support ticket error:', error);
    const fallback = 'Thanks for reaching out. We\'ll review this and get back to you soon.';
    sendSMS(subscriberPhone, fallback);
    return fallback;
  }
}

// ============================================================================
// SMS LOG
// ============================================================================

async function logSMS(fromNumber, body, intent, response) {
  try {
    await airtableCreate(TABLES.SMS_LOG, {
      'From Number': fromNumber,
      'Message Body': body,
      Intent: intent,
      Response: response,
      Timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('SMS logging error:', error);
  }
}

// ============================================================================
// HTTP ROUTES
// ============================================================================

/**
 * Health check route
 */
app.get('/', (req, res) => {
  res.status(200).send('FieldBrief webhook is running');
});

/**
 * Twilio inbound SMS webhook
 */
app.post('/sms', async (req, res) => {
  const fromNumber = req.body.From || '';
  const smsBody = req.body.Body || '';

  console.log(`Received SMS from ${fromNumber}: ${smsBody}`);

  // Look up subscriber by phone
  const subscribers = await airtableQuery(TABLES.SUBSCRIBERS, `{Phone} = "${fromNumber}"`);

  if (subscribers.length === 0) {
    // Not a subscriber
    const signupPrompt = 'Hey! Looks like you\'re not signed up yet. Visit fieldbrief.ai to get started. Reply DEMO for a free trial.';
    sendSMS(fromNumber, signupPrompt);
    logSMS(fromNumber, smsBody, 'signup_prompt', signupPrompt);
    res.type('text/xml').send(createTwiMLResponse(signupPrompt));
    return;
  }

  const subscriber = subscribers[0];
  const subscriberId = subscriber.id;
  const subscriberName = subscriber.fields.Name || 'Contractor';

  try {
    // Classify intent
    const intent = await classifyIntent(smsBody);
    console.log(`Intent: ${intent}`);

    let response = '';

    if (intent === 'job_log') {
      response = await handleJobLog(smsBody, fromNumber, subscriberName, subscriberId);
    } else if (intent === 'command') {
      // Extract command from text
      const commandMatch = smsBody.match(/^(JOBS|PARTS|INVOICE|BRIEF|HELP|STATUS)(?:\s+(.*))?$/i);
      const command = commandMatch ? commandMatch[1] : smsBody;
      response = await handleCommand(command, fromNumber, subscriberId, subscriberName);
    } else if (intent === 'support') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberId, 'support');
    } else if (intent === 'feature_request') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberId, 'feature_request');
    } else if (intent === 'cancel') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberId, 'cancel');
    } else if (intent === 'billing') {
      response = await handleSupportTicket(smsBody, fromNumber, subscriberId, 'billing');
    } else {
      // General conversation
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

/**
 * Stripe webhook handler
 */
app.post('/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET || ''
    );
  } catch (error) {
    console.error('Stripe signature verification error:', error);
    res.status(400).send(`Webhook Error: ${error.message}`);
    return;
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const metadata = session.metadata || {};

    try {
      // Extract from metadata
      const email = metadata.email || session.customer_email || '';
      const phone = metadata.phone || '';
      const plan = metadata.plan || 'Basic';
      const name = metadata.name || 'New Subscriber';

      // Create subscriber in Airtable
      const subscriberId = await airtableCreate(TABLES.SUBSCRIBERS, {
        Name: name,
        Email: email,
        Phone: phone,
        Plan: plan,
        Status: 'Active',
        'Signup Date': new Date().toISOString().split('T')[0],
        'Stripe Customer ID': session.customer || '',
      });

      // Send welcome SMS
      if (phone) {
        const welcome = `Welcome to FieldBrief! You're all set on the ${plan} plan. Reply HELP for available commands.`;
        sendSMS(phone, welcome);
      }

      console.log(`New subscriber created: ${subscriberId}`);
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
// CRON JOBS
// ============================================================================

/**
 * Morning brief cron job: 6 AM ET daily
 */
cron.schedule('0 6 * * *', async () => {
  console.log('Running morning brief cron job...');

  try {
    // Get all active subscribers
    const subscribers = await airtableQuery(TABLES.SUBSCRIBERS, `{Status} = "Active"`);

    for (const subscriber of subscribers) {
      const subscriberId = subscriber.id;
      const phone = subscriber.fields.Phone;

      if (!phone) continue;

      // Get yesterday's work orders
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const yesterdayStr = yesterday.toISOString().split('T')[0];

      const jobs = await airtableQuery(
        TABLES.WORK_ORDERS,
        `AND({Subscriber ID} = "${subscriberId}", {Date} = "${yesterdayStr}")`
      );

      // Generate brief
      const brief = await generateMorningBrief(jobs);

      // Send SMS
      sendSMS(phone, brief);
      console.log(`Morning brief sent to ${phone}`);
    }
  } catch (error) {
    console.error('Morning brief cron error:', error);
  }
}, { timezone: 'America/New_York' });

// ============================================================================
// SERVER STARTUP
// ============================================================================

app.listen(PORT, () => {
  console.log(`FieldBrief webhook server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});
