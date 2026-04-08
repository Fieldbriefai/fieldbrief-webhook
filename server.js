const express = require('express');
const bodyParser = require('body-parser');
const twilio = require('twilio');
const Anthropic = require('@anthropic-ai/sdk');
const Airtable = require('airtable');

const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Clients
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Health check
app.get('/', (req, res) => {
  res.send('FieldBrief webhook server is running.');
});

// Twilio SMS webhook
app.post('/sms', async (req, res) => {
  const incomingMsg = req.body.Body || '';
  const fromNumber = req.body.From || '';

  console.log(`Incoming SMS from ${fromNumber}: ${incomingMsg}`);

  // Look up subscriber in Airtable
  let subscriber = null;
  try {
    const records = await base('Subscribers').select({
      filterByFormula: `{Phone Number} = '${fromNumber}'`
    }).firstPage();

    if (records.length > 0) {
      subscriber = {
        name: records[0].get('Full Name') || 'there',
        plan: records[0].get('Plan') || 'Brief',
        trade: records[0].get('Trade') || 'trades',
        company: records[0].get('Company Name') || ''
      };
    }
  } catch (err) {
    console.error('Airtable lookup error:', err);
  }

  // Build system prompt
  const systemPrompt = `You are FieldBrief, an AI assistant for field service contractors (HVAC, plumbing, boiler service, electrical).

Your job is to receive job information by text and send back a clean, formatted daily brief.

The subscriber's name is: ${subscriber ? subscriber.name : 'there'}
Their trade is: ${subscriber ? subscriber.trade : 'field service'}
Their plan is: ${subscriber ? subscriber.plan : 'Brief'}

When they text you jobs, format a clean brief like this:
- Greeting with their name
- List each job with time, location, and any notes
- Flag any scheduling gaps
- End with a motivational one-liner

Keep it short, practical, and SMS-friendly. No markdown, no asterisks, just clean text.
If they text job notes after a job, acknowledge and log it.
If they ask a question, answer it helpfully and briefly.`;

  // Get AI response
  let aiReply = '';
  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 500,
      system: systemPrompt,
      messages: [{ role: 'user', content: incomingMsg }]
    });
    aiReply = message.content[0].text;
  } catch (err) {
    console.error('Anthropic error:', err);
    aiReply = "Hey! Got your message. Something went wrong on our end — try again in a minute.";
  }

  // Send reply via Twilio
  try {
    await twilioClient.messages.create({
      body: aiReply,
      from: process.env.TWILIO_PHONE_NUMBER,
      to: fromNumber
    });
  } catch (err) {
    console.error('Twilio send error:', err);
  }

  res.status(200).send('OK');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`FieldBrief server running on port ${PORT}`);
});
