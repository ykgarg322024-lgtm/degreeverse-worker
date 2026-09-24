require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { createClient } = require('@supabase/supabase-js');
const logger = require('./logger');
const WAClient = require('./services/whatsapp');
const MessageProcessor = require('./services/processor');

// ─────────────────────────────────────
// SUPABASE CLIENT (service role — full access)
// ─────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ─────────────────────────────────────
// EXPRESS APP
// ─────────────────────────────────────
const app = express();
app.use(helmet());
app.use(cors({ origin: process.env.FRONTEND_URL || '*' }));
app.use(express.json({ limit: '10mb' }));

// Rate limit — protect the worker endpoint
const limiter = rateLimit({ windowMs: 60 * 1000, max: 120 });
app.use(limiter);

// Worker secret middleware
function requireWorkerSecret(req, res, next) {
  const secret = req.headers['x-worker-secret'];
  if (secret !== process.env.WORKER_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ─────────────────────────────────────
// WHATSAPP CLIENT SINGLETON
// ─────────────────────────────────────
const waClient = new WAClient(supabase);
const processor = new MessageProcessor(supabase, waClient);

// ─────────────────────────────────────
// ROUTES
// ─────────────────────────────────────

// Health check
app.get('/health', async (req, res) => {
  const waStatus = waClient.getStatus();
  const { data: session } = await supabase
    .from('whatsapp_sessions')
    .select('status, last_heartbeat')
    .eq('session_key', 'main')
    .single();

  res.json({
    status: 'ok',
    worker: 'running',
    whatsapp: waStatus,
    database: session ? 'connected' : 'error',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// Process pending messages — called by Supabase pg_cron every minute
app.post('/process', requireWorkerSecret, async (req, res) => {
  try {
    const result = await processor.processPendingMessages();
    res.json({ success: true, processed: result.processed, skipped: result.skipped });
  } catch (err) {
    logger.error('Process error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get QR code for frontend
app.get('/qr', requireWorkerSecret, async (req, res) => {
  const qr = waClient.getQR();
  if (!qr) return res.json({ qr: null, status: waClient.getStatus() });
  res.json({ qr, status: waClient.getStatus() });
});

// Start WhatsApp connection
app.post('/connect', requireWorkerSecret, async (req, res) => {
  try {
    await waClient.initialize();
    res.json({ success: true, message: 'Connecting to WhatsApp...' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Disconnect WhatsApp
app.post('/disconnect', requireWorkerSecret, async (req, res) => {
  try {
    await waClient.disconnect();
    res.json({ success: true, message: 'WhatsApp disconnected.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Send a single test message
app.post('/send-test', requireWorkerSecret, async (req, res) => {
  const { mobile, message } = req.body;
  if (!mobile || !message) return res.status(400).json({ error: 'mobile and message required' });
  try {
    const result = await waClient.sendMessage(mobile, message);
    res.json({ success: true, result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Incoming reply webhook (WhatsApp → Supabase)
app.post('/webhook/reply', requireWorkerSecret, async (req, res) => {
  try {
    const { mobile, message } = req.body;
    await processor.handleIncomingReply(mobile, message);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────
// START SERVER
// ─────────────────────────────────────
const PORT = process.env.PORT || 3001;

app.listen(PORT, async () => {
  logger.info(`🚀 Yash WhatsApp WA Worker running on port ${PORT}`);

  // Auto-connect WhatsApp on startup if session exists
  try {
    const { data: session } = await supabase
      .from('whatsapp_sessions')
      .select('status')
      .eq('session_key', 'main')
      .single();

    if (session?.status === 'connected') {
      logger.info('Restoring previous WhatsApp session...');
      await waClient.initialize();
    }
  } catch (err) {
    logger.warn('Could not restore session:', err.message);
  }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received — shutting down gracefully');
  await waClient.disconnect();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection:', reason);
});
