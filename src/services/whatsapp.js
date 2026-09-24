const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeInMemoryStore,
  jidDecode,
  proto,
  getContentType
} = require('baileys');
const { Boom } = require('@hapi/boom');
const QRCode = require('qrcode');
const path = require('path');
const fs = require('fs');
const logger = require('../logger');

const SESSION_DIR = path.join(__dirname, '../../.wa_session');

class WAClient {
  constructor(supabase) {
    this.supabase = supabase;
    this.sock = null;
    this.status = 'disconnected';
    this.qrCode = null;
    this.qrBase64 = null;
    this.replyHandlers = [];
  }

  getStatus() { return this.status; }
  getQR() { return this.qrBase64; }

  async updateSessionStatus(status, extra = {}) {
    this.status = status;
    await this.supabase
      .from('whatsapp_sessions')
      .update({
        status,
        last_heartbeat: new Date().toISOString(),
        ...extra
      })
      .eq('session_key', 'main');
  }

  async initialize() {
    try {
      if (!fs.existsSync(SESSION_DIR)) {
        fs.mkdirSync(SESSION_DIR, { recursive: true });
      }

      const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
      const { version } = await fetchLatestBaileysVersion();

      logger.info(`Initializing WhatsApp with Baileys v${version.join('.')}`);

      await this.updateSessionStatus('waiting_qr');

      this.sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger: logger.child({ level: 'silent' }),
        browser: ['Yash WhatsApp', 'Chrome', '120.0.0'],
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 30000,
        keepAliveIntervalMs: 10000,
        markOnlineOnConnect: false,
      });

      // ── QR Code event ──
      this.sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          logger.info('QR code generated');
          this.qrCode = qr;
          // Convert to base64 image for frontend
          this.qrBase64 = await QRCode.toDataURL(qr, { width: 300, margin: 2 });
          await this.updateSessionStatus('qr_ready', { qr_code: this.qrBase64 });
        }

        if (connection === 'close') {
          const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
          const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

          logger.warn(`Connection closed — code: ${statusCode} — reconnect: ${shouldReconnect}`);
          await this.updateSessionStatus('disconnected', { disconnected_at: new Date().toISOString() });

          if (shouldReconnect) {
            logger.info('Reconnecting in 5 seconds...');
            setTimeout(() => this.initialize(), 5000);
          } else {
            // Logged out — clear session
            fs.rmSync(SESSION_DIR, { recursive: true, force: true });
            this.sock = null;
            this.qrCode = null;
            this.qrBase64 = null;
          }
        }

        if (connection === 'open') {
          const phone = this.sock.user?.id?.split(':')[0] || '';
          logger.info(`WhatsApp connected — phone: ${phone}`);
          this.qrCode = null;
          this.qrBase64 = null;
          await this.updateSessionStatus('connected', {
            phone_number: phone,
            connected_at: new Date().toISOString(),
            qr_code: null
          });
        }
      });

      // ── Save credentials ──
      this.sock.ev.on('creds.update', saveCreds);

      // ── Incoming messages ──
      this.sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
          if (msg.key.fromMe) continue; // Skip own messages
          if (!msg.message) continue;

          const jid = msg.key.remoteJid;
          const mobile = jidDecode(jid)?.user;
          if (!mobile) continue;

          const contentType = getContentType(msg.message);
          let text = '';

          if (contentType === 'conversation') {
            text = msg.message.conversation;
          } else if (contentType === 'extendedTextMessage') {
            text = msg.message.extendedTextMessage?.text || '';
          }

          if (text) {
            logger.info(`Incoming reply from ${mobile}: "${text}"`);
            await this.handleIncomingReply(mobile, text);
          }
        }
      });

    } catch (err) {
      logger.error('Failed to initialize WhatsApp:', err);
      await this.updateSessionStatus('error', { error_message: err.message });
      throw err;
    }
  }

  async sendMessage(mobile, text) {
    if (this.status !== 'connected' || !this.sock) {
      throw new Error('WhatsApp is not connected');
    }

    // Normalize number to WhatsApp JID format
    const normalized = this.normalizePhone(mobile);
    const jid = `${normalized}@s.whatsapp.net`;

    try {
      const result = await this.sock.sendMessage(jid, { text });
      logger.info(`Message sent to ${mobile} — ID: ${result?.key?.id}`);
      return { success: true, messageId: result?.key?.id };
    } catch (err) {
      logger.error(`Failed to send to ${mobile}:`, err.message);
      throw err;
    }
  }

  normalizePhone(mobile) {
    // Remove all non-digits
    let num = mobile.replace(/\D/g, '');

    // Handle Indian numbers
    if (num.length === 10) num = '91' + num;          // 9876543210 → 919876543210
    if (num.startsWith('0')) num = '91' + num.slice(1); // 09876543210 → 919876543210

    return num;
  }

  async handleIncomingReply(mobile, text) {
    try {
      const normalized = this.normalizePhone(mobile);

      // Find lead by mobile
      const { data: lead } = await this.supabase
        .from('leads')
        .select('id, full_name')
        .or(`mobile.eq.${normalized},mobile.eq.${mobile}`)
        .maybeSingle();

      if (!lead) {
        logger.warn(`Reply from unknown number: ${mobile}`);
        return;
      }

      // Find active campaign for this lead
      const { data: campaignLead } = await this.supabase
        .from('campaign_leads')
        .select('id, campaign_id')
        .eq('lead_id', lead.id)
        .eq('status', 'active')
        .maybeSingle();

      // Record the reply
      await this.supabase.from('replies').insert({
        lead_id: lead.id,
        campaign_id: campaignLead?.campaign_id || null,
        mobile: normalized,
        message_text: text,
        received_at: new Date().toISOString()
      });

      // Update lead status
      await this.supabase
        .from('leads')
        .update({ status: 'replied', updated_at: new Date().toISOString() })
        .eq('id', lead.id);

      // Pause automation for this lead
      if (campaignLead) {
        await this.supabase
          .from('campaign_leads')
          .update({ status: 'replied' })
          .eq('id', campaignLead.id);

        // Cancel all pending messages for this lead
        await this.supabase
          .from('message_queue')
          .update({ status: 'cancelled', updated_at: new Date().toISOString() })
          .eq('lead_id', lead.id)
          .in('status', ['pending', 'scheduled']);

        logger.info(`Automation paused for ${lead.full_name} — reply received`);
      }
    } catch (err) {
      logger.error('Error handling incoming reply:', err);
    }
  }

  async disconnect() {
    if (this.sock) {
      await this.sock.logout();
      this.sock = null;
    }
    this.qrCode = null;
    this.qrBase64 = null;
    await this.updateSessionStatus('disconnected', {
      disconnected_at: new Date().toISOString()
    });
    logger.info('WhatsApp disconnected');
  }
}

module.exports = WAClient;
