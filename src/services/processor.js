const logger = require('../logger');

class MessageProcessor {
  constructor(supabase, waClient) {
    this.supabase = supabase;
    this.waClient = waClient;
    this.isProcessing = false;
  }

  // ─────────────────────────────────────
  // MAIN PROCESS LOOP — called every minute by pg_cron
  // ─────────────────────────────────────
  async processPendingMessages() {
    if (this.isProcessing) {
      logger.info('Processor already running — skipping this tick');
      return { processed: 0, skipped: 0, reason: 'already_running' };
    }

    if (this.waClient.getStatus() !== 'connected') {
      logger.warn('WhatsApp not connected — skipping processing');
      return { processed: 0, skipped: 0, reason: 'wa_disconnected' };
    }

    this.isProcessing = true;
    let processed = 0;
    let skipped = 0;

    try {
      // Get settings
      const { data: settings } = await this.supabase
        .from('settings')
        .select('*')
        .single();

      const now = new Date();
      const nowIST = new Date(now.toLocaleString('en-US', { timeZone: settings?.timezone || 'Asia/Kolkata' }));
      const currentHour = nowIST.getHours();
      const currentMinute = nowIST.getMinutes();
      const currentTime = currentHour * 60 + currentMinute;

      // Get all active campaigns
      const { data: campaigns } = await this.supabase
        .from('campaigns')
        .select('*')
        .eq('status', 'active');

      for (const campaign of (campaigns || [])) {
        // Check campaign time window
        const [startH, startM] = campaign.start_time.split(':').map(Number);
        const [endH, endM] = campaign.end_time.split(':').map(Number);
        const startMins = startH * 60 + startM;
        const endMins = endH * 60 + endM;

        if (currentTime < startMins || currentTime > endMins) {
          logger.info(`Campaign "${campaign.name}" outside time window — skipping`);
          skipped++;
          continue;
        }

        // Check daily limit
        if (campaign.sent_today >= campaign.daily_limit) {
          logger.info(`Campaign "${campaign.name}" daily limit reached (${campaign.sent_today}/${campaign.daily_limit})`);
          skipped++;
          continue;
        }

        // Get next due message for this campaign
        const { data: job } = await this.supabase
          .from('message_queue')
          .select(`
            *,
            leads (id, full_name, mobile, whatsapp_number, university, course, semester, assignment, do_not_contact, status),
            message_templates (id, name, body),
            campaign_leads (id, status)
          `)
          .eq('campaign_id', campaign.id)
          .in('status', ['pending', 'scheduled'])
          .lte('scheduled_at', now.toISOString())
          .order('scheduled_at', { ascending: true })
          .limit(1)
          .maybeSingle();

        if (!job) {
          logger.info(`No pending jobs for campaign "${campaign.name}"`);
          continue;
        }

        // Safety checks
        if (job.leads?.do_not_contact || job.leads?.status === 'do_not_contact') {
          await this.skipMessage(job.id, 'Lead is Do Not Contact');
          skipped++;
          continue;
        }

        if (job.campaign_leads?.status !== 'active') {
          await this.skipMessage(job.id, `Campaign lead status: ${job.campaign_leads?.status}`);
          skipped++;
          continue;
        }

        if (job.attempt_count >= job.max_attempts) {
          await this.failMessage(job.id, 'Max attempts exceeded');
          skipped++;
          continue;
        }

        // Personalize message
        const personalizedMsg = this.personalizeMessage(
          job.message_templates?.body || '',
          job.leads
        );

        // Mark as sending
        await this.supabase
          .from('message_queue')
          .update({
            status: 'sending',
            attempt_count: job.attempt_count + 1,
            last_attempt_at: now.toISOString(),
            personalized_message: personalizedMsg
          })
          .eq('id', job.id);

        try {
          // Send the message
          const mobile = job.leads?.whatsapp_number || job.leads?.mobile;
          const sendResult = await this.waClient.sendMessage(mobile, personalizedMsg);

          // Log success
          await this.logMessage(job, personalizedMsg, 'sent', null, sendResult.messageId);

          // Update queue status
          await this.supabase
            .from('message_queue')
            .update({ status: 'sent', updated_at: now.toISOString() })
            .eq('id', job.id);

          // Update campaign daily counter
          await this.supabase
            .from('campaigns')
            .update({ sent_today: campaign.sent_today + 1 })
            .eq('id', campaign.id);

          // Update lead's last contacted
          await this.supabase
            .from('leads')
            .update({
              last_contacted_at: now.toISOString(),
              status: 'contacted',
              updated_at: now.toISOString()
            })
            .eq('id', job.lead_id);

          // Advance campaign lead step
          await this.supabase
            .from('campaign_leads')
            .update({ current_step: job.step_number })
            .eq('id', job.campaign_leads.id);

          // Schedule next message in sequence
          await this.scheduleNextMessage(job, campaign);

          logger.info(`✅ Sent to ${job.leads?.full_name} (${mobile}) — "${job.message_templates?.name}"`);
          processed++;

        } catch (sendErr) {
          logger.error(`❌ Failed to send to ${job.leads?.full_name}:`, sendErr.message);
          await this.logMessage(job, personalizedMsg, 'failed', sendErr.message);
          await this.supabase
            .from('message_queue')
            .update({
              status: job.attempt_count + 1 >= job.max_attempts ? 'failed' : 'pending',
              error_message: sendErr.message,
              updated_at: now.toISOString()
            })
            .eq('id', job.id);
          skipped++;
        }

        // Respect interval between messages
        await this.sleep(2000);
      }
    } catch (err) {
      logger.error('Processor error:', err);
    } finally {
      this.isProcessing = false;
    }

    logger.info(`Processor done — processed: ${processed}, skipped: ${skipped}`);
    return { processed, skipped };
  }

  // ─────────────────────────────────────
  // SCHEDULE NEXT MESSAGE IN SEQUENCE
  // ─────────────────────────────────────
  async scheduleNextMessage(currentJob, campaign) {
    // Get all steps for this campaign
    const { data: steps } = await this.supabase
      .from('campaign_message_steps')
      .select('*, message_templates(*)')
      .eq('campaign_id', campaign.id)
      .eq('is_enabled', true)
      .gt('step_number', currentJob.step_number)
      .order('step_number', { ascending: true })
      .limit(1);

    if (!steps || steps.length === 0) {
      // No more steps — mark campaign lead as completed
      await this.supabase
        .from('campaign_leads')
        .update({ status: 'completed', completed_at: new Date().toISOString() })
        .eq('id', currentJob.campaign_leads.id);
      return;
    }

    const nextStep = steps[0];
    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + nextStep.delay_days);

    // Set to campaign start time on that day
    const [h, m] = campaign.start_time.split(':').map(Number);
    nextDate.setHours(h, m, 0, 0);

    const idempotencyKey = `${campaign.id}-${currentJob.lead_id}-step-${nextStep.step_number}`;

    await this.supabase
      .from('message_queue')
      .upsert({
        idempotency_key: idempotencyKey,
        campaign_id: campaign.id,
        campaign_lead_id: currentJob.campaign_lead_id,
        lead_id: currentJob.lead_id,
        template_id: nextStep.template_id,
        step_number: nextStep.step_number,
        scheduled_at: nextDate.toISOString(),
        status: 'pending'
      }, { onConflict: 'idempotency_key', ignoreDuplicates: true });
  }

  // ─────────────────────────────────────
  // PERSONALIZE MESSAGE
  // ─────────────────────────────────────
  personalizeMessage(template, lead) {
    return template
      .replace(/{{name}}/g, lead?.full_name?.split(' ')[0] || 'Student')
      .replace(/{{full_name}}/g, lead?.full_name || '')
      .replace(/{{mobile}}/g, lead?.mobile || '')
      .replace(/{{university}}/g, lead?.university || '')
      .replace(/{{course}}/g, lead?.course || '')
      .replace(/{{semester}}/g, lead?.semester || '')
      .replace(/{{assignment}}/g, lead?.assignment || '');
  }

  // ─────────────────────────────────────
  // HELPERS
  // ─────────────────────────────────────
  async logMessage(job, text, status, error = null, waId = null) {
    await this.supabase.from('message_logs').insert({
      queue_id: job.id,
      campaign_id: job.campaign_id,
      lead_id: job.lead_id,
      template_id: job.template_id,
      mobile: job.leads?.mobile,
      message_text: text,
      step_number: job.step_number,
      scheduled_at: job.scheduled_at,
      sent_at: status === 'sent' ? new Date().toISOString() : null,
      status,
      error_message: error,
      whatsapp_message_id: waId
    });
  }

  async skipMessage(id, reason) {
    await this.supabase
      .from('message_queue')
      .update({ status: 'skipped', error_message: reason, updated_at: new Date().toISOString() })
      .eq('id', id);
  }

  async failMessage(id, reason) {
    await this.supabase
      .from('message_queue')
      .update({ status: 'failed', error_message: reason, updated_at: new Date().toISOString() })
      .eq('id', id);
  }

  async handleIncomingReply(mobile, text) {
    return this.waClient.handleIncomingReply(mobile, text);
  }

  sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
}

module.exports = MessageProcessor;
