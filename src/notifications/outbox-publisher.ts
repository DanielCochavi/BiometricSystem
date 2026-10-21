import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { RealtimeBus } from '../realtime/realtime-bus.service';
import { NotificationStore } from './notification-store';

@Injectable()
export class OutboxPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisher.name);
  private timer?: NodeJS.Timeout;
  private running?: Promise<void>;
  private scheduled?: NodeJS.Immediate;
  private requested = false;
  private lastPruned = Date.now();
  private stopped = false;
  readonly retentionHours: number;

  constructor(private readonly store: NotificationStore, private readonly bus: RealtimeBus) {
    this.retentionHours = Number(process.env.REPLAY_RETENTION_HOURS ?? 24);
    if (!Number.isInteger(this.retentionHours) || this.retentionHours < 24 || this.retentionHours > 8760) {
      throw new Error('NOTIFICATION_CONFIG_INVALID: REPLAY_RETENTION_HOURS must be 24–8760');
    }
  }

  onModuleInit(): void {
    this.timer = setInterval(() => { void this.tick(); }, 50);
  }

  // Coalesce committed writes without holding measurements in memory.
  wake(): void {
    if (this.stopped) return;
    this.requested = true;
    if (this.running || this.scheduled) return;
    this.scheduled = setImmediate(() => {
      this.scheduled = undefined;
      void this.tick();
    });
  }

  async tick(): Promise<void> {
    if (this.stopped) return;
    if (this.running) { this.requested = true; return this.running; }
    this.requested = false;
    this.running = this.work().catch(() => this.logger.error('OUTBOX_RETRY_FAILED: Pending notifications retained'));
    try { await this.running; }
    finally {
      this.running = undefined;
      if (this.requested) this.wake();
    }
  }

  private async work(): Promise<void> {
    const pending = await this.store.claimPending();
    const published: string[] = [];
    await Promise.all(pending.map(async ({ measurement, position }) => {
      try { await this.bus.publish(measurement, { position }); published.push(measurement.id); }
      catch { this.logger.error('REALTIME_PUBLISH_FAILED: Notification retained for retry'); }
    }));
    await this.store.markPublished(published);
    // Index after sending so recovery work does not delay this batch's live updates.
    await this.store.indexCommitted();
    if (pending.length === 250) this.requested = true;
    if (Date.now() - this.lastPruned >= 60000) {
      await this.store.prune(this.retentionHours);
      this.lastPruned = Date.now();
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    clearInterval(this.timer);
    if (this.scheduled) clearImmediate(this.scheduled);
    await this.running;
  }
}
