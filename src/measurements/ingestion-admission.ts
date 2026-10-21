import { Injectable, OnModuleDestroy, OnModuleInit, ServiceUnavailableException } from '@nestjs/common';
import { DataSource } from 'typeorm';

@Injectable()
export class IngestionAdmission implements OnModuleInit, OnModuleDestroy {
  private active = 0;
  private draining = false;
  private backlogFull = false;
  private storageUnavailable = false;
  private timer?: NodeJS.Timeout;
  private checking?: Promise<void>;
  private idle?: () => void;
  private readonly maxActive = this.setting('INGEST_MAX_IN_FLIGHT', 200, 1, 10000);
  private readonly maxPending = this.setting('INGEST_MAX_PENDING', 1000, 1, 1000000);

  constructor(private readonly database: DataSource) {}

  private setting(key: string, fallback: number, min: number, max: number): number {
    const value = Number(process.env[key] ?? fallback);
    if (!Number.isInteger(value) || value < min || value > max) throw new Error(`INGEST_CONFIG_INVALID: ${key} must be ${min}–${max}`);
    return value;
  }

  async onModuleInit(): Promise<void> {
    await this.refresh();
    this.timer = setInterval(() => { void this.refresh(); }, 250);
  }

  async refresh(): Promise<void> {
    if (this.draining) return;
    if (this.checking) return this.checking;
    this.checking = (async () => {
      try {
        // Bound the scan by the threshold instead of counting the whole queue.
        const [row] = await this.database.query(`SELECT EXISTS (
          SELECT 1 FROM notification_outbox WHERE published_at IS NULL OFFSET $1 LIMIT 1
        ) AS full`, [this.maxPending - 1]);
        this.backlogFull = row.full;
        this.storageUnavailable = false;
      } catch { this.storageUnavailable = true; }
    })();
    try { await this.checking; } finally { this.checking = undefined; }
  }

  assertReady(): void {
    if (this.draining) throw new ServiceUnavailableException({ code: 'SERVER_DRAINING', message: 'Server is stopping; retry the same eventId' });
    if (this.storageUnavailable) throw new ServiceUnavailableException({ code: 'DATABASE_UNAVAILABLE', message: 'Storage temporarily unavailable; retry the same eventId' });
    if (this.active >= this.maxActive || this.backlogFull) {
      throw new ServiceUnavailableException({ code: 'SERVER_BUSY', message: 'Capacity temporarily exceeded; retry the same eventId' });
    }
  }

  enter(): () => void {
    this.assertReady();
    this.active++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active--;
      if (this.active === 0) this.idle?.();
    };
  }

  async onModuleDestroy(): Promise<void> {
    this.draining = true;
    clearInterval(this.timer);
    await this.checking;
    if (this.active === 0) return;
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        new Promise<void>(resolve => { this.idle = resolve; }),
        new Promise<void>(resolve => { timer = setTimeout(resolve, 10000); }),
      ]);
    } finally { if (timer) clearTimeout(timer); this.idle = undefined; }
  }
}
