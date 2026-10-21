import { ConflictException, Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { OutboxPublisher } from '../notifications/outbox-publisher';
import { CreateMeasurementDto } from './dto/create-measurement.dto';
import { EVENT_UNIQUE_CONSTRAINT, Measurement } from './measurement.entity';
import { measurementFromRow } from '../notifications/notification-store';
import { IngestionAdmission } from './ingestion-admission';
import { databaseCause, throwDatabaseFailure } from '../common/database-errors';

export interface IngestionResult {
  measurement: Measurement;
  created: boolean;
}

@Injectable()
export class MeasurementsService {
  private readonly logger = new Logger(MeasurementsService.name);
  constructor(
    @InjectRepository(Measurement)
    private readonly repository: Repository<Measurement>,
    private readonly publisher: OutboxPublisher,
    private readonly admission: IngestionAdmission,
  ) {}

  async ingest(dto: CreateMeasurementDto): Promise<IngestionResult> {
    const release = this.admission.enter();
    try { return await this.store(dto); } finally { release(); }
  }

  private async store(dto: CreateMeasurementDto): Promise<IngestionResult> {
    const entity = this.repository.create({
      ...dto,
      timestamp: new Date(dto.timestamp),
      hrv: dto.hrv ?? null,
    });
    let saved: Measurement;
    try {
      // Both writes commit atomically in one SQL command. There is no replay counter lock here.
      const [row] = await this.repository.query(`WITH saved AS (
        INSERT INTO measurements ("eventId", "userId", "timestamp", "heartRate", hrv)
        VALUES ($1, $2, $3, $4, $5) RETURNING *
      ), queued AS (
        INSERT INTO notification_outbox (measurement_id, user_id, next_attempt_at)
        SELECT id, "userId", clock_timestamp() FROM saved RETURNING measurement_id
      ) SELECT saved.* FROM saved JOIN queued ON saved.id = queued.measurement_id`,
      [entity.eventId, entity.userId, entity.timestamp, entity.heartRate, entity.hrv]);
      saved = measurementFromRow(row);
    } catch (error) {
      const cause = databaseCause(error);
      if (cause.code === '23505' && cause.constraint === EVENT_UNIQUE_CONSTRAINT) {
        return this.resolveRetry(entity);
      }
      this.throwDatabaseFailure(error);
    }

    // Wake only after commit. Polling also discovers work if the process exits here.
    this.publisher.wake();
    return { measurement: saved, created: true };
  }

  private async resolveRetry(incoming: Measurement): Promise<IngestionResult> {
    let existing: Measurement | null;
    try {
      existing = await this.repository.findOneBy({ userId: incoming.userId, eventId: incoming.eventId });
    } catch (error) {
      this.throwDatabaseFailure(error);
    }
    if (!existing) this.throwDatabaseFailure(new Error('Missing row after uniqueness conflict'));
    if (existing.timestamp.getTime() !== incoming.timestamp.getTime() ||
        existing.heartRate !== incoming.heartRate || existing.hrv !== incoming.hrv) {
      throw new ConflictException({ code: 'EVENT_CONFLICT', message: 'Event identity already exists with different measurement values' });
    }
    return { measurement: existing, created: false };
  }

  async findRecent(userId: string, limit: number): Promise<Measurement[]> {
    try {
      return await this.repository.find({
        where: { userId },
        order: { timestamp: 'DESC', id: 'DESC' },
        take: limit,
      });
    } catch (error) {
      this.throwDatabaseFailure(error);
    }
  }

  private throwDatabaseFailure(error: unknown): never {
    return throwDatabaseFailure(error, this.logger);
  }
}
