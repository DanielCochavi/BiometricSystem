import { BadRequestException, GoneException, Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { Measurement } from '../measurements/measurement.entity';
import { checkpoint, checkpointPosition } from './checkpoint';
import { throwDatabaseFailure } from '../common/database-errors';

export function measurementFromRow(row: Measurement): Measurement {
  return { id: row.id, eventId: row.eventId, userId: row.userId,
    timestamp: new Date(row.timestamp), heartRate: row.heartRate, hrv: row.hrv,
    createdAt: new Date(row.createdAt) };
}

@Injectable()
export class NotificationStore {
  private readonly logger = new Logger(NotificationStore.name);
  constructor(private readonly database: DataSource) {}

  async markPublished(ids: string[]): Promise<void> {
    if (ids.length) await this.database.query(
      'UPDATE notification_outbox SET published_at = clock_timestamp() WHERE measurement_id = ANY($1::uuid[]) AND published_at IS NULL', [ids]);
  }

  // Only the replay indexer takes this lock. Ingestion never touches the counter.
  async indexCommitted(limit = 250): Promise<number> {
    return this.database.transaction(async manager => {
      const [state] = await manager.query('SELECT value FROM notification_position WHERE id = 1 FOR UPDATE');
      const rows = await manager.query(`WITH candidates AS (
        SELECT measurement_id, queued_at FROM notification_outbox WHERE position IS NULL
        ORDER BY queued_at, measurement_id LIMIT $1 FOR UPDATE SKIP LOCKED
      ), numbered AS (
        SELECT measurement_id, row_number() OVER (ORDER BY queued_at, measurement_id) AS n FROM candidates
      ) UPDATE notification_outbox o SET position = $2::bigint + numbered.n, replay_ready_at = clock_timestamp()
        FROM numbered WHERE o.measurement_id = numbered.measurement_id RETURNING o.position`, [limit, state.value]);
      // TypeORM returns [rows, affected] for raw UPDATE queries.
      const updated = rows[0] as { position: string }[];
      if (updated.length) await manager.query('UPDATE notification_position SET value = value + $1 WHERE id = 1', [updated.length]);
      return updated.length;
    });
  }

  async claimPending(limit = 250): Promise<{ measurement: Measurement; position?: string }[]> {
    const rows = await this.database.query(`WITH claimed AS (
      UPDATE notification_outbox o SET attempts = attempts + 1,
        next_attempt_at = clock_timestamp() + least(30, power(2, least(o.attempts, 5))) * interval '1 second'
      FROM (SELECT measurement_id FROM notification_outbox
        WHERE published_at IS NULL AND next_attempt_at <= clock_timestamp()
        ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED) c
      WHERE o.measurement_id = c.measurement_id RETURNING o.measurement_id, o.position
    ) SELECT m.*, c.position FROM claimed c JOIN measurements m ON m.id = c.measurement_id`, [limit]);
    return rows.map((row: Measurement & { position: string | null }) => ({
      measurement: measurementFromRow(row), position: row.position ?? undefined,
    }));
  }

  async replay(userId: string, after: string | undefined, limit: number) {
    const position = after === undefined ? undefined : checkpointPosition(userId, after);
    try { return await this.readReplay(userId, position, limit); }
    catch (error) {
      if (error instanceof BadRequestException || error instanceof GoneException) throw error;
      throwDatabaseFailure(error, this.logger);
    }
  }

  private async readReplay(userId: string, position: string | undefined, limit: number) {
    // Floor, watermark and events share one PostgreSQL statement snapshot.
    const [result] = await this.database.query(`SELECT
      s.value::text AS watermark, coalesce(f.position, 0)::text AS floor,
      coalesce((SELECT jsonb_agg(jsonb_build_object('position', page.position::text, 'measurement', to_jsonb(m)) ORDER BY page.position)
        FROM (SELECT measurement_id, position FROM notification_outbox
          WHERE user_id = $1 AND position > $2::bigint ORDER BY position LIMIT $3) page
        JOIN measurements m ON m.id = page.measurement_id), '[]'::jsonb) AS events
      FROM notification_position s LEFT JOIN notification_replay_floor f ON f.user_id = $1 WHERE s.id = 1`,
      [userId, position ?? '9223372036854775807', limit + 1]);
    if (position !== undefined && BigInt(position) > BigInt(result.watermark)) {
      throw new BadRequestException({ code: 'REPLAY_CURSOR_INVALID', message: 'Checkpoint exceeds committed replay position' });
    }
    if (position !== undefined && BigInt(position) < BigInt(result.floor)) {
      throw new GoneException({ code: 'REPLAY_EXPIRED', message: 'Checkpoint expired; refresh history and explicitly reset recovery' });
    }
    const hasMore = result.events.length > limit;
    const events = result.events.slice(0, limit).map((event: { position: string; measurement: Measurement }) => ({
      cursor: checkpoint(userId, event.position), measurement: measurementFromRow(event.measurement),
    }));
    // Moving to the committed watermark is safe when this user's page is exhausted.
    const nextCursor = hasMore ? events.at(-1).cursor : checkpoint(userId, result.watermark);
    return { userId, events, nextCursor, hasMore };
  }

  async prune(retentionHours: number, limit = 1000): Promise<void> {
    // Keep a per-user prefix: never remove completed entries beyond an older pending one.
    await this.database.query(`WITH eligible AS (
      SELECT o.measurement_id FROM notification_outbox o
      WHERE o.position IS NOT NULL AND o.published_at IS NOT NULL
        AND o.replay_ready_at < clock_timestamp() - $1 * interval '1 hour'
        AND NOT EXISTS (SELECT 1 FROM notification_outbox p WHERE p.user_id = o.user_id AND p.position < o.position
          AND (p.published_at IS NULL OR p.replay_ready_at >= clock_timestamp() - $1 * interval '1 hour'))
      ORDER BY o.position LIMIT $2
    ), removed AS (
      DELETE FROM notification_outbox WHERE measurement_id IN (SELECT measurement_id FROM eligible)
      RETURNING user_id, position
    ) INSERT INTO notification_replay_floor (user_id, position)
      SELECT user_id, max(position) FROM removed GROUP BY user_id
      ON CONFLICT (user_id) DO UPDATE SET position = greatest(notification_replay_floor.position, excluded.position)`, [retentionHours, limit]);
  }
}
