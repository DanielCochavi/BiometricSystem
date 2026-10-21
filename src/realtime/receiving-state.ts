import { Measurement } from '../measurements/measurement.entity';
import { checkpointPosition } from '../notifications/checkpoint';

export class ReceivingState {
  readonly measurements = new Map<string, Measurement>();
  // Keep IDs until replay confirms them, even if the display cache evicts a row.
  private readonly pending = new Map<string, string | null>();
  latest?: Measurement;
  cursor?: string;

  constructor(readonly userId: string, private readonly capacity = 1000) {}

  apply(value: unknown, cursor?: string | null): boolean {
    const m = value as Measurement | undefined;
    if (!m || m.userId !== this.userId || typeof m.id !== 'string' || !/^[0-9a-f-]{36}$/.test(m.id) ||
      typeof m.eventId !== 'string' || !Number.isFinite(new Date(m.timestamp).getTime()) ||
      !Number.isFinite(new Date(m.createdAt).getTime()) || !Number.isInteger(m.heartRate) || m.heartRate < 20 || m.heartRate > 250 ||
      (m.hrv !== null && (typeof m.hrv !== 'number' || !Number.isFinite(m.hrv) || m.hrv < 0))) {
      throw new Error('RECEIVED_MEASUREMENT_INVALID');
    }
    if (cursor) checkpointPosition(this.userId, cursor);
    if (cursor && this.cursor && BigInt(checkpointPosition(this.userId, cursor)) <= BigInt(checkpointPosition(this.userId, this.cursor))) return false;
    const duplicate = this.measurements.has(m.id) || this.pending.has(m.id);
    this.pending.set(m.id, cursor ?? this.pending.get(m.id) ?? null);
    if (duplicate) return false;
    this.measurements.set(m.id, m);
    if (!this.latest || new Date(m.timestamp).getTime() > new Date(this.latest.timestamp).getTime() ||
      (new Date(m.timestamp).getTime() === new Date(this.latest.timestamp).getTime() && m.id > this.latest.id)) this.latest = m;
    if (this.measurements.size > this.capacity) {
      const oldest = [...this.measurements.keys()].find(id => id !== this.latest?.id)!;
      this.measurements.delete(oldest);
    }
    return true;
  }

  snapshot(cursor = this.cursor) {
    if (cursor) checkpointPosition(this.userId, cursor);
    if (cursor && this.cursor && BigInt(checkpointPosition(this.userId, cursor)) < BigInt(checkpointPosition(this.userId, this.cursor))) {
      throw new Error('LISTENER_CHECKPOINT_REGRESSION');
    }
    return { userId: this.userId, cursor, measurements: [...this.measurements.values()],
      pending: [...this.pending].filter(([, position]) => !position || !cursor ||
        BigInt(checkpointPosition(this.userId, position)) > BigInt(checkpointPosition(this.userId, cursor))) };
  }

  advance(cursor: string): void {
    checkpointPosition(this.userId, cursor);
    if (this.cursor && BigInt(checkpointPosition(this.userId, cursor)) < BigInt(checkpointPosition(this.userId, this.cursor))) {
      throw new Error('LISTENER_CHECKPOINT_REGRESSION');
    }
    this.cursor = cursor;
    for (const [id, position] of this.pending) {
      if (position && BigInt(checkpointPosition(this.userId, position)) <= BigInt(checkpointPosition(this.userId, cursor))) this.pending.delete(id);
    }
  }

  restore(value: unknown): void {
    const saved = value as ReturnType<ReceivingState['snapshot']>;
    if (!saved || saved.userId !== this.userId || !Array.isArray(saved.measurements) ||
        saved.measurements.length > this.capacity || !Array.isArray(saved.pending) || typeof saved.cursor !== 'string') {
      throw new Error('LISTENER_STATE_INVALID');
    }
    checkpointPosition(this.userId, saved.cursor);
    for (const measurement of saved.measurements) this.apply(measurement);
    this.pending.clear();
    for (const entry of saved.pending) {
      if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== 'string' || !/^[0-9a-f-]{36}$/.test(entry[0]) ||
          (entry[1] !== null && typeof entry[1] !== 'string')) throw new Error('LISTENER_STATE_INVALID');
      if (entry[1]) checkpointPosition(this.userId, entry[1]);
      this.pending.set(entry[0], entry[1]);
    }
    this.advance(saved.cursor);
  }
}
