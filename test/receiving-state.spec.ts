import { randomUUID } from 'node:crypto';
import { ReceivingState } from '../src/realtime/receiving-state';
import { checkpoint, checkpointPosition } from '../src/notifications/checkpoint';
import { Measurement } from '../src/measurements/measurement.entity';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { saveReceiver } from '../src/realtime/receiver-file';

const measurement = (time = '2026-10-20T10:00:00Z') => ({ id: randomUUID(), eventId: randomUUID(), userId: 'user-123', timestamp: new Date(time), createdAt: new Date(time), heartRate: 80, hrv: null } as Measurement);
describe('Receiving-client merging and user-bound checkpoints', () => {
  it('applies a measurement once across live delivery, replay, and retries', () => {
    const state = new ReceivingState('user-123'); const m = measurement();
    expect(state.apply(m)).toBe(true);
    expect(state.apply({ ...m }, checkpoint('user-123', '1'))).toBe(false);
    expect(state.measurements.size).toBe(1);
    // Live processing never advances the replay checkpoint.
    expect(state.cursor).toBeUndefined();
  });
  it('shows newer live data immediately and does not replace it with older recovery data', () => {
    const state = new ReceivingState('user-123'); const newest = measurement('2026-10-21T10:00:00Z');
    state.apply(newest); state.apply(measurement(), checkpoint('user-123', '1'));
    expect(state.latest).toBe(newest); expect(state.measurements.size).toBe(2);
  });
  it('keeps the latest measurement across cache eviction and a persisted restart', () => {
    const state = new ReceivingState('user-123', 1);
    const newest = measurement('2026-10-21T10:00:00Z');
    state.apply(newest); state.apply(measurement()); state.apply(measurement());
    const resumed = new ReceivingState('user-123', 1);
    resumed.restore(state.snapshot(checkpoint('user-123', '0')));
    expect(resumed.latest?.id).toBe(newest.id);
    expect(resumed.measurements.size).toBe(1);
  });
  it('ignores already-checkpointed retries even after the bounded display cache evicts a record', () => {
    const state = new ReceivingState('user-123', 1); const first = measurement();
    state.apply(first); state.apply(measurement()); state.cursor = checkpoint('user-123', '2');
    expect(state.apply(first, checkpoint('user-123', '1'))).toBe(false);
    expect(state.measurements.size).toBe(1);
  });
  it('deduplicates live records beyond display capacity until replay catches up', () => {
    const state = new ReceivingState('user-123', 1);
    const first = measurement();
    state.apply(first); state.apply(measurement()); state.apply(measurement());
    expect(state.apply(first, checkpoint('user-123', '1'))).toBe(false);
    state.advance(checkpoint('user-123', '1'));
    expect(state.apply(first, checkpoint('user-123', '1'))).toBe(false);
  });
  it('saves displayed records and checkpoint together and deduplicates after restart', async () => {
    const folder = await mkdtemp(join(tmpdir(), 'biometric-receiver-'));
    try {
      const state = new ReceivingState('user-123', 1);
      const first = measurement(), second = measurement();
      state.apply(first); state.apply(second);
      state.apply(first, checkpoint('user-123', '1'));
      const path = join(folder, 'state.json');
      await saveReceiver(path, state, checkpoint('user-123', '1'));
      const resumed = new ReceivingState('user-123', 1);
      resumed.restore(JSON.parse(await readFile(path, 'utf8')));
      expect(resumed.cursor).toBe(checkpoint('user-123', '1'));
      expect(resumed.apply(first, checkpoint('user-123', '1'))).toBe(false);
      expect(resumed.apply(second, checkpoint('user-123', '2'))).toBe(false);
    } finally { await rm(folder, { recursive: true, force: true }); }
  });
  it('does not advance the checkpoint when saving local state fails', async () => {
    const folder = await mkdtemp(join(tmpdir(), 'biometric-receiver-'));
    try {
      const state = new ReceivingState('user-123');
      state.apply(measurement());
      await expect(saveReceiver(join(folder, 'missing', 'state.json'), state, checkpoint('user-123', '1'))).rejects.toThrow();
      expect(state.cursor).toBeUndefined();
    } finally { await rm(folder, { recursive: true, force: true }); }
  });
  it('rejects checkpoint regression and restored state for another user', () => {
    const state = new ReceivingState('user-123');
    state.advance(checkpoint('user-123', '2'));
    expect(() => state.advance(checkpoint('user-123', '1'))).toThrow('LISTENER_CHECKPOINT_REGRESSION');
    expect(() => state.restore({ userId: 'user-456', measurements: [], pending: [], cursor: checkpoint('user-456', '0') })).toThrow();
  });
  it('rejects wrong-user and invalid measurements without changing state', () => {
    const state = new ReceivingState('user-123');
    for (const value of [null, {}, { ...measurement(), userId: 'user-456' }, { ...measurement(), heartRate: '80' }, { ...measurement(), hrv: Infinity }]) {
      expect(() => state.apply(value)).toThrow('RECEIVED_MEASUREMENT_INVALID');
    }
    expect(state.measurements.size).toBe(0);
  });
  it('round-trips bigint checkpoints without JavaScript precision loss', () => {
    const value = checkpoint('user-123', '9223372036854775807');
    expect(checkpointPosition('user-123', value)).toBe('9223372036854775807');
  });
  it.each(['garbage', checkpoint('other-user', '1'), checkpoint('user-123', '-1'), checkpoint('user-123', '01'), checkpoint('user-123', '9223372036854775808')])('rejects invalid checkpoints #%#', value => {
    expect(() => checkpointPosition('user-123', value)).toThrow();
  });
});
