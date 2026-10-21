import { Logger } from '@nestjs/common';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { NotificationStore } from '../src/notifications/notification-store';
import { RealtimeBus } from '../src/realtime/realtime-bus.service';

const flush = () => new Promise<void>(resolve => setImmediate(resolve));
describe('Single publication path scheduling', () => {
  let publisher: OutboxPublisher;
  let store: { claimPending: jest.Mock; markPublished: jest.Mock; indexCommitted: jest.Mock; prune: jest.Mock };
  let bus: { publish: jest.Mock };
  beforeEach(() => {
    store = { claimPending: jest.fn().mockResolvedValue([]), markPublished: jest.fn().mockResolvedValue(undefined),
      indexCommitted: jest.fn().mockResolvedValue(0), prune: jest.fn().mockResolvedValue(undefined) };
    bus = { publish: jest.fn().mockResolvedValue(undefined) };
    publisher = new OutboxPublisher(store as unknown as NotificationStore, bus as unknown as RealtimeBus);
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
  });
  afterEach(async () => { await publisher.onModuleDestroy(); jest.restoreAllMocks(); });

  it('coalesces wake-ups and continues when work arrives during publication', async () => {
    let finish!: () => void;
    const held = new Promise<void>(resolve => { finish = resolve; });
    store.claimPending.mockResolvedValueOnce([{ measurement: { id: 'one' } }]);
    store.markPublished.mockImplementationOnce(() => held);
    for (let i = 0; i < 20; i++) publisher.wake();
    await flush();
    expect(bus.publish).toHaveBeenCalledTimes(1);
    for (let i = 0; i < 20; i++) publisher.wake();
    await flush();
    expect(store.claimPending).toHaveBeenCalledTimes(1);
    expect(store.markPublished).toHaveBeenCalledWith(['one']);
    finish();
    await flush(); await flush();
    expect(store.claimPending).toHaveBeenCalledTimes(2);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it('drains full bounded batches and records each batch together', async () => {
    store.claimPending.mockResolvedValueOnce(Array.from({ length: 250 }, (_, i) => ({ measurement: { id: String(i) } })))
      .mockResolvedValueOnce([{ measurement: { id: 'last' } }]);
    await publisher.tick(); await flush(); await flush();
    expect(bus.publish).toHaveBeenCalledTimes(251);
    expect(store.markPublished.mock.calls.map(([ids]) => ids.length)).toEqual([250, 1]);
  });

  it('records only successful sends and retains failed sends for retry', async () => {
    store.claimPending.mockResolvedValueOnce([{ measurement: { id: 'bad' } }, { measurement: { id: 'good' } }]);
    bus.publish.mockRejectedValueOnce(new Error('private patient details'));
    await publisher.tick();
    expect(store.markPublished).toHaveBeenCalledWith(['good']);
    expect(Logger.prototype.error).toHaveBeenCalledWith('REALTIME_PUBLISH_FAILED: Notification retained for retry');
  });

  it('recovers on a later tick after a failed batch marker without overlapping workers', async () => {
    store.claimPending.mockResolvedValueOnce([{ measurement: { id: 'one' } }]);
    store.markPublished.mockRejectedValueOnce(new Error('private database details'));
    await publisher.tick();
    expect(Logger.prototype.error).toHaveBeenCalledWith('OUTBOX_RETRY_FAILED: Pending notifications retained');
    await publisher.tick();
    expect(store.claimPending).toHaveBeenCalledTimes(2);
  });

  it('cancels a queued wake-up on shutdown and ignores later wake-ups', async () => {
    publisher.wake();
    await publisher.onModuleDestroy();
    publisher.wake(); await flush();
    expect(store.claimPending).not.toHaveBeenCalled();
  });
});
