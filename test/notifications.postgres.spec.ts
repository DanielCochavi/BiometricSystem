import { INestApplication, Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { io, Socket } from 'socket.io-client';
import source from '../src/database/data-source';
import { MeasurementsModule } from '../src/measurements/measurements.module';
import { IngestionAdmission } from '../src/measurements/ingestion-admission';
import { NotificationStore } from '../src/notifications/notification-store';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { RealtimeBus } from '../src/realtime/realtime-bus.service';
import { RealtimeGateway } from '../src/realtime/realtime.gateway';
import { createValidationPipe } from '../src/common/validation';
import { checkpoint, checkpointPosition } from '../src/notifications/checkpoint';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
describe('Durable notifications and safe replay with real PostgreSQL and Socket.IO', () => {
  const originalAuth = process.env.AUTH_IDENTITIES;
  const name = `biometric_notifications_${randomUUID().replaceAll('-', '')}`;
  let admin: DataSource, db: DataSource, app: INestApplication, store: NotificationStore, publisher: OutboxPublisher;
  let base: string;
  let created = false;
  const sockets: Socket[] = [];
  const payload = { eventId: 'evt-1', userId: 'user-123', timestamp: '2026-10-20T10:00:00.120Z', heartRate: 80, hrv: 42 };
  beforeAll(async () => {
    if (source.options.type !== 'postgres') throw new Error('PostgreSQL required');
    const config = JSON.parse(authConfig);
    config[0].writeUserIds.push('user-456', 'user-789'); config[1].readUserIds.push('user-456');
    config.push({ id: 'other-clinician', token: 'other-clinician-test-token', readUserIds: ['user-789'], writeUserIds: [] });
    process.env.AUTH_IDENTITIES = JSON.stringify(config);
    admin = await new DataSource({ type: 'postgres', url: process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview' }).initialize();
    await admin.query(`CREATE DATABASE "${name}"`); created = true;
    const url = new URL(process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview'); url.pathname = `/${name}`;
    const module = await Test.createTestingModule({ imports: [TypeOrmModule.forRoot({ ...source.options, url: url.toString(), retryAttempts: 0 }), MeasurementsModule] })
      .overrideProvider(OutboxPublisher).useValue({ wake() {} }).compile();
    app = module.createNestApplication({ logger: false }); app.useGlobalPipes(createValidationPipe());
    db = app.get(DataSource); await db.runMigrations(); store = app.get(NotificationStore);
    await app.listen(0, '127.0.0.1'); base = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;
  }, 20000);
  beforeEach(async () => {
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    await db.query('TRUNCATE measurements CASCADE'); await db.query('TRUNCATE notification_replay_floor');
    await db.query('UPDATE notification_position SET value = 0');
    await app.get(IngestionAdmission).refresh();
    publisher = new OutboxPublisher(store, app.get(RealtimeBus));
  });
  afterEach(async () => { await publisher.onModuleDestroy(); sockets.splice(0).forEach(s => s.disconnect()); jest.restoreAllMocks(); });
  afterAll(async () => {
    await app?.close();
    if (created) await admin.query(`DROP DATABASE "${name}"`);
    await admin?.destroy();
    if (originalAuth === undefined) delete process.env.AUTH_IDENTITIES; else process.env.AUTH_IDENTITIES = originalAuth;
  });
  function post(body: unknown = payload) { return fetch(`${base}/measurements`, { method: 'POST', headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'application/json' }, body: JSON.stringify(body) }); }
  function replay(after?: string, userId = 'user-123', query = '', token = clinicianToken) {
    return fetch(`${base}/measurements/${encodeURIComponent(userId)}/events?${after === undefined ? '' : `after=${encodeURIComponent(after)}&`}${query}`, { headers: { authorization: `Bearer ${token}` } });
  }
  async function receiver() {
    const messages: any[] = [];
    const socket = io(base, { auth: { token: clinicianToken }, transports: ['websocket'], reconnection: false }); sockets.push(socket);
    socket.on('measurement', (m, meta) => { messages.push({ measurement: m, metadata: meta }); });
    await new Promise<void>((resolve, reject) => { socket.once('connect', resolve); socket.once('connect_error', reject); });
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: 'user-123' })).toEqual({ subscribed: true, userId: 'user-123' });
    return { socket, messages };
  }
  it('stores exactly one outbox entry under 20 concurrent identical submissions', async () => {
    const responses = await Promise.all(Array.from({ length: 20 }, () => post()));
    expect(responses.filter(r => r.status === 201)).toHaveLength(1);
    expect(responses.filter(r => r.status === 200)).toHaveLength(19);
    expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(1);
    expect((await post({ ...payload, heartRate: 81 })).status).toBe(409);
    expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(1);
  });
  it('batches publication markers after atomic storage without sending from ingestion', async () => {
    const queries = jest.spyOn(db, 'query');
    const publish = jest.spyOn(app.get(RealtimeBus), 'publish');
    await Promise.all(Array.from({ length: 20 }, (_, i) => post({ ...payload, eventId: `published-${i}` })));
    expect(publish).not.toHaveBeenCalled();
    await publisher.tick();
    expect(publish).toHaveBeenCalledTimes(20);
    expect(queries.mock.calls.filter(([sql]) => String(sql).startsWith('UPDATE notification_outbox SET published_at'))).toHaveLength(1);
    expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox WHERE published_at IS NOT NULL'))[0].n).toBe(20);
  });
  it('keeps a notification retryable if recording publication fails after sending', async () => {
    const marker = jest.spyOn(store, 'markPublished').mockRejectedValueOnce(new Error('marker update failed'));
    expect((await post()).status).toBe(201);
    await publisher.tick();
    expect((await db.query('SELECT published_at FROM notification_outbox'))[0].published_at).toBeNull();
    await db.query("UPDATE notification_outbox SET published_at = NULL, next_attempt_at = clock_timestamp() - interval '1 second'");
    await publisher.tick();
    expect(marker).toHaveBeenCalledTimes(2);
    expect((await db.query('SELECT published_at FROM notification_outbox'))[0].published_at).not.toBeNull();
  });
  it('does not overlap sends while a publication marker waits beyond retry eligibility', async () => {
    const { messages } = await receiver();
    const original = store.markPublished.bind(store);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const marker = jest.spyOn(store, 'markPublished').mockImplementationOnce(async ids => { await gate; await original(ids); });
    expect((await post()).status).toBe(201);
    const running = publisher.tick();
    try {
      for (let i = 0; i < 100 && marker.mock.calls.length === 0; i++) await pause(10);
      expect(marker).toHaveBeenCalledTimes(1);
      await pause(1100);
      // The row is eligible again, but this publisher must finish its batch first.
      const overlapping = publisher.tick();
      publisher.wake();
      await pause(50);
      expect(messages).toHaveLength(1);
      expect(marker).toHaveBeenCalledTimes(1);
      release();
      await Promise.all([running, overlapping]);
      await publisher.tick();
      await pause(50);
      expect(messages).toHaveLength(1);
      expect((await db.query('SELECT published_at, attempts FROM notification_outbox'))[0])
        .toMatchObject({ published_at: expect.any(Date), attempts: 1 });
    } finally { release(); await running; }
  });
  it('rolls back the measurement if insertion into the outbox fails', async () => {
    await db.query(`CREATE FUNCTION reject_notification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private failure'; END $$`);
    await db.query('CREATE TRIGGER reject_notification BEFORE INSERT ON notification_outbox FOR EACH ROW EXECUTE FUNCTION reject_notification()');
    try {
      const response = await post(); expect(response.status).toBe(500);
      expect(await response.json()).toMatchObject({ code: 'INTERNAL_ERROR' });
      expect((await db.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(0);
      expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(0);
    } finally { await db.query('DROP TRIGGER reject_notification ON notification_outbox'); await db.query('DROP FUNCTION reject_notification()'); }
  });
  it('preserves pre-migration history while replay starts with new measurements', async () => {
    await db.undoLastMigration();
    try {
      await db.query('INSERT INTO measurements ("eventId","userId",timestamp,"heartRate",hrv) VALUES ($1,$2,$3,$4,$5)',
        ['before-outbox', payload.userId, payload.timestamp, 80, 42]);
    } finally { await db.runMigrations(); }
    // Wait for admission to observe the restored schema after this simulated outage.
    await app.get(IngestionAdmission).refresh();
    const response = await fetch(`${base}/measurements/user-123`, { headers: { authorization: `Bearer ${clinicianToken}` } });
    expect((await response.json()).map((m: any) => m.eventId)).toEqual(['before-outbox']);
    expect((await (await replay(checkpoint('user-123', '0'))).json()).events).toEqual([]);
    expect((await post()).status).toBe(201);
    expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(1);
  });
  it('returns a sanitized 503 when recovery storage is temporarily unavailable', async () => {
    const query = db.query.bind(db);
    jest.spyOn(db, 'query').mockImplementation((sql: string, parameters?: any[], runner?: any) => {
      if (sql.includes('s.value::text AS watermark')) return Promise.reject(Object.assign(new Error('private database detail'), { code: 'ECONNREFUSED' }));
      return query(sql, parameters, runner);
    });
    const response = await replay(checkpoint('user-123', '0'));
    expect(response.status).toBe(503);
    const body = await response.json();
    expect(body.code).toBe('DATABASE_UNAVAILABLE');
    expect(JSON.stringify(body)).not.toContain('private database detail');
  });
  it('replays in bounded pages with stable values, metadata, and user isolation', async () => {
    const bodies = [];
    for (let i = 0; i < 5; i++) bodies.push(await (await post({ ...payload, eventId: `evt-${i}` })).json());
    expect((await post({ ...payload, userId: 'user-456' })).status).toBe(201);
    await store.indexCommitted();
    const first = await (await replay(checkpoint('user-123', '0'), 'user-123', 'limit=2')).json();
    expect(first.events).toHaveLength(2); expect(first.hasMore).toBe(true);
    const second = await (await replay(first.nextCursor, 'user-123', 'limit=2')).json();
    const third = await (await replay(second.nextCursor, 'user-123', 'limit=2')).json();
    const received = [...first.events, ...second.events, ...third.events].map(e => e.measurement);
    expect(received).toEqual(bodies); expect(third.hasMore).toBe(false);
    expect((await (await replay(third.nextCursor)).json()).events).toEqual([]);
  });
  it('does not skip a transaction that commits after a newer transaction was indexed', async () => {
    const runner = db.createQueryRunner(); await runner.connect(); await runner.startTransaction();
    try {
      const [late] = await runner.query('INSERT INTO measurements ("eventId","userId",timestamp,"heartRate",hrv) VALUES ($1,$2,$3,$4,$5) RETURNING *', ['late-commit', payload.userId, payload.timestamp, 80, 42]);
      await runner.query('INSERT INTO notification_outbox (measurement_id,user_id) VALUES ($1,$2)', [late.id, late.userId]);
      const earlier = await (await post()).json();
      await store.indexCommitted();
      const page = await (await replay(checkpoint('user-123', '0'))).json();
      expect(page.events.map((e: any) => e.measurement.id)).toEqual([earlier.id]);
      await runner.commitTransaction();
      await store.indexCommitted();
      const resumed = await (await replay(page.nextCursor)).json();
      expect(resumed.events.map((e: any) => e.measurement.id)).toEqual([late.id]);
    } finally { if (runner.isTransactionActive) await runner.rollbackTransaction(); await runner.release(); }
  });
  it('serializes only background indexers and gives each committed event one position', async () => {
    await Promise.all(Array.from({ length: 20 }, (_, i) => post({ ...payload, eventId: `evt-${i}` })));
    await Promise.all([store.indexCommitted(10), store.indexCommitted(10)]);
    const rows = await db.query('SELECT position::text FROM notification_outbox ORDER BY notification_outbox.position');
    expect(rows.map((r: any) => r.position)).toEqual(Array.from({ length: 20 }, (_, i) => String(i + 1)));
  });
  it('claims disjoint retry batches concurrently and applies backoff before another attempt', async () => {
    await Promise.all(Array.from({ length: 20 }, (_, i) => post({ ...payload, eventId: `claim-${i}` })));
    await store.indexCommitted();
    await db.query("UPDATE notification_outbox SET published_at = NULL, next_attempt_at = clock_timestamp() - interval '1 second'");
    const batches = await Promise.all([store.claimPending(10), store.claimPending(10)]);
    const ids = batches.flat().map(row => row.measurement.id);
    expect(ids).toHaveLength(20);
    expect(new Set(ids).size).toBe(20);
    expect(batches.flat().every(row => typeof row.position === 'string')).toBe(true);
    expect((await db.query('SELECT attempts FROM notification_outbox')).every((row: any) => row.attempts === 1)).toBe(true);
    expect(await store.claimPending(20)).toEqual([]);
  });
  it('recovers failed publication and retries safely after failure to record emission', async () => {
    const { messages } = await receiver();
    const gateway = app.get(RealtimeGateway);
    jest.spyOn(gateway, 'publishMeasurement').mockImplementationOnce(() => { throw new Error('private payload'); });
    const body = await (await post()).json();
    await publisher.tick(); expect(messages).toHaveLength(0);
    await db.query("UPDATE notification_outbox SET published_at = NULL, next_attempt_at = clock_timestamp() - interval '1 second'");
    jest.spyOn(store, 'markPublished').mockRejectedValueOnce(new Error('crash after emit'));
    await publisher.tick(); await pause(50); expect(messages.map(m => m.measurement.id)).toEqual([body.id]);
    expect((await db.query('SELECT published_at FROM notification_outbox'))[0].published_at).toBeNull();
    await db.query("UPDATE notification_outbox SET published_at = NULL, next_attempt_at = clock_timestamp() - interval '1 second'");
    await publisher.tick(); await pause(50);
    expect(messages.map(m => m.measurement.id)).toEqual([body.id, body.id]);
    expect((await db.query('SELECT published_at FROM notification_outbox'))[0].published_at).not.toBeNull();
    expect((await post()).status).toBe(200);
    await pause(50); expect(messages).toHaveLength(2);
  });
  it('never deletes pending work and expires only replay data, keeping measurements', async () => {
    for (let i = 0; i < 3; i++) await post({ ...payload, eventId: `retained-${i}` });
    await store.indexCommitted();
    await db.query("UPDATE notification_outbox SET replay_ready_at = clock_timestamp() - interval '25 hours'");
    await db.query('UPDATE notification_outbox SET published_at = NULL WHERE position = 1');
    await store.prune(24);
    expect((await db.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(3);
    const response = await replay(checkpoint('user-123', '0')); expect(response.status).toBe(200);
    await store.markPublished((await db.query('SELECT measurement_id FROM notification_outbox')).map((r: any) => r.measurement_id));
    await store.prune(24);
    expect((await replay(checkpoint('user-123', '0'))).status).toBe(410);
    expect((await db.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(3);
    expect((await (await replay()).json()).events).toEqual([]);
  });
  it.each(['', 'no', checkpoint('other-user', '0'), checkpoint('user-123', '999')])('rejects malformed, other-user, and future checkpoints #%#', async cursor => {
    expect((await replay(cursor)).status).toBe(400);
  });
  it.each(['limit=0','limit=101','limit=1.5','unknown=1','after=x&after=y'])('rejects invalid replay options %s', async query => {
    expect((await replay(undefined, 'user-123', query)).status).toBe(400);
  });
  it('enforces read permissions without revealing whether a forbidden user exists', async () => {
    await post();
    for (const userId of ['user-123', 'does-not-exist']) {
      const response = await replay(undefined, userId, '', deviceToken);
      expect(response.status).toBe(403); expect(await response.json()).toMatchObject({ code: 'PATIENT_ACCESS_DENIED' });
    }
  });
  it('isolates recovery between two clinicians and gives the same denial for unknown patients', async () => {
    await post(); await post({ ...payload, userId: 'user-789' });
    await store.indexCommitted();
    const other = 'other-clinician-test-token';
    for (const [token, userId] of [[clinicianToken, 'user-123'], [other, 'user-789']]) {
      const response = await replay(checkpoint(userId, '0'), userId, '', token);
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.events).toHaveLength(1);
      expect(body.events[0].measurement.userId).toBe(userId);
    }
    for (const [token, forbidden] of [[clinicianToken, 'user-789'], [other, 'user-123']]) {
      const known = await replay(undefined, forbidden, '', token);
      const unknown = await replay(undefined, 'unknown-patient', '', token);
      expect(known.status).toBe(403); expect(unknown.status).toBe(403);
      expect(await known.json()).toEqual(await unknown.json());
    }
  });
  it('delivers without a processing callback or pending acknowledgement', async () => {
    const { socket, messages } = await receiver();
    let argumentsReceived: unknown[] = [];
    socket.on('measurement', (...args: unknown[]) => { argumentsReceived = args; });
    const gateway = app.get(RealtimeGateway) as unknown as { server: { sockets: { sockets: Map<string, { acks: Map<number, unknown> }> } } };
    const serverSocket = gateway.server.sockets.sockets.get(socket.id!)!;
    expect((await post()).status).toBe(201);
    await publisher.tick();
    for (let i = 0; i < 100 && messages.length === 0; i++) await pause(10);
    expect(messages).toHaveLength(1);
    expect(argumentsReceived).toHaveLength(2);
    expect(argumentsReceived.some(value => typeof value === 'function')).toBe(false);
    expect(serverSocket.acks.size).toBe(0);
  });
});
