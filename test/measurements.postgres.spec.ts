import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { TypeOrmModule } from '@nestjs/typeorm';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { DataSource, Repository } from 'typeorm';
import dataSource from '../src/database/data-source';
import { createValidationPipe } from '../src/common/validation';
import { Measurement } from '../src/measurements/measurement.entity';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { MeasurementsModule } from '../src/measurements/measurements.module';
import { NotificationStore } from '../src/notifications/notification-store';
import { RealtimeBus } from '../src/realtime/realtime-bus.service';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

function postgresOptions(url: string) {
  if (dataSource.options.type !== 'postgres') throw new Error('These tests require PostgreSQL options');
  return { ...dataSource.options, url };
}

// This suite creates and drops only its own uniquely named database.
// PostgreSQL is required; missing dependencies fail visibly rather than skipping tests.
describe('Measurement ingestion over HTTP with real PostgreSQL', () => {
  const originalAuth = process.env.AUTH_IDENTITIES;
  const databaseName = `biometric_test_${randomUUID().replace(/-/g, '')}`;
  const baseDatabaseUrl = process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview';
  let admin: DataSource;
  let database: DataSource;
  let app: INestApplication;
  let repository: Repository<Measurement>;
  let baseUrl: string;
  let testDatabaseUrl: string;
  let createdDatabase = false;
  const bus = { publish: jest.fn() };
  let publisher: OutboxPublisher;
  const payload = { eventId: 'evt-1', userId: 'user-123', timestamp: '2026-10-20T10:00:00.120Z', heartRate: 80, hrv: 42 };

  beforeAll(async () => {
    const identities = JSON.parse(authConfig);
    identities[0].writeUserIds.push('user-456');
    identities[1].readUserIds.push('user-456');
    process.env.AUTH_IDENTITIES = JSON.stringify(identities);
    admin = new DataSource({ type: 'postgres', url: baseDatabaseUrl, extra: { connectionTimeoutMillis: 3000 } });
    await admin.initialize();
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    createdDatabase = true;
    const testUrl = new URL(baseDatabaseUrl);
    testUrl.pathname = `/${databaseName}`;
    testDatabaseUrl = testUrl.toString();
    const module = await Test.createTestingModule({
      imports: [TypeOrmModule.forRoot({ ...postgresOptions(testDatabaseUrl), retryAttempts: 0 }), MeasurementsModule],
    }).overrideProvider(OutboxPublisher).useValue({ wake() {} }).overrideProvider(RealtimeBus).useValue(bus).compile();
    app = module.createNestApplication({ logger: false });
    app.useGlobalPipes(createValidationPipe());
    database = app.get(DataSource);
    await database.runMigrations();
    repository = database.getRepository(Measurement);
    publisher = new OutboxPublisher(app.get(NotificationStore), app.get(RealtimeBus));
    await app.listen(0, '127.0.0.1');
    baseUrl = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;
  }, 20000);

  beforeEach(async () => {
    await app.get(DataSource).query('TRUNCATE measurements CASCADE');
    bus.publish.mockReset().mockResolvedValue(undefined);
  });

  afterAll(async () => {
    try {
      await publisher?.onModuleDestroy();
      await app?.close();
      if (database?.isInitialized) await database.destroy();
      if (createdDatabase && admin?.isInitialized) await admin.query(`DROP DATABASE "${databaseName}"`);
    } finally {
      if (admin?.isInitialized) await admin.destroy();
      if (originalAuth === undefined) delete process.env.AUTH_IDENTITIES;
      else process.env.AUTH_IDENTITIES = originalAuth;
    }
  });

  async function post(body: unknown = payload, raw = false, drain = true) {
    const response = await fetch(`${baseUrl}/measurements`, { method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${deviceToken}` },
      body: raw ? body as string : JSON.stringify(body),
    });
    if (drain) await publisher.tick();
    return response;
  }

  async function history(query = '', userId = 'user-123') {
    return fetch(`${baseUrl}/measurements/${userId}${query}`, {
      headers: { authorization: `Bearer ${clinicianToken}` },
    });
  }

  it('stores, publishes a committed row, and returns the unchanged measurement response shape', async () => {
    const checker = new DataSource(postgresOptions(testDatabaseUrl));
    await checker.initialize();
    let observedCommittedRow: Measurement | null = null;
    bus.publish.mockImplementation(async (measurement: Measurement) => {
      observedCommittedRow = await checker.getRepository(Measurement).findOneBy({ id: measurement.id });
    });
    try {
      const response = await post();
      expect(response.status).toBe(201);
      const body = await response.json();
      expect(Object.keys(body).sort()).toEqual(['id', 'eventId', 'userId', 'timestamp', 'heartRate', 'hrv', 'createdAt'].sort());
      expect(body).toMatchObject(payload);
      expect(await repository.count()).toBe(1);
      expect(bus.publish).toHaveBeenCalledTimes(1);
      // Assert outside the publication callback: application code deliberately catches publication errors.
      expect(observedCommittedRow).toMatchObject({ id: body.id });
    } finally { await checker.destroy(); }
  });

  it('atomically inserts the measurement and outbox in one command without waiting for publication', async () => {
    const queries = jest.spyOn(database.logger, 'logQuery');
    try {
      const response = await post(payload, false, false);
      expect(response.status).toBe(201);
      const stored = await response.json();
      expect(stored).toMatchObject(payload);
      expect(stored.id).toMatch(/^[0-9a-f-]{36}$/);
      expect(Number.isFinite(Date.parse(stored.createdAt))).toBe(true);
      expect(queries.mock.calls.filter(([sql]) => !sql.includes('AS full')).map(([sql]) => sql.trim().split(/\s+/)[0].toUpperCase()))
        .toEqual(['WITH']);
      expect(queries.mock.calls[0][0]).toContain('INSERT INTO measurements');
      expect(queries.mock.calls[0][0]).toContain('INSERT INTO notification_outbox');
    } finally { queries.mockRestore(); }
  });

  it('normalizes equivalent timestamps and treats missing/null HRV as an identical retry', async () => {
    const { hrv: _, ...withoutHrv } = payload;
    const first = await post(withoutHrv);
    const stored = await first.json();
    expect(first.status).toBe(201);
    expect(stored.hrv).toBeNull();
    const retry = await post({ ...withoutHrv, timestamp: '2026-10-20T12:00:00.120+02:00', hrv: null });
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual(stored);
    expect(await repository.count()).toBe(1);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it('deduplicates 20 concurrent identical HTTP requests with one publication attempt', async () => {
    const responses = await Promise.all(Array.from({ length: 20 }, () => post()));
    expect(responses.filter((response) => response.status === 201)).toHaveLength(1);
    expect(responses.filter((response) => response.status === 200)).toHaveLength(19);
    const bodies = await Promise.all(responses.map((response) => response.json()));
    expect(new Set(bodies.map((body) => body.id)).size).toBe(1);
    expect(await repository.count()).toBe(1);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it.each([{ heartRate: 81 }, { hrv: 43 }, { timestamp: '2026-10-20T10:00:00.121Z' }])('rejects conflicting retries without overwriting #%#', async (change) => {
    const first = await post();
    const stored = await first.json();
    const response = await post({ ...payload, ...change });
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ code: 'EVENT_CONFLICT' });
    expect((await repository.find())[0].id).toBe(stored.id);
    expect(await (await history()).json()).toEqual([stored]);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it('resolves a race between different payloads to one creation and one conflict', async () => {
    const responses = await Promise.all([post(), post({ ...payload, heartRate: 81 })]);
    expect(responses.map((response) => response.status).sort()).toEqual([201, 409]);
    const winner = await responses.find((response) => response.status === 201)!.json();
    expect((await repository.find())[0].heartRate).toBe(winner.heartRate);
    expect(await repository.count()).toBe(1);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it('permits the same eventId for different patients', async () => {
    for (const userId of ['user-123', 'user-456']) {
      const response = await post({ ...payload, userId });
      expect(response.status).toBe(201);
      await response.arrayBuffer();
    }
    expect(await repository.count()).toBe(2);
    expect(bus.publish).toHaveBeenCalledTimes(2);
  });

  it.each([
    { eventId: '' }, { eventId: ' ' }, { eventId: ' evt-1' }, { eventId: 'x'.repeat(101) }, { eventId: 1 },
    { eventId: 'evt\u0000id' }, { eventId: 'evt\nid' }, { eventId: 'evt\ud800' },
    { eventId: 'evt\u0085id' },
    { userId: '' }, { userId: 'user-123 ' }, { userId: 'x'.repeat(101) },
    { userId: 'user\u0000id' }, { userId: 'user\udfff' },
    { timestamp: '2026-02-30T10:00:00Z' }, { timestamp: '2026-10-20' },
    { timestamp: '2026-10-20T10:00:00' }, { timestamp: '2026-10-20T10:00:60Z' },
    { timestamp: '2026-10-20T10:00:00.1234Z' }, { timestamp: '0000-01-01T00:00:00Z' },
    { heartRate: '80' }, { heartRate: null }, { heartRate: 19 }, { heartRate: 251 }, { heartRate: 80.5 },
    { hrv: '42' }, { hrv: '' }, { hrv: -1 }, { unexpected: true },
  ])('rejects invalid measurement #%# before storage/publication', async (change) => {
    const response = await post({ ...payload, ...change });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'VALIDATION_FAILED' });
    expect(await repository.count()).toBe(0);
    expect(bus.publish).not.toHaveBeenCalled();
  });

  it('rejects JSON numeric overflow instead of accepting infinite HRV', async () => {
    const response = await post(JSON.stringify(payload).replace('"hrv":42', '"hrv":1e400'), true);
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'VALIDATION_FAILED' });
    expect(await repository.count()).toBe(0);
  });

  it.each([{}, [], null])('rejects incomplete or malformed measurement bodies #%#', async (body) => {
    const response = await post(body);
    expect(response.status).toBe(400);
    await response.arrayBuffer();
    expect(await repository.count()).toBe(0);
  });

  it.each([20, 250])('accepts heart rate boundary %s and zero HRV', async (heartRate) => {
    const response = await post({ ...payload, heartRate, hrv: 0, eventId: 'x'.repeat(100) });
    expect(response.status).toBe(201);
    expect(await response.json()).toMatchObject({ heartRate, hrv: 0 });
  });

  it('preserves valid Unicode identifiers and their retry identity', async () => {
    const eventId = 'מדידה-🫀';
    const first = await post({ ...payload, eventId });
    expect(first.status).toBe(201);
    const stored = await first.json();
    expect(stored.eventId).toBe(eventId);
    const retry = await post({ ...payload, eventId });
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual(stored);
    expect(bus.publish).toHaveBeenCalledTimes(1);
  });

  it.each(['0', '-1', '101', '1.5', 'no', '', '1e2', 'Infinity', '1&limit=2'])('rejects invalid history limit %s', async (limit) => {
    const response = await history(`?limit=${limit}`);
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'VALIDATION_FAILED' });
  });

  it('rejects unknown history query options', async () => {
    const response = await history('?extra=true');
    expect(response.status).toBe(400);
    await response.arrayBuffer();
  });

  it('returns newest-first bounded history, accepts delayed events, and isolates patients', async () => {
    await repository.save(Array.from({ length: 25 }, (_, index) => repository.create({
      ...payload, eventId: `history-${index}`, timestamp: new Date(Date.UTC(2026, 9, 20, 10, 0, index)),
    })));
    const otherPatient = await post({ ...payload, userId: 'user-456' });
    await otherPatient.arrayBuffer();
    const all = await (await history()).json();
    expect(all).toHaveLength(20);
    expect(all[0].eventId).toBe('history-24');
    expect(all.every((row: Measurement) => row.userId === 'user-123')).toBe(true);
    const limited = await (await history('?limit=1')).json();
    expect(limited).toHaveLength(1);
    expect(limited[0]).toEqual(all[0]);
    expect(await (await history('?limit=100')).json()).toHaveLength(25);
  });

  it('fails the unique-key migration on existing duplicates without discarding data', async () => {
    await database.undoLastMigration(); // outbox
    await database.undoLastMigration(); // uniqueness
    try {
      await repository.save([1, 2].map(() => repository.create({ ...payload, timestamp: new Date(payload.timestamp) })));
      await expect(database.runMigrations()).rejects.toMatchObject({ driverError: { code: '23505' } });
      expect(await repository.count()).toBe(2);
    } finally {
      await app.get(DataSource).query('TRUNCATE measurements CASCADE');
      await database.runMigrations();
    }
  });
});
