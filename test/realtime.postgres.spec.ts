import { INestApplication, Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { TypeOrmModule } from '@nestjs/typeorm';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { io, Socket } from 'socket.io-client';
import { DataSource, Repository } from 'typeorm';
import dataSource from '../src/database/data-source';
import { createValidationPipe } from '../src/common/validation';
import { Measurement } from '../src/measurements/measurement.entity';
import { NotificationStore } from '../src/notifications/notification-store';
import { RealtimeBus } from '../src/realtime/realtime-bus.service';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { MeasurementsModule } from '../src/measurements/measurements.module';
import { RealtimeGateway } from '../src/realtime/realtime.gateway';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

type Delivered = Omit<Measurement, 'timestamp' | 'createdAt'> & { timestamp: string; createdAt: string };
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// Real HTTP -> service -> PostgreSQL -> bus -> Socket.IO. Only failure tests inject faults.
describe('Committed measurements delivered to real Socket.IO clients', () => {
  const originalAuth = process.env.AUTH_IDENTITIES;
  const scopedClinicianToken = 'test-single-patient-clinician-123';
  const otherClinicianToken = 'test-single-patient-clinician-456';
  const patientCases = [
    { userId: 'user-123', otherUserId: 'user-456', token: 'test-patient-device-token-123' },
    { userId: 'user-456', otherUserId: 'user-123', token: 'test-patient-device-token-456' },
  ];
  const databaseName = `biometric_realtime_${randomUUID().replace(/-/g, '')}`;
  const sockets: Socket[] = [];
  const messages = new Map<Socket, Delivered[]>();
  const payload = { eventId: 'evt-live', userId: 'user-123', timestamp: '2026-10-20T10:00:00.120Z', heartRate: 80, hrv: 42 };
  let admin: DataSource;
  let checker: DataSource;
  let app: INestApplication;
  let repository: Repository<Measurement>;
  let gateway: RealtimeGateway;
  let publisher: OutboxPublisher;
  let baseUrl: string;
  let createdDatabase = false;

  beforeAll(async () => {
    if (dataSource.options.type !== 'postgres') throw new Error('PostgreSQL required');
    const identities = JSON.parse(authConfig);
    identities[0].writeUserIds.push('user-456', 'user-789');
    identities[1].readUserIds.push('user-456', 'user-789');
    identities.push({ id: 'single-patient-clinician', token: scopedClinicianToken,
      readUserIds: ['user-123'], writeUserIds: [] });
    identities.push({ id: 'other-single-patient-clinician', token: otherClinicianToken,
      readUserIds: ['user-456'], writeUserIds: [] });
    for (const patient of patientCases) {
      identities.push({ id: `patient-${patient.userId}`, token: patient.token,
        readUserIds: [patient.userId], writeUserIds: [patient.userId] });
    }
    process.env.AUTH_IDENTITIES = JSON.stringify(identities);
    const baseDatabaseUrl = process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview';
    admin = new DataSource({ type: 'postgres', url: baseDatabaseUrl,
      extra: { connectionTimeoutMillis: 3000 } });
    await admin.initialize();
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    createdDatabase = true;
    const url = new URL(baseDatabaseUrl);
    url.pathname = `/${databaseName}`;
    const options = { ...dataSource.options, url: url.toString() };
    const module = await Test.createTestingModule({
      imports: [TypeOrmModule.forRoot({ ...options, retryAttempts: 0 }), MeasurementsModule],
    }).overrideProvider(OutboxPublisher).useValue({ wake() {} }).compile();
    app = module.createNestApplication({ logger: false });
    app.useGlobalPipes(createValidationPipe());
    await app.get(DataSource).runMigrations();
    repository = app.get(DataSource).getRepository(Measurement);
    gateway = app.get(RealtimeGateway);
    publisher = new OutboxPublisher(app.get(NotificationStore), app.get(RealtimeBus));
    checker = await new DataSource(options).initialize();
    await app.listen(0, '127.0.0.1');
    baseUrl = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;
  }, 20000);

  beforeEach(async () => { await app.get(DataSource).query('TRUNCATE measurements CASCADE'); });
  afterEach(() => {
    jest.restoreAllMocks();
    sockets.splice(0).forEach((socket) => { socket.removeAllListeners(); socket.disconnect(); });
    messages.clear();
  });
  afterAll(async () => {
    try {
      if (checker?.isInitialized) await checker.destroy();
      await publisher?.onModuleDestroy();
      await app?.close();
      if (createdDatabase && admin?.isInitialized) await admin.query(`DROP DATABASE "${databaseName}"`);
    } finally {
      if (admin?.isInitialized) await admin.destroy();
      if (originalAuth === undefined) delete process.env.AUTH_IDENTITIES;
      else process.env.AUTH_IDENTITIES = originalAuth;
    }
  });

  function connected(socket: Socket): Promise<Socket> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error('Socket connection deadline exceeded')), 3000);
      const onConnect = () => finish();
      const onError = (error: Error) => finish(error);
      function finish(error?: Error) {
        clearTimeout(timer);
        socket.off('connect', onConnect);
        socket.off('connect_error', onError);
        if (error) reject(error); else resolve(socket);
      }
      socket.once('connect', onConnect);
      socket.once('connect_error', onError);
      socket.connect();
    });
  }

  async function connect(token: string | undefined = clinicianToken) {
    const socket = io(baseUrl, { autoConnect: false, forceNew: true, reconnection: false,
      auth: token ? { token } : {}, timeout: 2000 });
    sockets.push(socket);
    const received: Delivered[] = [];
    messages.set(socket, received);
    socket.on('measurement', (measurement: Delivered, _metadata: unknown) => { received.push(measurement); });
    return connected(socket);
  }

  async function subscribe(socket: Socket, userId = 'user-123') {
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId }))
      .toEqual({ subscribed: true, userId });
  }

  async function post(body: unknown = payload, token: string | null = deviceToken) {
    const response = await fetch(`${baseUrl}/measurements`, { method: 'POST',
      headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify(body) });
    await publisher.tick();
    return response;
  }

  async function received(socket: Socket, count: number) {
    const deadline = Date.now() + 3000;
    while (messages.get(socket)!.length < count && Date.now() < deadline) await delay(10);
    expect(messages.get(socket)).toHaveLength(count);
    return messages.get(socket)!;
  }

  async function expectCounts(counts: [Socket, number][]) {
    // Give forbidden/duplicate messages a bounded observation window after HTTP completion.
    await delay(150);
    for (const [socket, count] of counts) expect(messages.get(socket)).toHaveLength(count);
  }

  it('fans out the saved payload to both patient subscribers, never other or unsubscribed clients', async () => {
    const [first, second, other, idle] = await Promise.all([connect(), connect(), connect(), connect()]);
    await Promise.all([subscribe(first), subscribe(second), subscribe(other, 'user-456')]);
    // A failed attempt to join another room must not grant access to that room.
    const device = await connect(deviceToken);
    expect(await device.timeout(2000).emitWithAck('subscribe', { userId: payload.userId }))
      .toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
    let rowAtReceipt: Promise<Measurement | null> | undefined;
    first.once('measurement', (event: Delivered) => {
      rowAtReceipt = checker.getRepository(Measurement).findOneBy({ id: event.id });
    });
    const response = await post();
    expect(response.status).toBe(201);
    const stored = await response.json();
    expect((await received(first, 1))[0]).toEqual(stored);
    expect((await received(second, 1))[0]).toEqual(stored);
    expect(await rowAtReceipt).toMatchObject({ id: stored.id });
    expect(await repository.count()).toBe(1);
    await expectCounts([[first, 1], [second, 1], [other, 0], [idle, 0], [device, 0]]);
  });

  it('accepts a measurement without subscribers and publishes null HRV after a subscription', async () => {
    expect((await post()).status).toBe(201);
    const socket = await connect();
    await subscribe(socket);
    const { hrv: _, ...withoutHrv } = payload;
    const response = await post({ ...withoutHrv, eventId: 'no-hrv' });
    expect(response.status).toBe(201);
    expect((await received(socket, 1))[0]).toEqual(await response.json());
    expect(messages.get(socket)![0].hrv).toBeNull();
    expect(await repository.count()).toBe(2);
    await expectCounts([[socket, 1]]);
  });

  it('deduplicates repeated subscriptions, identical retries, and 20 concurrent submissions', async () => {
    const [first, second] = await Promise.all([connect(), connect()]);
    await subscribe(first);
    await subscribe(first);
    await subscribe(second);
    const responses = await Promise.all(Array.from({ length: 20 }, () => post()));
    expect(responses.filter((r) => r.status === 201)).toHaveLength(1);
    expect(responses.filter((r) => r.status === 200)).toHaveLength(19);
    const bodies = await Promise.all(responses.map((r) => r.json()));
    expect(new Set(bodies.map((body) => body.id)).size).toBe(1);
    expect(await repository.count()).toBe(1);
    expect((await received(first, 1))[0]).toEqual(bodies[0]);
    await received(second, 1);
    expect((await post()).status).toBe(200);
    await expectCounts([[first, 1], [second, 1]]);
  });

  it('publishes only the committed winner of conflicting concurrent submissions', async () => {
    const socket = await connect();
    await subscribe(socket);
    const responses = await Promise.all([post(), post({ ...payload, heartRate: 81 })]);
    expect(responses.map((r) => r.status).sort()).toEqual([201, 409]);
    const winner = await responses.find((r) => r.status === 201)!.json();
    expect((await received(socket, 1))[0]).toEqual(winner);
    expect(await repository.count()).toBe(1);
    expect((await repository.find())[0].heartRate).toBe(winner.heartRate);
    await expectCounts([[socket, 1]]);
  });

  it('isolates the same event ID across patients, including room-like identifiers', async () => {
    const [first, other] = await Promise.all([connect(), connect()]);
    await subscribe(first);
    await subscribe(other, 'user-456');
    for (const userId of ['user-123', 'user-456']) expect((await post({ ...payload, userId })).status).toBe(201);
    expect((await received(first, 1))[0].userId).toBe('user-123');
    expect((await received(other, 1))[0].userId).toBe('user-456');
    // Room names are never interpreted as a broadcast or authorization shortcut.
    expect((await post({ ...payload, userId: 'user:user-123' })).status).toBe(403);
    await expectCounts([[first, 1], [other, 1]]);
  });

  it('rejects bad requests and unauthorized writes without storing or notifying', async () => {
    const socket = await connect();
    await subscribe(socket);
    for (const change of [{ heartRate: '80' }, { timestamp: 'invalid' }, { extra: true }, { hrv: -1 }]) {
      expect((await post({ ...payload, ...change })).status).toBe(400);
    }
    expect((await post(payload, null)).status).toBe(401);
    expect((await post(payload, clinicianToken)).status).toBe(403);
    expect(await repository.count()).toBe(0);
    await expectCounts([[socket, 0]]);
  });

  it('keeps an allowed subscription after denying another patient, without leaking that patient', async () => {
    const socket = await connect(scopedClinicianToken);
    await subscribe(socket);
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: 'user-456' }))
      .toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
    expect((await post({ ...payload, userId: 'user-456' })).status).toBe(201);
    const allowed = await post();
    expect(allowed.status).toBe(201);
    expect((await received(socket, 1))[0]).toEqual(await allowed.json());
    await expectCounts([[socket, 1]]);
  });

  it.each([
    { userId: 'user-123', otherUserId: 'user-456', token: scopedClinicianToken },
    { userId: 'user-456', otherUserId: 'user-123', token: otherClinicianToken },
  ])('restricts therapist history and live updates to their assigned patient $userId', async ({ userId, otherUserId, token }) => {
    const socket = await connect(token);
    await subscribe(socket, userId);
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: otherUserId }))
      .toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });

    const saved: Delivered[] = [];
    for (const patientId of [userId, otherUserId]) {
      const response = await post({ ...payload, userId: patientId });
      expect(response.status).toBe(201);
      saved.push(await response.json());
    }
    const ownHistory = await fetch(`${baseUrl}/measurements/${userId}`, {
      headers: { authorization: `Bearer ${token}` } });
    expect(ownHistory.status).toBe(200);
    expect(await ownHistory.json()).toEqual([saved[0]]);
    const forbiddenHistory = await fetch(`${baseUrl}/measurements/${otherUserId}`, {
      headers: { authorization: `Bearer ${token}` } });
    expect(forbiddenHistory.status).toBe(403);
    expect(await forbiddenHistory.json()).toEqual({ code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' });
    // Read permission must never authorize submitting biometric data, even for the assigned patient.
    expect((await post({ ...payload, userId }, token)).status).toBe(403);
    expect((await post({ ...payload, userId: otherUserId }, token)).status).toBe(403);
    expect((await received(socket, 1))[0]).toEqual(saved[0]);
    expect(await repository.count()).toBe(2);
    await expectCounts([[socket, 1]]);
  });

  it.each(patientCases)('restricts patient/device $userId to its own reads, writes, and live updates', async ({ userId, otherUserId, token }) => {
    const socket = await connect(token);
    await subscribe(socket, userId);
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: otherUserId }))
      .toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
    const own = await post({ ...payload, userId }, token);
    expect(own.status).toBe(201);
    const saved = await own.json();
    const rejected = await post({ ...payload, userId: otherUserId }, token);
    expect(rejected.status).toBe(403);
    expect(await rejected.json()).toEqual({ code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' });
    const other = await post({ ...payload, userId: otherUserId });
    expect(other.status).toBe(201);
    await other.json();
    for (const patientId of [userId, otherUserId]) {
      const response = await fetch(`${baseUrl}/measurements/${patientId}`, {
        headers: { authorization: `Bearer ${token}` } });
      expect(response.status).toBe(patientId === userId ? 200 : 403);
      expect(await response.json()).toEqual(patientId === userId ? [saved] :
        { code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' });
    }
    expect((await received(socket, 1))[0]).toEqual(saved);
    expect(await repository.count()).toBe(2);
    await expectCounts([[socket, 1]]);
  });

  it('delivers every distinct concurrent event only to its patient subscribers', async () => {
    const [first, second, other] = await Promise.all([connect(), connect(), connect()]);
    await Promise.all([subscribe(first), subscribe(second), subscribe(other, 'user-456')]);
    const submissions = Array.from({ length: 20 }, (_, index) => ({ ...payload,
      eventId: `distinct-${index}`, userId: index % 2 ? 'user-456' : 'user-123', heartRate: 70 + index }));
    const responses = await Promise.all(submissions.map((body) => post(body)));
    expect(responses.every((response) => response.status === 201)).toBe(true);
    const saved: Delivered[] = await Promise.all(responses.map((response) => response.json()));
    expect(await repository.count()).toBe(20);
    for (const [socket, userId] of [[first, 'user-123'], [second, 'user-123'], [other, 'user-456']] as const) {
      const delivered = await received(socket, 10);
      expect([...delivered].sort((a, b) => a.eventId.localeCompare(b.eventId)))
        .toEqual(saved.filter((row) => row.userId === userId).sort((a, b) => a.eventId.localeCompare(b.eventId)));
    }
    await expectCounts([[first, 10], [second, 10], [other, 10]]);
  });

  it('continues delivery to remaining subscribers when another subscriber disconnects', async () => {
    const [leaving, remaining] = await Promise.all([connect(), connect()]);
    await Promise.all([subscribe(leaving), subscribe(remaining)]);
    expect((await post()).status).toBe(201);
    await Promise.all([received(leaving, 1), received(remaining, 1)]);
    leaving.disconnect();
    const response = await post({ ...payload, eventId: 'after-other-client-disconnect' });
    expect(response.status).toBe(201);
    expect((await received(remaining, 2))[1]).toEqual(await response.json());
    await expectCounts([[leaving, 1], [remaining, 2]]);
  });

  it.each(['', 'invalid-token'])('rejects invalid handshake credentials #%#', async (token) => {
    await expect(connect(token)).rejects.toMatchObject({ data: { code: 'UNAUTHENTICATED' } });
  });

  it('rejects malformed and forbidden subscriptions without leaking subsequent measurements', async () => {
    const socket = await connect(deviceToken);
    for (const body of [null, {}, { userId: ' ' }, { userId: 'user-123', extra: true }]) {
      expect(await socket.timeout(2000).emitWithAck('subscribe', body))
        .toMatchObject({ subscribed: false, error: { code: 'INVALID_SUBSCRIPTION' } });
    }
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: 'user-123' }))
      .toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
    expect((await post()).status).toBe(201);
    await expectCounts([[socket, 0]]);
  });

  it('returns a safe 503 without notification on storage failure, then accepts the same retry', async () => {
    const socket = await connect();
    await subscribe(socket);
    const insert = jest.spyOn(repository, 'query').mockRejectedValueOnce(
      Object.assign(new Error('private database detail'), { code: '08006' }));
    const failed = await post();
    expect(failed.status).toBe(503);
    expect(await failed.json()).toMatchObject({ code: 'DATABASE_UNAVAILABLE' });
    expect(await repository.count()).toBe(0);
    await expectCounts([[socket, 0]]);
    insert.mockRestore();
    expect((await post()).status).toBe(201);
    await received(socket, 1);
    expect((await post()).status).toBe(200);
    await expectCounts([[socket, 1]]);
  });

  it('preserves committed storage and sanitizes logs if the gateway fails; retries do not republish', async () => {
    const socket = await connect();
    await subscribe(socket);
    const log = jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const publish = jest.spyOn(gateway, 'publishMeasurement').mockImplementationOnce(() => {
      throw new Error('private patient payload');
    });
    const response = await post();
    expect(response.status).toBe(201);
    const stored = await response.json();
    expect(await checker.getRepository(Measurement).findOneBy({ id: stored.id })).not.toBeNull();
    expect(log).toHaveBeenCalledWith('REALTIME_PUBLISH_FAILED: Notification retained for retry');
    expect(JSON.stringify(log.mock.calls)).not.toContain('private patient payload');
    publish.mockRestore();
    expect((await post()).status).toBe(200);
    await expectCounts([[socket, 0]]);
  });

  it('requires resubscription after reconnect and does not replay missed measurements', async () => {
    const socket = await connect();
    await subscribe(socket);
    expect((await post()).status).toBe(201);
    await received(socket, 1);
    socket.disconnect();
    expect((await post({ ...payload, eventId: 'offline' })).status).toBe(201);
    await connected(socket);
    expect((await post({ ...payload, eventId: 'before-resubscribe' })).status).toBe(201);
    await expectCounts([[socket, 1]]);
    await subscribe(socket);
    const response = await post({ ...payload, eventId: 'after-resubscribe' });
    expect(response.status).toBe(201);
    expect((await received(socket, 2))[1]).toEqual(await response.json());
    await expectCounts([[socket, 2]]);
  });

  it('clears previous patient subscriptions when reconnecting with a different therapist identity', async () => {
    const socket = await connect(scopedClinicianToken);
    await subscribe(socket);
    expect((await post()).status).toBe(201);
    await received(socket, 1);
    const previousConnectionId = socket.id;
    socket.disconnect();
    socket.auth = { token: otherClinicianToken };
    await connected(socket);
    expect(socket.id).not.toBe(previousConnectionId);
    expect(await socket.timeout(2000).emitWithAck('subscribe', { userId: 'user-123' }))
      .toEqual({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' } });
    for (const userId of ['user-123', 'user-456']) {
      expect((await post({ ...payload, userId, eventId: 'before-new-subscription' })).status).toBe(201);
    }
    await expectCounts([[socket, 1]]);
    await subscribe(socket, 'user-456');
    expect((await post({ ...payload, eventId: 'old-patient-after-reconnect' })).status).toBe(201);
    const allowed = await post({ ...payload, userId: 'user-456', eventId: 'new-patient-after-reconnect' });
    expect(allowed.status).toBe(201);
    expect((await received(socket, 2))[1]).toEqual(await allowed.json());
    await expectCounts([[socket, 2]]);
  });

  it('delivers multiple subscribed patients without including another assigned but unsubscribed patient', async () => {
    const socket = await connect();
    await subscribe(socket, 'user-123');
    const first = await post();
    expect(first.status).toBe(201);
    const expected: Delivered[] = [await first.json()];
    expect((await post({ ...payload, userId: 'user-456' })).status).toBe(201);
    await received(socket, 1);
    await expectCounts([[socket, 1]]);
    await subscribe(socket, 'user-456');
    for (const userId of ['user-123', 'user-456', 'user-789']) {
      const response = await post({ ...payload, userId, eventId: 'after-second-subscription' });
      expect(response.status).toBe(201);
      const saved = await response.json();
      if (userId !== 'user-789') expected.push(saved);
    }
    expect(await received(socket, 3)).toEqual(expected);
    expect(await repository.count()).toBe(5);
    await expectCounts([[socket, 3]]);
  });

  it('does not reveal whether an unauthorized patient has stored measurements', async () => {
    const other = await post({ ...payload, userId: 'user-456' });
    expect(other.status).toBe(201);
    await other.json();
    expect(await repository.countBy({ userId: 'user-456' })).toBe(1);
    expect(await repository.countBy({ userId: 'unknown-patient' })).toBe(0);
    const socket = await connect(scopedClinicianToken);
    await subscribe(socket);
    const denied = { code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' };
    for (const userId of ['user-456', 'unknown-patient']) {
      const response = await fetch(`${baseUrl}/measurements/${userId}`, {
        headers: { authorization: `Bearer ${scopedClinicianToken}` } });
      expect(response.status).toBe(403);
      expect(await response.json()).toEqual(denied);
      expect(await socket.timeout(2000).emitWithAck('subscribe', { userId }))
        .toEqual({ subscribed: false, error: denied });
    }
    const ownHistory = await fetch(`${baseUrl}/measurements/user-123`, {
      headers: { authorization: `Bearer ${scopedClinicianToken}` } });
    expect(ownHistory.status).toBe(200);
    expect(await ownHistory.json()).toEqual([]);
    expect((await post({ ...payload, userId: 'user-456', eventId: 'after-enumeration-attempt' })).status).toBe(201);
    await expectCounts([[socket, 0]]);
  });

  it('delivers delayed measurements while history remains ordered by measurement timestamp', async () => {
    const socket = await connect();
    await subscribe(socket);
    expect((await post()).status).toBe(201);
    await received(socket, 1);
    const delayed = { ...payload, eventId: 'delayed', timestamp: '2025-01-01T00:00:00.000Z' };
    expect((await post(delayed)).status).toBe(201);
    expect((await received(socket, 2))[1]).toMatchObject(delayed);
    const history = await fetch(`${baseUrl}/measurements/user-123`, {
      headers: { authorization: `Bearer ${clinicianToken}` } });
    expect(history.status).toBe(200);
    expect((await history.json()).map((row: Delivered) => row.eventId)).toEqual([payload.eventId, 'delayed']);
  });
});
