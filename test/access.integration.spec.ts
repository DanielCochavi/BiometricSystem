import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { AddressInfo } from 'node:net';
import { io, Socket } from 'socket.io-client';
import { AuthModule } from '../src/auth/auth.module';
import { NotificationStore } from '../src/notifications/notification-store';
import { MeasurementsController } from '../src/measurements/measurements.controller';
import { MeasurementsService } from '../src/measurements/measurements.service';
import { RealtimeModule } from '../src/realtime/realtime.module';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';
import { createValidationPipe } from '../src/common/validation';

// Focused access-control tests use real HTTP/Socket.IO transports and stub business/storage.
describe('Access control over HTTP and Socket.IO', () => {
  const original = process.env.AUTH_IDENTITIES;
  let app: INestApplication;
  let baseUrl: string;
  const sockets: Socket[] = [];
  const measurements = {
    ingest: jest.fn().mockResolvedValue({ measurement: { id: 'accepted-by-stub' }, created: true }),
    findRecent: jest.fn().mockResolvedValue([]),
  };
  const payload = { eventId: 'evt-1', userId: 'user-123', timestamp: '2026-10-20T10:00:00Z', heartRate: 80 };

  beforeAll(async () => {
    process.env.AUTH_IDENTITIES = authConfig;
    const module = await Test.createTestingModule({
      imports: [AuthModule, RealtimeModule],
      controllers: [MeasurementsController],
      providers: [{ provide: MeasurementsService, useValue: measurements }, { provide: NotificationStore, useValue: { replay: jest.fn() } }],
    }).compile();
    app = module.createNestApplication({ logger: false });
    app.useGlobalPipes(createValidationPipe());
    await app.listen(0, '127.0.0.1');
    baseUrl = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;
  });
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => sockets.splice(0).forEach((socket) => socket.disconnect()));
  afterAll(async () => {
    await app?.close();
    if (original === undefined) delete process.env.AUTH_IDENTITIES;
    else process.env.AUTH_IDENTITIES = original;
  });

  function request(method: string, path: string, token?: string, body?: unknown) {
    return fetch(`${baseUrl}${path}`, { method,
      headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  }

  it.each([undefined, 'invalid-token'])('rejects unauthenticated HTTP reads and writes #%#', async (token) => {
    for (const response of [await request('GET', '/measurements/user-123', token),
      await request('POST', '/measurements', token, payload)]) {
      expect(response.status).toBe(401);
      expect(await response.json()).toMatchObject({ code: 'UNAUTHENTICATED' });
    }
    expect(measurements.ingest).not.toHaveBeenCalled();
    expect(measurements.findRecent).not.toHaveBeenCalled();
  });

  it('permits a clinician read and a device write for the configured patient', async () => {
    expect((await request('GET', '/measurements/user-123', clinicianToken)).status).toBe(200);
    expect(measurements.findRecent).toHaveBeenCalledWith('user-123', 20);
    expect((await request('POST', '/measurements', deviceToken, payload)).status).toBe(201);
    expect(measurements.ingest).toHaveBeenCalledWith(payload);
  });

  it('denies wrong patients and wrong permission types before invoking business logic', async () => {
    for (const response of [
      await request('GET', '/measurements/other-user', clinicianToken),
      await request('GET', '/measurements/user-123', deviceToken),
      await request('POST', '/measurements', clinicianToken, payload),
      await request('POST', '/measurements', deviceToken, { ...payload, userId: 'other-user' }),
    ]) {
      expect(response.status).toBe(403);
      expect(await response.json()).toMatchObject({ code: 'PATIENT_ACCESS_DENIED' });
    }
    expect(measurements.ingest).not.toHaveBeenCalled();
    expect(measurements.findRecent).not.toHaveBeenCalled();
  });

  function connect(token?: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, { autoConnect: false, auth: token ? { token } : {},
        reconnection: false, timeout: 2000 });
      sockets.push(socket);
      socket.once('connect', () => resolve(socket));
      socket.once('connect_error', reject);
      socket.connect();
    });
  }

  it.each([undefined, 'invalid-token'])('rejects unauthenticated socket handshakes #%#', async (token) => {
    await expect(connect(token)).rejects.toMatchObject({ data: { code: 'UNAUTHENTICATED' } });
  });

  it('acknowledges allowed subscriptions and denies malformed or unauthorized ones', async () => {
    const socket = await connect(clinicianToken);
    await expect(socket.timeout(2000).emitWithAck('subscribe', { userId: 'user-123' }))
      .resolves.toEqual({ subscribed: true, userId: 'user-123' });
    await expect(socket.timeout(2000).emitWithAck('subscribe', { userId: 'other-user' }))
      .resolves.toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
    await expect(socket.timeout(2000).emitWithAck('subscribe', null))
      .resolves.toMatchObject({ subscribed: false, error: { code: 'INVALID_SUBSCRIPTION' } });
    const device = await connect(deviceToken);
    await expect(device.timeout(2000).emitWithAck('subscribe', { userId: 'user-123' }))
      .resolves.toMatchObject({ subscribed: false, error: { code: 'PATIENT_ACCESS_DENIED' } });
  });
});
