import { Socket } from 'socket.io';
import { AuthService } from '../src/auth/auth.service';
import { RealtimeGateway } from '../src/realtime/realtime.gateway';
import { Measurement } from '../src/measurements/measurement.entity';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

describe('RealtimeGateway subscriptions', () => {
  const original = process.env.AUTH_IDENTITIES;
  let auth: AuthService;
  let gateway: RealtimeGateway;
  beforeEach(() => {
    process.env.AUTH_IDENTITIES = authConfig;
    auth = new AuthService();
    gateway = new RealtimeGateway(auth);
  });
  afterEach(() => {
    if (original === undefined) delete process.env.AUTH_IDENTITIES;
    else process.env.AUTH_IDENTITIES = original;
  });

  function client(token?: string) {
    return { data: { principal: token ? auth.authenticate(token) : undefined },
      join: jest.fn().mockResolvedValue(undefined) };
  }

  it('emits the saved measurement exclusively through its patient room', () => {
    const receiver = { data: { principal: { id: 'clinician' } }, id: 'allowed', timeout: jest.fn().mockReturnThis(), emit: jest.fn() };
    const unrelated = { timeout: jest.fn(), emit: jest.fn() };
    const rooms = new Map([['user:user-123', new Set(['allowed'])], ['user:other', new Set(['other'])]]);
    Object.assign(gateway, { server: { sockets: { adapter: { rooms }, sockets: new Map([['allowed', receiver], ['other', unrelated]]) } } });
    const measurement = { userId: 'user-123', eventId: 'evt-1', id: 'saved-id' } as Measurement;
    gateway.publishMeasurement(measurement);
    expect(receiver.emit).toHaveBeenCalledWith('measurement', measurement, { cursor: null });
    expect(unrelated.emit).not.toHaveBeenCalled();
  });

  it('joins an authorized user-specific room', async () => {
    const socket = client(clinicianToken);
    await expect(gateway.subscribe(socket as unknown as Socket, { userId: 'user-123' }))
      .resolves.toEqual({ subscribed: true, userId: 'user-123' });
    expect(socket.join).toHaveBeenCalledWith('user:user-123');
  });

  it.each([undefined, null, [], 'user-123', {}, { userId: ' ' }, { userId: 1 },
    { userId: 'x'.repeat(101) }, { userId: 'user-123', extra: true }])('rejects malformed subscription #%#', async (body) => {
    const socket = client(clinicianToken);
    const response = await gateway.subscribe(socket as unknown as Socket, body);
    expect(response).toMatchObject({ subscribed: false, error: { code: 'INVALID_SUBSCRIPTION' } });
    expect(socket.join).not.toHaveBeenCalled();
  });

  it.each([clinicianToken, deviceToken, undefined])('does not join an unauthorized patient room #%#', async (token) => {
    const socket = client(token);
    const response = await gateway.subscribe(socket as unknown as Socket, { userId: 'other-user' });
    expect(response).toMatchObject({ subscribed: false,
      error: { code: token ? 'PATIENT_ACCESS_DENIED' : 'UNAUTHENTICATED' } });
    expect(socket.join).not.toHaveBeenCalled();
  });
});
