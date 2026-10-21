import { ForbiddenException, UnauthorizedException } from '@nestjs/common';
import { AuthService } from '../src/auth/auth.service';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

describe('AuthService', () => {
  const original = process.env.AUTH_IDENTITIES;
  beforeEach(() => { process.env.AUTH_IDENTITIES = authConfig; });
  afterEach(() => {
    if (original === undefined) delete process.env.AUTH_IDENTITIES;
    else process.env.AUTH_IDENTITIES = original;
  });

  it('authenticates configured identities with explicit, immutable permissions', () => {
    const auth = new AuthService();
    const principal = auth.authenticate(clinicianToken);
    expect(principal.id).toBe('clinician');
    expect(Object.isFrozen(principal.readUserIds)).toBe(true);
    expect(() => auth.requirePermission(principal, 'user-123', 'read')).not.toThrow();
    expect(() => auth.requirePermission(principal, 'user-123', 'write')).toThrow(ForbiddenException);
    expect(() => auth.requirePermission(principal, 'user-other', 'read')).toThrow(ForbiddenException);
    const device = auth.authenticate(deviceToken);
    expect(() => auth.requirePermission(device, 'user-123', 'write')).not.toThrow();
    expect(() => auth.requirePermission(device, 'user-123', 'read')).toThrow(ForbiddenException);
  });

  it.each([undefined, null, '', 'wrong-token', 123, {}, 'x'.repeat(1025)])('rejects invalid credentials #%#', (token) => {
    expect(() => new AuthService().authenticate(token)).toThrow(UnauthorizedException);
  });

  it('denies missing identity and nonmatching patient IDs by default', () => {
    const auth = new AuthService();
    expect(() => auth.requirePermission(undefined, 'user-123', 'read')).toThrow(UnauthorizedException);
    expect(() => auth.requirePermission(auth.authenticate(clinicianToken), 'user-123 ', 'read')).toThrow(ForbiddenException);
  });

  it.each([
    undefined, '', 'not-json', 'null', '{}', '[]', '[null]',
    JSON.stringify([{ id: 'device', token: deviceToken, readUserIds: [] }]),
    JSON.stringify([{ id: 'device', token: deviceToken, readUserIds: [''], writeUserIds: [] }]),
    JSON.stringify([{ id: 'device', token: deviceToken, readUserIds: ['user\u0000id'], writeUserIds: [] }]),
    JSON.stringify([{ id: 'device', token: deviceToken, readUserIds: ['user\udfff'], writeUserIds: [] }]),
    JSON.stringify([{ id: 'device', token: 'short', readUserIds: [], writeUserIds: [] }]),
    JSON.stringify([{ id: 'device', token: deviceToken, readUserIds: [], writeUserIds: [], extra: true }]),
    JSON.stringify([{ id: 'one', token: deviceToken, readUserIds: [], writeUserIds: [] },
      { id: 'two', token: deviceToken, readUserIds: [], writeUserIds: [] }]),
    JSON.stringify([{ id: 'one', token: deviceToken, readUserIds: [], writeUserIds: [] },
      { id: 'one', token: clinicianToken, readUserIds: [], writeUserIds: [] }]),
  ])('fails closed on malformed configuration #%#', (config) => {
    if (config === undefined) delete process.env.AUTH_IDENTITIES;
    else process.env.AUTH_IDENTITIES = config;
    expect(() => new AuthService()).toThrow('AUTH_CONFIG_INVALID');
  });

  it('does not expose credential contents in configuration errors', () => {
    process.env.AUTH_IDENTITIES = `secret-${deviceToken}`;
    try {
      new AuthService();
      throw new Error('Expected configuration rejection');
    } catch (error) {
      expect((error as Error).message).not.toContain(deviceToken);
      expect((error as Error).message).toContain('AUTH_CONFIG_INVALID');
    }
  });
});
