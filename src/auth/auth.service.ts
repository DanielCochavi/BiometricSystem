import { ForbiddenException, Injectable, UnauthorizedException } from '@nestjs/common';
import { createHash, timingSafeEqual } from 'node:crypto';
import { isIdentifier } from '../common/validation';

export interface Principal {
  readonly id: string;
  readonly readUserIds: readonly string[];
  readonly writeUserIds: readonly string[];
}

export const isPatientId = isIdentifier;

@Injectable()
export class AuthService {
  private readonly identities: { digest: Buffer; principal: Principal }[];

  constructor() {
    this.identities = this.loadIdentities(process.env.AUTH_IDENTITIES);
  }

  authenticate(token: unknown): Principal {
    if (typeof token !== 'string' || !token || token.length > 1024) {
      throw this.unauthorized();
    }
    const digest = createHash('sha256').update(token).digest();
    const identity = this.identities.find((entry) => timingSafeEqual(entry.digest, digest));
    if (!identity) throw this.unauthorized();
    return identity.principal;
  }

  requirePermission(principal: Principal | undefined, userId: unknown, action: 'read' | 'write'): void {
    if (!principal) throw this.unauthorized();
    const allowed = action === 'read' ? principal.readUserIds : principal.writeUserIds;
    if (typeof userId !== 'string' || !allowed.includes(userId)) {
      throw new ForbiddenException({ code: 'PATIENT_ACCESS_DENIED', message: 'Patient access denied' });
    }
  }

  private unauthorized(): UnauthorizedException {
    return new UnauthorizedException({ code: 'UNAUTHENTICATED', message: 'Valid credentials required' });
  }

  private loadIdentities(raw: string | undefined): { digest: Buffer; principal: Principal }[] {
    const invalid = () => new Error('AUTH_CONFIG_INVALID: AUTH_IDENTITIES must be a nonempty array of unique identities with valid tokens and explicit patient permission lists');
    let entries: unknown;
    try {
      entries = JSON.parse(raw ?? '');
    } catch {
      throw invalid();
    }
    if (!Array.isArray(entries) || entries.length === 0) throw invalid();
    const ids = new Set<string>();
    const tokens = new Set<string>();
    return entries.map((entry: unknown) => {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw invalid();
      const value = entry as Record<string, unknown>;
      if (Object.keys(value).some((key) => !['id', 'token', 'readUserIds', 'writeUserIds'].includes(key))) throw invalid();
      const { id, token, readUserIds, writeUserIds } = value;
      if (!isPatientId(id) || typeof token !== 'string' || !/^[A-Za-z0-9._~+\/-]{16,1024}$/.test(token)) throw invalid();
      if (!Array.isArray(readUserIds) || !readUserIds.every(isPatientId) ||
          !Array.isArray(writeUserIds) || !writeUserIds.every(isPatientId)) throw invalid();
      if (ids.has(id) || tokens.has(token)) throw invalid();
      ids.add(id);
      tokens.add(token);
      return {
        digest: createHash('sha256').update(token).digest(),
        principal: Object.freeze({
          id,
          readUserIds: Object.freeze([...readUserIds]),
          writeUserIds: Object.freeze([...writeUserIds]),
        }),
      };
    });
  }
}
