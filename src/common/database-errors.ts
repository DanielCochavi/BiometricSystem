import { InternalServerErrorException, Logger, ServiceUnavailableException } from '@nestjs/common';
import { QueryFailedError } from 'typeorm';

export function databaseCause(error: unknown): { code?: string; constraint?: string; message?: string } {
  const cause = error instanceof QueryFailedError ? error.driverError : error;
  return cause && typeof cause === 'object' ? cause : {};
}

export function throwDatabaseFailure(error: unknown, logger: Logger): never {
  const cause = databaseCause(error);
  const unavailable = cause.code?.startsWith('08') ||
    ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EHOSTUNREACH',
      '53300', '57P01', '57P02', '57P03', '57014'].includes(cause.code ?? '') ||
    ['Connection terminated unexpectedly', 'Connection terminated',
      'Connection terminated due to connection timeout', 'timeout exceeded when trying to connect'].includes(cause.message ?? '');
  if (unavailable) {
    throw new ServiceUnavailableException({ code: 'DATABASE_UNAVAILABLE', message: 'Measurement storage temporarily unavailable; retry with the same eventId' });
  }
  logger.error('DATABASE_OPERATION_FAILED: Measurement storage operation failed');
  throw new InternalServerErrorException({ code: 'INTERNAL_ERROR', message: 'Measurement storage operation failed' });
}
