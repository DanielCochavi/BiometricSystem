import { BadRequestException } from '@nestjs/common';

export function checkpoint(userId: string, position: string): string {
  return Buffer.from(JSON.stringify([userId, position])).toString('base64url');
}

export function checkpointPosition(userId: string, value: string): string {
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    if (!Array.isArray(parsed) || parsed.length !== 2 || parsed[0] !== userId ||
      typeof parsed[1] !== 'string' || !/^(0|[1-9]\d{0,18})$/.test(parsed[1]) ||
      BigInt(parsed[1]) > 9223372036854775807n || checkpoint(userId, parsed[1]) !== value) throw new Error();
    return parsed[1];
  } catch {
    throw new BadRequestException({ code: 'REPLAY_CURSOR_INVALID', message: 'Invalid checkpoint for this user' });
  }
}
