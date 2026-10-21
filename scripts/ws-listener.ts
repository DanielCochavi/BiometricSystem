import { io } from 'socket.io-client';
import { createHash } from 'node:crypto';
import { mkdir, open, readFile, unlink } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { ReceivingState } from '../src/realtime/receiving-state';
import { saveReceiver } from '../src/realtime/receiver-file';
import { checkpointPosition } from '../src/notifications/checkpoint';
import { isIdentifier } from '../src/common/validation';

const userId = process.argv[2] ?? 'user-123';
const name = process.argv[3] ?? 'default';
const socketUrl = process.env.SOCKET_URL ?? 'http://localhost:3000';
const token = process.env.SOCKET_TOKEN;
if (!token || !isIdentifier(userId) || !/^[a-zA-Z0-9_-]{1,100}$/.test(name)) throw new Error('Valid userId, listener name, and SOCKET_TOKEN required');
const key = createHash('sha256').update(JSON.stringify([socketUrl, token, userId, name])).digest('hex');
const path = resolve(process.env.LISTENER_STATE_FILE ?? `.runtime/receivers/${key}.json`);
const state = new ReceivingState(userId);
let lock: Awaited<ReturnType<typeof open>> | undefined;
let closed = false;
let recoveryExpired = false;
let historyLoaded = false;
let recovering: Promise<void> | undefined;
let timer: NodeJS.Timeout | undefined;
const socket = io(socketUrl, { autoConnect: false, auth: { token } });

async function save(cursor: string): Promise<void> {
  await saveReceiver(path, state, cursor);
}

async function get(route: string) {
  const response = await fetch(new URL(route, socketUrl), {
    headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10000) });
  const body = await response.json();
  if (!response.ok) {
    if (response.status === 410) recoveryExpired = true;
    throw new Error(String(body.code ?? `HTTP_${response.status}`));
  }
  return body;
}

function apply(measurement: unknown, cursor?: string | null): void {
  if (state.apply(measurement, cursor)) console.log('measurement:', measurement);
}

async function recover(): Promise<void> {
  if (recovering || closed || recoveryExpired) return recovering;
  recovering = (async () => {
    const baseline = state.cursor ?? (await get(`/measurements/${encodeURIComponent(userId)}/events`)).nextCursor;
    checkpointPosition(userId, baseline);
    if (!historyLoaded) {
      const history = await get(`/measurements/${encodeURIComponent(userId)}?limit=100`);
      for (const measurement of history) apply(measurement);
      historyLoaded = true;
    }
    if (!state.cursor) await save(baseline);
    // Live handlers remain active between pages and while network/disk operations are awaited.
    for (let page = 0; page < 10 && !closed; page++) {
      const result = await get(`/measurements/${encodeURIComponent(userId)}/events?after=${encodeURIComponent(state.cursor!)}&limit=100`);
      checkpointPosition(userId, result.nextCursor);
      for (const event of result.events) apply(event.measurement, event.cursor);
      await save(result.nextCursor);
      if (!result.hasMore) return;
    }
  })().catch(error => console.error(recoveryExpired ? 'REPLAY_EXPIRED: Refresh history; use a new listener name to explicitly reset recovery' : `REPLAY_FAILED: ${error.message}`));
  try { await recovering; } finally { recovering = undefined; }
}

socket.on('connect', () => {
  console.log(`connected: ${socket.id}`);
  socket.emit('subscribe', { userId }, (ack: { subscribed?: boolean }) => {
    console.log('subscription ack:', ack);
    if (ack.subscribed) void recover();
  });
});
socket.on('measurement', (measurement: unknown, metadata: { cursor?: string | null } | undefined) => {
  try {
    apply(measurement, metadata?.cursor);
  } catch { console.error('RECEIVED_MEASUREMENT_INVALID'); }
});
socket.on('connect_error', () => console.error('CONNECT_FAILED: Check server and credentials'));

async function start() {
  await mkdir(dirname(path), { recursive: true });
  try { lock = await open(`${path}.lock`, 'wx', 0o600); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    const pid = Number(await readFile(`${path}.lock`, 'utf8'));
    if (!Number.isInteger(pid) || pid <= 0) throw new Error('LISTENER_STATE_LOCK_INVALID');
    try { process.kill(pid, 0); throw new Error('LISTENER_ALREADY_RUNNING: Use a different listener name'); }
    catch (failure) { if ((failure as NodeJS.ErrnoException).code !== 'ESRCH') throw failure; }
    await unlink(`${path}.lock`);
    lock = await open(`${path}.lock`, 'wx', 0o600);
  }
  await lock.writeFile(String(process.pid)); await lock.sync();
  try {
    const stored = JSON.parse(await readFile(path, 'utf8'));
    state.restore(stored);
    historyLoaded = true;
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  socket.auth = { token };
  // Capture a first-use checkpoint before subscribing; replay covers the join race.
  await recover();
  socket.connect();
  timer = setInterval(() => { if (socket.connected) void recover(); }, 1000);
}

async function stop() {
  if (closed) return;
  closed = true;
  clearInterval(timer);
  socket.disconnect();
  await recovering;
  if (lock) { await lock.close(); lock = undefined; await unlink(`${path}.lock`); }
}
for (const signal of ['SIGINT', 'SIGTERM'] as const) process.once(signal, () => { void stop(); });
void start().catch(async error => { console.error(error.message); process.exitCode = 1; await stop(); });
