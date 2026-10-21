import { fork, ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createServer as tcpServer, AddressInfo } from 'node:net';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DataSource } from 'typeorm';
import { io, Socket } from 'socket.io-client';
import source from '../src/database/data-source';
import { checkpoint } from '../src/notifications/checkpoint';
import { ReceivingState } from '../src/realtime/receiving-state';
import { authConfig, clinicianToken, deviceToken } from './auth.fixture';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn: () => boolean | Promise<boolean>, label: string, timeout = 15000) {
  const end = Date.now() + timeout; while (Date.now() < end) { if (await fn()) return; await delay(20); } throw new Error(`Deadline: ${label}`);
}
async function freePort() { const server = tcpServer(); await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve)); const port = (server.address() as AddressInfo).port; await new Promise<void>(resolve => server.close(() => resolve())); return port; }
type Tracked = ChildProcess & { messages: any[]; output: string };
function child(file: string, env: NodeJS.ProcessEnv): Tracked {
  const processChild = fork(file, [], { execArgv: file.endsWith('.ts') ? ['-r', 'ts-node/register/transpile-only'] : [], env: { ...process.env, ...env }, stdio: ['ignore','pipe','pipe','ipc'] }) as Tracked;
  processChild.messages = []; processChild.output = '';
  processChild.on('message', m => processChild.messages.push(m));
  processChild.stdout!.on('data', b => { processChild.output = (processChild.output + b).slice(-10000); });
  processChild.stderr!.on('data', b => { processChild.output = (processChild.output + b).slice(-10000); });
  return processChild;
}
async function stop(proc: Tracked, signal: NodeJS.Signals = 'SIGTERM') {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  proc.kill(signal);
  try { await until(() => proc.exitCode !== null || proc.signalCode !== null, 'child stopped', 2000); }
  catch {
    // A fault-injection gate may deliberately prevent graceful shutdown.
    proc.kill('SIGKILL');
    await until(() => proc.exitCode !== null || proc.signalCode !== null, 'gated child killed', 2000);
  }
}

describe('Real process crash boundaries and receiving-client recovery', () => {
  const name = `biometric_crash_${randomUUID().replaceAll('-', '')}`;
  const children: Tracked[] = [], sockets: Socket[] = [];
  const payload = { eventId: 'crash-event', userId: 'user-123', timestamp: '2026-10-20T10:00:00Z', heartRate: 80, hrv: 42 };
  let admin: DataSource, checker: DataSource, databaseUrl: string, port: number, base: string, folder: string;
  let created = false;
  beforeAll(async () => {
    if (source.options.type !== 'postgres') throw new Error('PostgreSQL required');
    const parent = process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview';
    admin = await new DataSource({ type: 'postgres', url: parent }).initialize();
    await admin.query(`CREATE DATABASE "${name}"`); created = true;
    const url = new URL(parent); url.pathname = `/${name}`; databaseUrl = url.toString();
    checker = await new DataSource({ ...source.options, url: databaseUrl }).initialize(); await checker.runMigrations();
    folder = await mkdtemp(join(tmpdir(), 'biometric-process-'));
  }, 20000);
  beforeEach(async () => {
    port = await freePort(); base = `http://127.0.0.1:${port}`;
    await checker.query('TRUNCATE measurements CASCADE'); await checker.query('TRUNCATE notification_replay_floor');
  });
  afterEach(async () => { sockets.splice(0).forEach(s => s.disconnect()); for (const proc of children.splice(0)) await stop(proc); });
  afterAll(async () => {
    if (checker?.isInitialized) await checker.destroy();
    if (created) await admin.query(`DROP DATABASE "${name}"`);
    await admin?.destroy(); if (folder) await rm(folder, { recursive: true, force: true });
  });
  async function backend(boundary = '') {
    const proc = child('test/fixtures/phase5-server.cjs', { PORT: String(port), DATABASE_URL: databaseUrl, AUTH_IDENTITIES: authConfig,
      CRASH_BOUNDARY: boundary }); children.push(proc);
    await until(() => { if (proc.exitCode !== null) throw new Error(proc.output); return proc.messages.some(m => m.ready); }, 'backend startup');
    return proc;
  }
  function post(body = payload) { return fetch(`${base}/measurements`, { method: 'POST', headers: { authorization: `Bearer ${deviceToken}`, 'content-type': 'application/json' }, body: JSON.stringify(body) }); }
  async function listener(path: string) {
    const proc = child('scripts/ws-listener.ts', { SOCKET_URL: base, SOCKET_TOKEN: clinicianToken, LISTENER_STATE_FILE: path });
    children.push(proc);
    await until(() => { if (proc.exitCode !== null) throw new Error(proc.output); return /subscribed: true/.test(proc.output); }, 'listener subscription');
    return proc;
  }
  async function receive(state: ReceivingState) {
    const socket = io(base, { auth: { token: clinicianToken }, reconnection: false, transports: ['websocket'] }); sockets.push(socket);
    const applied: boolean[] = [];
    socket.on('measurement', (m, meta) => { applied.push(state.apply(m, meta?.cursor)); });
    await new Promise<void>((resolve, reject) => { socket.once('connect', resolve); socket.once('connect_error', reject); });
    await socket.timeout(2000).emitWithAck('subscribe', { userId: payload.userId });
    return applied;
  }
  async function replay() {
    return (await fetch(`${base}/measurements/user-123/events?after=${checkpoint('user-123','0')}`, { headers: { authorization: `Bearer ${clinicianToken}` } })).json();
  }
  it.each(['committed','emitted'])('recovers a real SIGKILL at the %s boundary without duplicate business state', async boundary => {
    const first = await backend(boundary); const state = new ReceivingState('user-123'); const applied = await receive(state);
    const request = post().catch(() => undefined);
    await until(() => first.messages.some(m => m.boundary === boundary), 'crash boundary');
    if (boundary === 'emitted') await until(() => applied.length === 1, 'initial live receipt');
    await stop(first, 'SIGKILL'); await request;
    expect((await checker.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(1);
    expect((await checker.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(1);
    await backend(); await receive(state);
    await until(async () => (await checker.query('SELECT published_at, position FROM notification_outbox'))[0]?.published_at != null, 'publication recovery');
    const data = await replay(); expect(data.events).toHaveLength(1);
    for (const e of data.events) state.apply(e.measurement, e.cursor);
    expect(state.measurements.size).toBe(1);
    expect((await post()).status).toBe(200);
    if (boundary === 'emitted') expect(state.apply(data.events[0].measurement, data.events[0].cursor)).toBe(false);
  }, 30000);
  it('polls durable pending work when the post-commit wake-up is lost', async () => {
    await backend('missed-wake');
    const state = new ReceivingState('user-123');
    const applied = await receive(state);
    expect((await post()).status).toBe(201);
    await until(() => applied.length === 1, 'fallback delivery without wake-up');
    await until(async () => (await checker.query('SELECT published_at FROM notification_outbox'))[0].published_at != null, 'fallback publication recorded');
    expect(state.measurements.size).toBe(1);
    await delay(600);
    expect(applied).toEqual([true]);
  }, 30000);
  it('rolls back both rows when the database transaction is terminated before commit', async () => {
    const guard = checker.createQueryRunner(); await guard.connect();
    const key = 1000000000 + Math.floor(Math.random() * 100000000);
    await guard.query('SELECT pg_advisory_lock($1)', [key]);
    await checker.query(`CREATE FUNCTION wait_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(${key}); RETURN NEW; END $$`);
    await checker.query('CREATE TRIGGER wait_outbox BEFORE INSERT ON notification_outbox FOR EACH ROW EXECUTE FUNCTION wait_outbox()');
    try {
      const proc = await backend(); const request = post().catch(() => undefined);
      await until(async () => (await admin.query("SELECT pid FROM pg_stat_activity WHERE datname=$1 AND wait_event='advisory'", [name])).length === 1, 'transaction waiting before commit');
      const [waiting] = await admin.query("SELECT pid FROM pg_stat_activity WHERE datname=$1 AND wait_event='advisory'", [name]);
      await stop(proc, 'SIGKILL'); await admin.query('SELECT pg_terminate_backend($1)', [waiting.pid]); await request;
      expect((await checker.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(0);
      expect((await checker.query('SELECT count(*)::int AS n FROM notification_outbox'))[0].n).toBe(0);
    } finally {
      await guard.query('SELECT pg_advisory_unlock($1)', [key]); await guard.release();
      await checker.query('DROP TRIGGER wait_outbox ON notification_outbox'); await checker.query('DROP FUNCTION wait_outbox()');
    }
  }, 30000);
  it('finishes an admitted write during SIGTERM and recovers its notification on restart', async () => {
    const guard = checker.createQueryRunner(); await guard.connect();
    const key = 1600000000 + Math.floor(Math.random() * 10000000);
    await guard.query('SELECT pg_advisory_lock($1)', [key]);
    await checker.query(`CREATE FUNCTION drain_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(${key}); RETURN NEW; END $$`);
    await checker.query('CREATE TRIGGER drain_write BEFORE INSERT ON notification_outbox FOR EACH ROW EXECUTE FUNCTION drain_write()');
    const server = await backend();
    const request = post();
    try {
      await until(async () => (await admin.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=$1 AND wait_event='advisory'", [name]))[0].n === 1, 'write held before commit');
      server.kill('SIGTERM');
      await delay(100);
      const rejected = await post({ ...payload, eventId: 'rejected-during-shutdown' });
      expect(rejected.status).toBe(503); expect(await rejected.json()).toMatchObject({ code: 'SERVER_DRAINING' });
      await guard.query('SELECT pg_advisory_unlock($1)', [key]);
      const response = await request; expect(response.status).toBe(201);
      const saved = await response.json();
      await until(() => server.exitCode !== null || server.signalCode !== null, 'graceful process stop');
      expect(server.signalCode).not.toBe('SIGKILL');
      expect((await checker.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(1);
      await backend();
      await until(async () => (await checker.query('SELECT published_at FROM notification_outbox'))[0].published_at != null, 'queued notification after shutdown');
      const recovered = await replay(); expect(recovered.events.map((e: any) => e.measurement.id)).toEqual([saved.id]);
      expect((await post()).status).toBe(200);
    } finally {
      await guard.query('SELECT pg_advisory_unlock($1)', [key]); await request.catch(() => undefined);
      await checker.query('DROP TRIGGER drain_write ON notification_outbox'); await checker.query('DROP FUNCTION drain_write()'); await guard.release();
    }
  }, 30000);
  it('waits for an active publisher during SIGTERM and preserves replay state', async () => {
    const server = await backend('committed');
    const response = await post(); expect(response.status).toBe(201); const saved = await response.json();
    await until(() => server.messages.some(m => m.boundary === 'committed'), 'publisher paused');
    server.kill('SIGTERM'); await delay(100);
    expect(server.exitCode).toBeNull(); expect(server.signalCode).toBeNull();
    server.send({ release: true });
    await until(() => server.exitCode !== null || server.signalCode !== null, 'publisher drained');
    expect(server.signalCode).not.toBe('SIGKILL');
    expect((await checker.query('SELECT published_at FROM notification_outbox'))[0].published_at).not.toBeNull();
    await backend(); const data = await replay();
    expect(data.events.map((e: any) => e.measurement.id)).toEqual([saved.id]);
  }, 30000);
  it('automatically catches up after a real listener restart without displaying the first event twice', async () => {
    await backend();
    const path = join(folder, `listener-${randomUUID()}.json`);
    const first = await listener(path);
    const saved = await (await post()).json();
    await until(() => first.output.includes(saved.id), 'initial listener receipt');
    await until(async () => JSON.parse(await readFile(path, 'utf8')).measurements.some((m: any) => m.id === saved.id), 'durable receiver state');
    await stop(first);
    const missed = await (await post({ ...payload, eventId: 'missed-during-listener-restart' })).json();
    const resumed = await listener(path);
    await until(() => resumed.output.includes(missed.id), 'automatic missed event recovery');
    expect(resumed.output).not.toContain(saved.id);
    await until(async () => {
      const stored = JSON.parse(await readFile(path, 'utf8'));
      return stored.measurements.some((m: any) => m.id === missed.id);
    }, 'recovered state saved');
    const stored = JSON.parse(await readFile(path, 'utf8'));
    expect(new Set(stored.measurements.map((m: any) => m.id)).size).toBe(2);
  }, 30000);
  it('automatically recovers a missed event after reconnecting to a restarted backend', async () => {
    const server = await backend();
    const path = join(folder, `reconnect-${randomUUID()}.json`);
    const receiving = await listener(path);
    const first = await (await post()).json();
    await until(() => receiving.output.includes(first.id), 'first live event');
    await until(async () => JSON.parse(await readFile(path, 'utf8')).measurements.some((m: any) => m.id === first.id), 'first event persisted');
    receiving.kill('SIGSTOP');
    try {
      await stop(server, 'SIGKILL');
      await backend();
      const missed = await (await post({ ...payload, eventId: 'missed-while-disconnected' })).json();
      receiving.kill('SIGCONT');
      await until(() => receiving.output.includes(missed.id), 'reconnect catch-up', 20000);
      await delay(1200);
      expect(receiving.output.split(missed.id)).toHaveLength(2);
    } finally { receiving.kill('SIGCONT'); }
  }, 30000);
});
