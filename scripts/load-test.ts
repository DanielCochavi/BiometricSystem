import { spawn, execFile } from 'node:child_process';
import { promisify, parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { cpus, totalmem, platform, release } from 'node:os';
import { performance } from 'node:perf_hooks';
import { createServer } from 'node:net';
import { io, Socket } from 'socket.io-client';
import { DataSource } from 'typeorm';
import source from '../src/database/data-source';
import { Measurement } from '../src/measurements/measurement.entity';
import { scheduleArrivals } from './load-schedule';
import { PublicationLogEvidence, recoveryResponseMatches, reconcileDeliveries } from './load-evidence';

const exec = promisify(execFile);
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const percentile = (values: number[], p: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length ? +sorted[Math.ceil(sorted.length * p) - 1].toFixed(2) : null;
};
const { values: cli } = parseArgs({ options: {
  'outage-project': { type: 'string' }, profile: { type: 'string', default: 'full' },
}, allowPositionals: false });
const outageProject = cli['outage-project'];
const profile = cli.profile;
if (!['full', 'review', 'phase5'].includes(profile!) || (outageProject !== undefined && !/^[a-z][a-z0-9-]+$/.test(outageProject))) {
  throw new Error('Usage: npm run test:load -- [--profile full|review|phase5] [--outage-project isolated-compose-project]');
}
const runId = randomUUID().replace(/-/g, '');
const dbName = `biometric_load_${runId}`;
const patients = Array.from({ length: 20 }, (_, i) => `load-patient-${i}`);
const writer = `writer-${randomUUID()}`;
const readers = patients.map(() => `reader-${randomUUID()}`);
const identities = [{ id: 'load-writer', token: writer, readUserIds: [], writeUserIds: patients },
  ...patients.map((userId, i) => ({ id: `reader-${i}`, token: readers[i], readUserIds: [userId], writeUserIds: [] }))];
const outputPath = `load-results/${runId}.json`;
const report: any = { runId, profile, schemaVersion: 2, environment: { node: process.version, platform: `${platform()} ${release()}`,
  cpu: cpus()[0]?.model, logicalCpus: cpus().length, hostRamMiB: Math.round(totalmem() / 1048576),
  backendInstances: 1, dbPoolMax: 10, ingestMaxInFlight: Number(process.env.INGEST_MAX_IN_FLIGHT ?? 200), pendingThreshold: Number(process.env.INGEST_MAX_PENDING ?? 1000), sharedHost: true },
  targets: { baselineMeasurementsPerSecond: 100, dashboardConnections: 100,
    httpP95Ms: 500, receiptP95Ms: 1000, requestDeadlineMs: 10000, drainDeadlineMs: 10000,
    outstandingLimit: 1000, requireAllPlannedArrivals: true, normalSeconds: 30, sustainedSeconds: 180 }, stages: [] };
let admin: DataSource;
let database: DataSource;
let app: ReturnType<typeof spawn>;
let baseUrl = '';
let created = false;
let stopping = false;
let databaseStopped = false;
const logEvidence = new PublicationLogEvidence();
let monitor: NodeJS.Timeout;
let sampleInFlight = false;
let current: Stage | undefined;
const sockets: { socket: Socket; patient: string }[] = [];
const rssLimitMiB = Math.min(1024, totalmem() / 1048576 / 4);
let stageNumber = 0;
let monitoredResources: any[] = [];

interface RecordState {
  payload: any; kind: string; expectedStatus: number; dispatch: number; status?: number; response?: any;
  rowExpected: boolean; notify: boolean; deliveries: Set<number>; eventMetadata: Set<string>;
}
interface Stage {
  name: string; rate: number; seconds: number; prefix: string; started: number;
  records: Map<string, RecordState>; attempts: RecordState[]; expectedRows: Map<string, any>;
  statuses: Record<string, number>; httpMs: number[]; receiptMs: number[]; lagMs: number[];
  unexpectedEvents: number; wrongPatients: number; duplicates: number; badPayloads: number; disconnects: number;
  sent: number; unsent: number; peakPending: number; stopReason?: string;
}
function key(payload: any) { return `${payload.userId}/${payload.eventId}`; }
function valuesMatch(actual: any, expected: any) {
  return actual && actual.eventId === expected.eventId && actual.userId === expected.userId &&
    new Date(actual.timestamp).getTime() === new Date(expected.timestamp).getTime() &&
    actual.heartRate === expected.heartRate && (actual.hrv ?? null) === (expected.hrv ?? null);
}
function payloadFor(stage: Stage, index: number, patientIndex = index % 20) {
  return { eventId: `${stage.prefix}${index}`, userId: patients[patientIndex],
    timestamp: '2026-10-20T10:00:00.120Z', heartRate: 70 + index % 30, hrv: 40 };
}
async function checkpoint() { await writeFile(outputPath, JSON.stringify(report, null, 2) + '\n'); }
async function until(fn: () => boolean | Promise<boolean>, label: string, timeout = 15000) {
  const deadline = performance.now() + timeout;
  while (performance.now() < deadline) { if (await fn()) return; await pause(25); }
  throw new Error(`Deadline exceeded: ${label}`);
}
async function request(payload: any, token = writer) {
  try {
    const response = await fetch(`${baseUrl}/measurements`, { method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify(payload), signal: AbortSignal.timeout(10000) });
    return { status: response.status, body: await response.json() };
  } catch { return { status: 0, body: null }; }
}
async function configureSockets(count: number, hot = false) {
  current = undefined;
  for (const { socket } of sockets.splice(0)) { socket.removeAllListeners(); socket.disconnect(); }
  const start = performance.now();
  // Bound connection startup concurrency; these measurements are separate from HTTP load.
  for (let offset = 0; offset < count; offset += 20) {
    await Promise.all(Array.from({ length: Math.min(20, count - offset) }, async (_, index) => {
      const patientIndex = hot ? 0 : (offset + index) % 20;
      const socketIndex = offset + index;
      const socket = io(baseUrl, { autoConnect: false, forceNew: true, transports: ['websocket'],
        auth: { token: readers[patientIndex] }, reconnection: false, timeout: 5000 });
      sockets.push({ socket, patient: patients[patientIndex] });
      socket.on('measurement', (measurement, _metadata) => {
        const stage = current;
        if (!stage) return;
        if (measurement?.userId !== patients[patientIndex]) { stage.wrongPatients++; return; }
        const record = stage.records.get(key(measurement ?? {}));
        if (!record || !record.notify) { stage.unexpectedEvents++; return; }
        if (record.deliveries.has(socketIndex)) stage.duplicates++;
        record.deliveries.add(socketIndex);
        record.eventMetadata.add(`${measurement.id}/${new Date(measurement.createdAt).getTime()}`);
        if (!valuesMatch(measurement, record.payload) || !measurement.id || !measurement.createdAt) stage.badPayloads++;
        stage.receiptMs.push(performance.now() - record.dispatch);
      });
      socket.on('disconnect', () => { if (current) current.disconnects++; });
      await new Promise<void>((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('connect_error', reject);
        socket.connect();
      });
      const ack = await socket.timeout(5000).emitWithAck('subscribe', { userId: patients[patientIndex] });
      if (!ack.subscribed) throw new Error('Load subscription rejected');
    }));
  }
  return +(performance.now() - start).toFixed(2);
}
function newStage(name: string, rate: number, seconds: number): Stage {
  return { name, rate, seconds, prefix: `load-${runId}-${++stageNumber}-`, started: performance.now(),
    records: new Map(), attempts: [], expectedRows: new Map(), statuses: {}, httpMs: [], receiptMs: [], lagMs: [],
    unexpectedEvents: 0, wrongPatients: 0, duplicates: 0, badPayloads: 0, disconnects: 0,
    sent: 0, unsent: 0, peakPending: 0 };
}
async function runStage(name: string, rate: number, seconds: number, options: { hot?: boolean; mixed?: boolean; fault?: boolean } = {}) {
  current = undefined;
  const stage = newStage(name, rate, seconds);
  await until(async () => {
    const [pending] = await database.query('SELECT EXISTS (SELECT 1 FROM notification_outbox WHERE published_at IS NULL) AS pending');
    return !pending.pending;
  }, 'previous stage durable notifications drained', 45000);
  const startingRows = await database.getRepository(Measurement).count();
  const seeds = Array.from({ length: 20 }, (_, i) => payloadFor(stage, -i - 1, i));
  if (options.mixed) {
    for (const seed of seeds) {
      if ((await request(seed)).status !== 201) throw new Error('Cannot seed retry workload');
      stage.expectedRows.set(key(seed), seed);
    }
    await pause(200);
  }
  current = stage;
  monitoredResources = [];
  let pending = 0;
  let completedErrors = 0;
  const jobs = new Set<Promise<void>>();
  stage.started = performance.now();
  function dispatch(index: number) {
    const selection = index % 100;
    let kind = 'new', expectedStatus = 201, token = writer;
    let payload: any = payloadFor(stage, index, options.hot ? 0 : index % 20);
    if (options.mixed) {
      if (selection >= 70 && selection < 90) { kind = 'retry'; expectedStatus = 200; payload = seeds[index % 20]; }
      else if (selection >= 90 && selection < 95) { kind = 'conflict'; expectedStatus = 409; payload = { ...seeds[index % 20], heartRate: 200 }; }
      else if (selection >= 95 && selection < 98) { kind = 'invalid'; expectedStatus = 400; payload.heartRate = '80'; }
      else if (selection >= 98) { kind = 'unauthorized'; expectedStatus = 403; token = readers[index % 20]; }
    }
    const dispatchTime = performance.now();
    const record: RecordState = { payload, kind, expectedStatus, dispatch: dispatchTime,
      rowExpected: kind === 'new', notify: kind === 'new', deliveries: new Set(), eventMetadata: new Set() };
    // Retry/conflict attempts share seeded identities; none should emit a new event.
    if (!stage.records.has(key(payload))) stage.records.set(key(payload), record);
    if (kind === 'new') stage.expectedRows.set(key(payload), payload);
    stage.attempts.push(record);
    stage.lagMs.push(dispatchTime - (stage.started + index * 1000 / rate));
    stage.sent++;
    pending++;
    stage.peakPending = Math.max(stage.peakPending, pending);
    const job = (async () => {
      const result = await request(payload, token);
      record.status = result.status;
      record.response = result.body;
      stage.statuses[result.status] = (stage.statuses[result.status] ?? 0) + 1;
      stage.httpMs.push(performance.now() - dispatchTime);
      if (result.status !== expectedStatus) completedErrors++;
    })().finally(() => { pending--; jobs.delete(job); });
    jobs.add(job);
  }
  let lastSafetyCheck = 0;
  let faultAction: Promise<void> | undefined;
  let faultError: unknown;
  if (options.fault) faultAction = (async () => {
    await pause(1000);
    await admin.destroy(); // Avoid idle admin connection errors during PostgreSQL shutdown.
    await database.destroy();
    await exec('docker', ['compose', '-p', outageProject!, 'stop', 'postgres']);
    databaseStopped = true;
    const failedRead = await fetch(`${baseUrl}/measurements/${patients[0]}`, {
      headers: { authorization: `Bearer ${readers[0]}` }, signal: AbortSignal.timeout(10000) });
    stage.statuses[`history-${failedRead.status}`] = 1;
    if ((await failedRead.json()).code !== 'DATABASE_UNAVAILABLE') throw new Error('Unsafe or unexpected outage read error');
    await pause(3000);
    await exec('docker', ['compose', '-p', outageProject!, 'up', '-d', '--wait', 'postgres']);
    databaseStopped = false;
    await admin.initialize();
    await database.initialize();
  })().catch(error => { faultError = error; });
  stage.unsent = await scheduleArrivals({ rate, durationMs: seconds * 1000,
    elapsedMs: () => performance.now() - stage.started,
    dispatch,
    canDispatch: () => pending < 1000 && !stage.stopReason,
    shouldStop: () => stopping || Boolean(stage.stopReason),
    onTick: elapsed => {
      if (!options.fault && stage.sent >= rate * 3 && elapsed - lastSafetyCheck >= 1000) {
        lastSafetyCheck = elapsed;
        if (completedErrors > stage.sent * 0.05) stage.stopReason = 'Unexpected response rate exceeded 5%';
        if ((percentile(stage.httpMs, 0.95) ?? 0) > 2000) stage.stopReason = 'HTTP p95 exceeded 2 seconds';
      }
    },
  });
  const actualScheduleSeconds = (performance.now() - stage.started) / 1000;
  await Promise.all([...jobs]);
  if (faultAction) await faultAction;
  if (faultError) throw faultError;
  const recoveryStatuses: Record<string, number> = {};
  const recoveredResponses: { record: RecordState; response: unknown }[] = [];
  if (options.fault) {
    for (const record of stage.attempts) {
      if (record.status === 201) continue;
      const retry = await request(record.payload);
      recoveryStatuses[retry.status] = (recoveryStatuses[retry.status] ?? 0) + 1;
      recoveredResponses.push({ record, response: retry.body });
      if (![200, 201].includes(retry.status)) stage.stopReason = 'Recovery retry failed';
    }
  }
  const readRows = () => database.getRepository(Measurement).createQueryBuilder('m')
    .where('m.eventId LIKE :prefix', { prefix: `${stage.prefix}%` }).getMany();
  let rows: Measurement[] = await readRows();
  const deliveryCounts = () => reconcileDeliveries(
    [...stage.records.values()].filter(r => r.notify).map(r => ({
      identity: key(r.payload), patient: r.payload.userId, received: r.deliveries.size,
    })), new Set(rows.map(key)), sockets.map(s => s.patient));
  const drainDeadline = performance.now() + 10000;
  while (deliveryCounts().missingEvents > 0 && performance.now() < drainDeadline) await pause(25);
  await pause(150);
  rows = await readRows();
  const deliveries = deliveryCounts();
  current = undefined;
  let badRows = 0;
  for (const row of rows) {
    const expected = stage.expectedRows.get(key(row));
    if (!expected || !valuesMatch(row, expected)) badRows++;
  }
  const badResponses = stage.attempts.filter(r => r.status !== r.expectedStatus).length;
  const badSuccessPayloads = stage.attempts.filter(r => [200, 201].includes(r.status ?? 0) && !valuesMatch(r.response, r.payload)).length;
  const missingEvents = deliveries.missingEvents;
  const rowsByIdentity = new Map(rows.map(row => [key(row), row]));
  const identityMismatches = [...stage.records.values()].filter(r => r.response?.id &&
    rowsByIdentity.get(key(r.payload))?.id !== r.response.id).length;
  const badRecoveryResponses = recoveredResponses.filter(({ record, response }) =>
    !recoveryResponseMatches(response, rowsByIdentity.get(key(record.payload)), record.payload)).length;
  const missingAcknowledgedRows = stage.attempts.filter(r => r.status === 201 && !rowsByIdentity.has(key(r.payload))).length;
  const cpuSamples = [...monitoredResources];
  const eventMetadataMismatches = [...stage.records.values()].filter(r => {
    const row = rowsByIdentity.get(key(r.payload));
    return [...r.eventMetadata].some(value => value !== `${row?.id}/${row?.createdAt.getTime()}`);
  }).length;
  const responseMetadataMismatches = stage.attempts.filter(r => {
    if (![200, 201].includes(r.status ?? 0)) return false;
    const row = rowsByIdentity.get(key(r.payload));
    return !row || row.id !== r.response?.id || row.createdAt.getTime() !== new Date(r.response?.createdAt).getTime();
  }).length;
  const expectedErrorCodes: Record<number, string[]> = { 400: ['VALIDATION_FAILED'], 403: ['PATIENT_ACCESS_DENIED'],
    409: ['EVENT_CONFLICT'], 503: ['DATABASE_UNAVAILABLE', 'SERVER_BUSY', 'SERVER_DRAINING'] };
  const badErrorPayloads = stage.attempts.filter(r => expectedErrorCodes[r.status ?? 0] &&
    !expectedErrorCodes[r.status!].includes(r.response?.code)).length;
  const generatorSchedulingLimited = (percentile(stage.lagMs, 0.95) ?? 0) > 50;
  const outstandingCapReached = stage.peakPending >= 1000;
  const generatorLimited = stage.unsent > 0 || generatorSchedulingLimited;
  const correctness = rows.length === stage.expectedRows.size && badRows === 0 && identityMismatches === 0 &&
    badSuccessPayloads === 0 && missingAcknowledgedRows === 0 && deliveries.notificationsForUnstoredRows === 0 && badRecoveryResponses === 0 && eventMetadataMismatches === 0 && responseMetadataMismatches === 0 && badErrorPayloads === 0 && stage.wrongPatients === 0 && stage.duplicates === 0 && stage.badPayloads === 0 &&
    stage.unexpectedEvents === 0 && !stage.stopReason &&
    (options.fault || (badResponses === 0 && missingEvents === 0 && stage.disconnects === 0));
  const speed = (percentile(stage.httpMs, 0.95) ?? Infinity) <= 500 &&
    (sockets.length === 0 || (percentile(stage.receiptMs, 0.95) ?? Infinity) <= 1000);
  const faultPassed = !options.fault || (stage.statuses['503'] > 0 && stage.statuses['history-503'] === 1 &&
    stage.attempts.every(r => [0, 201, 503].includes(r.status ?? 0)));
  const result = { name, startingRows, targetRate: rate, seconds, clients: sockets.length, sent: stage.sent, unsent: stage.unsent,
    sentRate: +(stage.sent / seconds).toFixed(2), actualScheduleSeconds: +actualScheduleSeconds.toFixed(3),
    activeSentRate: +(stage.sent / actualScheduleSeconds).toFixed(2), statuses: stage.statuses, recoveryStatuses,
    rows: rows.length, expectedRows: stage.expectedRows.size, badRows, badSuccessPayloads, identityMismatches,
    eventMetadataMismatches, responseMetadataMismatches, badErrorPayloads, badRecoveryResponses,
    ...deliveries, missingAcknowledgedRows, targetRowsNotStored: stage.expectedRows.size - rows.length,
    wrongPatients: stage.wrongPatients, duplicates: stage.duplicates, badPayloads: stage.badPayloads,
    unexpectedEvents: stage.unexpectedEvents, disconnects: stage.disconnects,
    httpP50Ms: percentile(stage.httpMs, .5), httpP95Ms: percentile(stage.httpMs, .95), httpP99Ms: percentile(stage.httpMs, .99),
    receiptP95Ms: percentile(stage.receiptMs, .95), dispatchLagP95Ms: percentile(stage.lagMs, .95),
    peakPending: stage.peakPending, resources: cpuSamples, generatorLimited, generatorSchedulingLimited, outstandingCapReached,
    passed: Boolean(correctness && faultPassed && !generatorLimited && (options.fault || speed)),
    stopReason: stage.stopReason ?? null, faultWindowBestEffort: Boolean(options.fault) };
  report.stages.push(result);
  await checkpoint();
  console.log(JSON.stringify({ ...result, resources: `${cpuSamples.length} samples` }));
  if (stopping) throw new Error('Load run interrupted');
  return result;
}
async function monitorResources() {
  if (!current || sampleInFlight) return;
  sampleInFlight = true;
  const sampledStage = current;
  try {
    const { stdout } = await exec('ps', ['-o', '%cpu=,rss=', '-p', String(app.pid)]);
    const [cpuPercent, rssKiB] = stdout.trim().split(/\s+/).map(Number);
    const sample: any = { elapsedSeconds: +((performance.now() - sampledStage.started) / 1000).toFixed(1),
      cpuPercent, rssMiB: +(rssKiB / 1024).toFixed(1), generatorRssMiB: +(process.memoryUsage().rss / 1048576).toFixed(1) };
    if (database.isInitialized) {
      try {
        const [counts] = await database.query("SELECT count(*)::int AS connections, count(*) FILTER (WHERE state = 'active')::int AS active, count(*) FILTER (WHERE wait_event_type = 'Lock')::int AS lock_waits FROM pg_stat_activity WHERE datname = current_database()");
        Object.assign(sample, counts);
      } catch { /* Expected while the database is interrupted; requests capture availability errors. */ }
    }
    if (current !== sampledStage) return;
    monitoredResources.push(sample);
    if (sample.rssMiB > rssLimitMiB && current) current.stopReason = `Backend exceeded RSS limit ${rssLimitMiB} MiB`;
  } catch { /* Process exit is also detected by failed requests and startup checks. */ }
  finally { sampleInFlight = false; }
}
async function main() {
  await mkdir('load-results', { recursive: true });
  if (source.options.type !== 'postgres') throw new Error('PostgreSQL required');
  const parentUrl = process.env.TEST_DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview';
  admin = await new DataSource({ type: 'postgres', url: parentUrl, extra: { connectionTimeoutMillis: 3000 } }).initialize();
  await admin.query(`CREATE DATABASE "${dbName}"`);
  created = true;
  const url = new URL(parentUrl); url.pathname = `/${dbName}`;
  database = await new DataSource({ ...source.options, url: url.toString() }).initialize();
  await database.runMigrations();
  report.environment.postgres = (await database.query('SHOW server_version'))[0].server_version;
  report.environment.rssSafetyLimitMiB = rssLimitMiB;
  report.environment.startingRows = await database.getRepository(Measurement).count();
  try {
    const { stdout } = await exec('docker', ['info', '--format', '{{json .MemTotal}}']);
    report.environment.dockerRamMiB = Math.round(Number(stdout) / 1048576);
  } catch { report.environment.dockerRamMiB = null; }
  const probe = createServer();
  await new Promise<void>(resolve => probe.listen(0, '127.0.0.1', resolve));
  const port = (probe.address() as { port: number }).port;
  await new Promise<void>(resolve => probe.close(() => resolve()));
  baseUrl = `http://127.0.0.1:${port}`;
  app = spawn(process.execPath, ['dist/main.js'], { env: { ...process.env, DATABASE_URL: url.toString(),
    PORT: String(port), AUTH_IDENTITIES: JSON.stringify(identities) }, stdio: ['ignore', 'pipe', 'pipe'] });
  app.stdout!.on('data', chunk => logEvidence.append(chunk));
  app.stderr!.on('data', chunk => logEvidence.append(chunk));
  await until(async () => {
    if (app.exitCode !== null) throw new Error('Load backend exited before startup');
    try { return (await fetch(`${baseUrl}/health`, { signal: AbortSignal.timeout(500) })).ok; } catch { return false; }
  }, 'backend startup');
  monitor = setInterval(() => { void monitorResources(); }, 1000);
  console.log(`Load test started; report: ${outputPath}`);
  await runStage('warmup', 100, 5);
  if (profile === 'full') {
    for (const rate of [25, 50, 100]) await runStage(`ingestion-${rate}`, rate, 30);
    const connectionMs = await configureSockets(100);
    await pause(2000);
    report.connections = { count: sockets.length, subscribed: sockets.filter(s => s.socket.connected).length, connectAndSubscribeMs: connectionMs };
    await checkpoint();
    await runStage('combined-baseline', 100, 30);
    await runStage('sustained-baseline', 100, 180);
    await configureSockets(100, true);
    await runStage('hot-patient', 100, 30, { hot: true });
    await configureSockets(0);
    for (const rate of [200, 500, 1000]) {
      const result = await runStage(`ingestion-stress-${rate}`, rate, 30);
      if (!result.passed) break;
    }
    await configureSockets(100);
    for (const rate of [200, 500, 1000]) {
      const result = await runStage(`combined-stress-${rate}`, rate, 30);
      if (!result.passed) break;
    }
  } else if (profile === 'phase5') {
    await runStage('ingestion-stress-1000', 1000, 30);
    const connectionMs = await configureSockets(100);
    await pause(2000);
    report.connections = { count: sockets.length, subscribed: sockets.filter(s => s.socket.connected).length, connectAndSubscribeMs: connectionMs };
    await runStage('combined-stress-500', 500, 30);
    await runStage('combined-stress-1000', 1000, 30);
  } else {
    await configureSockets(100);
    await runStage('combined-baseline', 100, 30);
    for (const rate of [500, 1000]) {
      const result = await runStage(`combined-stress-${rate}`, rate, 30);
      if (!result.passed) break;
    }
  }
  await runStage('mixed-requests', 100, 30, { mixed: true });
  if (outageProject) await runStage('database-outage-recovery', 100, 15, { fault: true });
  else report.outage = 'Not run: supply --outage-project for an isolated verification Compose project';
  await runStage('baseline-after-stress', 100, 30);
  report.appPublicationFailureLogged = logEvidence.publicationFailureSeen;
  report.completed = true;
  report.finishedAt = new Date().toISOString();
  await checkpoint();
}
async function cleanup() {
  current = undefined;
  clearInterval(monitor);
  await until(() => !sampleInFlight, 'resource sampler cleanup');
  for (const { socket } of sockets) { socket.removeAllListeners(); socket.disconnect(); }
  if (app && app.exitCode === null && app.signalCode === null) {
    app.kill('SIGTERM');
    await until(() => app.exitCode !== null || app.signalCode !== null, 'backend shutdown');
  }
  if (databaseStopped) await exec('docker', ['compose', '-p', outageProject!, 'up', '-d', '--wait', 'postgres']);
  if (database?.isInitialized) await database.destroy();
  if (created) {
    if (!admin.isInitialized) await admin.initialize();
    await admin.query(`DROP DATABASE "${dbName}"`);
  }
  if (admin?.isInitialized) await admin.destroy();
}
process.once('SIGINT', () => { stopping = true; });
process.once('SIGTERM', () => { stopping = true; });
main().catch(error => { console.error(error); report.error = String(error); process.exitCode = 1; })
  .finally(async () => {
    try { await cleanup(); report.cleanedUp = true; } catch (error) { report.cleanupError = String(error); process.exitCode = 1; }
    if (report.completed && report.stages.some((s: any) => !s.passed)) process.exitCode = 1;
    await checkpoint();
    console.log(`Load report saved: ${outputPath}`);
  });
