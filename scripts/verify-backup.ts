import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { DataSource } from 'typeorm';
import source from '../src/database/data-source';
import { NotificationStore } from '../src/notifications/notification-store';
import { checkpoint } from '../src/notifications/checkpoint';

function docker(project: string | undefined, command: string[], input?: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['compose', ...(project ? ['-p', project] : []), 'exec', '-T', 'postgres', ...command],
      { stdio: ['pipe', 'pipe', 'pipe'] });
    const output: Buffer[] = [];
    child.stdout.on('data', value => output.push(value));
    // Never echo credentials or database output on failure.
    child.stderr.resume();
    child.stdin.on('error', () => {});
    child.on('error', () => reject(new Error('BACKUP_COMMAND_FAILED')));
    child.on('close', code => code === 0 ? resolve(Buffer.concat(output)) : reject(new Error('BACKUP_COMMAND_FAILED')));
    child.stdin.end(input);
  });
}

export async function verifyBackup(options: { databaseUrl?: string; composeProject?: string } = {}) {
  const parent = new URL(options.databaseUrl ?? process.env.DATABASE_URL ?? 'postgres://interview:interview@127.0.0.1:55432/interview');
  if (!['localhost', '127.0.0.1'].includes(parent.hostname)) throw new Error('BACKUP_REHEARSAL_REQUIRES_LOCAL_DATABASE');
  if (options.composeProject && !/^[a-z][a-z0-9_-]*$/.test(options.composeProject)) throw new Error('INVALID_COMPOSE_PROJECT');
  if (source.options.type !== 'postgres') throw new Error('PostgreSQL required');
  const suffix = randomUUID().replaceAll('-', '');
  const names = [`biometric_backup_${suffix}`, `biometric_restore_${suffix}`];
  const created: string[] = [], connections: DataSource[] = [];
  const admin = await new DataSource({ type: 'postgres', url: parent.toString(), extra: { connectionTimeoutMillis: 3000 } }).initialize();
  const user = decodeURIComponent(parent.username);
  const patient = 'backup-patient';
  try {
    // Refuse to use pg_dump from a Compose service connected to another cluster.
    const [identity] = await admin.query('SELECT system_identifier::text AS id FROM pg_control_system()');
    const containerIdentity = await docker(options.composeProject, ['psql', '-U', user, '-d', decodeURIComponent(parent.pathname.slice(1)), '-Atc', 'SELECT system_identifier FROM pg_control_system()']);
    if (containerIdentity.toString().trim() !== identity.id) throw new Error('BACKUP_DATABASE_MISMATCH');
    for (const name of names) { await admin.query(`CREATE DATABASE "${name}"`); created.push(name); }
    for (const name of names) {
      const url = new URL(parent); url.pathname = `/${name}`;
      connections.push(await new DataSource({ ...source.options, url: url.toString() }).initialize());
    }
    const [original, restored] = connections;
    await original.runMigrations();
    const rows = await original.query(`WITH saved AS (
      INSERT INTO measurements ("eventId","userId",timestamp,"heartRate",hrv)
      VALUES ('backup-one',$1,'2026-10-20T10:00:00Z',80,42),('backup-two',$1,'2026-10-20T10:00:01Z',81,NULL) RETURNING *
    ), queued AS (INSERT INTO notification_outbox(measurement_id,user_id) SELECT id,"userId" FROM saved RETURNING measurement_id)
    SELECT saved.* FROM saved JOIN queued ON measurement_id=id ORDER BY "eventId"`, [patient]);
    const before = new NotificationStore(original);
    await before.indexCommitted(); await before.markPublished([rows[0].id]);
    await original.query("UPDATE notification_outbox SET attempts=2, next_attempt_at=clock_timestamp()-interval '1 second' WHERE published_at IS NULL");
    await original.query('INSERT INTO notification_replay_floor(user_id,position) VALUES($1,0)',[patient]);
    const tables = ['measurements','notification_outbox','notification_position','notification_replay_floor','migrations'];
    async function state(db: DataSource) {
      const result: Record<string,unknown> = {};
      for (const table of tables) result[table] = await db.query(`SELECT to_jsonb(t) AS row FROM ${table} t ORDER BY to_jsonb(t)::text`);
      return result;
    }
    const expected = JSON.stringify(await state(original));
    const expectedReplay = JSON.stringify(await before.replay(patient,checkpoint(patient,'0'),100));
    const archive = await docker(options.composeProject,['pg_dump','-U',user,'-d',names[0],'-Fc','--no-owner','--no-privileges']);
    await docker(options.composeProject,['pg_restore','-U',user,'-d',names[1],'--no-owner','--no-privileges','--exit-on-error'],archive);
    if (JSON.stringify(await state(restored)) !== expected) throw new Error('BACKUP_STATE_MISMATCH');
    const after = new NotificationStore(restored);
    if (JSON.stringify(await after.replay(patient,checkpoint(patient,'0'),100)) !== expectedReplay) throw new Error('BACKUP_REPLAY_MISMATCH');
    let duplicateRejected = false;
    try { await restored.query(`INSERT INTO measurements("eventId","userId",timestamp,"heartRate") VALUES('backup-one',$1,now(),80)`,[patient]); }
    catch(error) { duplicateRejected = (error as {driverError?:{code?:string}}).driverError?.code === '23505'; }
    if (!duplicateRejected) throw new Error('BACKUP_UNIQUENESS_MISSING');
    const pending = await after.claimPending();
    if (pending.length !== 1 || pending[0].measurement.id !== rows[1].id) throw new Error('BACKUP_PENDING_WORK_MISMATCH');
    await after.markPublished([pending[0].measurement.id]);
    if ((await restored.query('SELECT count(*)::int AS n FROM notification_outbox WHERE published_at IS NULL'))[0].n !== 0) throw new Error('BACKUP_RETRY_FAILED');
    return { measurements: 2, replayEvents: 2, pendingRecovered: 1, migrations: 3, uniquenessPreserved: true, temporaryDatabasesRemoved: true };
  } finally {
    for (const db of connections) if (db.isInitialized) await db.destroy();
    for (const name of created) await admin.query(`DROP DATABASE "${name}"`);
    await admin.destroy();
  }
}

if (require.main === module) {
  const { values } = parseArgs({ options: { 'compose-project': { type: 'string' } } });
  verifyBackup({ composeProject: values['compose-project'] })
    .then(result => console.log('BACKUP_RESTORE_PASSED',JSON.stringify(result)))
    .catch(() => { console.error('BACKUP_RESTORE_FAILED: Check local PostgreSQL, Docker Compose, and database permissions'); process.exitCode=1; });
}
