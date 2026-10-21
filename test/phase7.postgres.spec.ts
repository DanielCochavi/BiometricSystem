import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { fork } from 'node:child_process';
import { createServer } from 'node:net';
import source from '../src/database/data-source';
import { HealthController } from '../src/health.controller';
import { MeasurementsModule } from '../src/measurements/measurements.module';
import { IngestionAdmission } from '../src/measurements/ingestion-admission';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { NotificationStore } from '../src/notifications/notification-store';
import { createValidationPipe } from '../src/common/validation';
import { authConfig, deviceToken, clinicianToken } from './auth.fixture';
const pause = (ms:number) => new Promise(resolve=>setTimeout(resolve,ms));
async function until(check:()=>boolean|Promise<boolean>, label:string, timeout=10000) {
  const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await pause(20);}throw new Error(`Deadline: ${label}`);
}
describe('Overload and lifecycle with real PostgreSQL and HTTP',()=>{
  const old={auth:process.env.AUTH_IDENTITIES,active:process.env.INGEST_MAX_IN_FLIGHT,pending:process.env.INGEST_MAX_PENDING};
  const name=`biometric_phase7_${randomUUID().replaceAll('-','')}`;
  let admin:DataSource,db:DataSource,app:INestApplication,admission:IngestionAdmission,base:string,url:string,created=false;
  const payload={eventId:'phase7-event',userId:'user-123',timestamp:'2026-10-20T10:00:00Z',heartRate:80,hrv:42};
  beforeAll(async()=>{
    if(source.options.type!=='postgres')throw new Error('PostgreSQL required');
    process.env.AUTH_IDENTITIES=authConfig;process.env.INGEST_MAX_IN_FLIGHT='2';process.env.INGEST_MAX_PENDING='2';
    const parent=process.env.TEST_DATABASE_URL??'postgres://interview:interview@127.0.0.1:55432/interview';
    admin=await new DataSource({type:'postgres',url:parent}).initialize();await admin.query(`CREATE DATABASE "${name}"`);created=true;
    const address=new URL(parent);address.pathname=`/${name}`;url=address.toString();
    const module=await Test.createTestingModule({imports:[TypeOrmModule.forRoot({...source.options,url,retryAttempts:0}),MeasurementsModule],controllers:[HealthController]})
      .overrideProvider(OutboxPublisher).useValue({wake(){}}).compile();
    app=module.createNestApplication({logger:false});app.useGlobalPipes(createValidationPipe());db=app.get(DataSource);await db.runMigrations();
    admission=app.get(IngestionAdmission);await app.listen(0,'127.0.0.1');base=`http://127.0.0.1:${(app.getHttpServer().address()as AddressInfo).port}`;
  },20000);
  beforeEach(async()=>{await db.query('TRUNCATE measurements CASCADE');await admission.refresh();});
  afterAll(async()=>{
    await app?.close();if(created)await admin.query(`DROP DATABASE "${name}"`);await admin?.destroy();
    for(const[key,value]of[['AUTH_IDENTITIES',old.auth],['INGEST_MAX_IN_FLIGHT',old.active],['INGEST_MAX_PENDING',old.pending]]){
      if(value===undefined)delete process.env[key!];else process.env[key!]=value;
    }
  });
  function post(eventId=payload.eventId,token=deviceToken){return fetch(`${base}/measurements`,{method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:JSON.stringify({...payload,eventId})});}
  it('rejects excess work without storing it, then safely retries the same identity',async()=>{
    const guard=db.createQueryRunner();await guard.connect();const key=790000000+Math.floor(Math.random()*10000000);
    await guard.query('SELECT pg_advisory_lock($1)',[key]);
    await db.query(`CREATE FUNCTION block_ingestion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(${key}); RETURN NEW; END $$`);
    await db.query('CREATE TRIGGER block_ingestion BEFORE INSERT ON measurements FOR EACH ROW EXECUTE FUNCTION block_ingestion()');
    const jobs=[post('held-one'),post('held-two')];
    try{
      await until(async()=>(await admin.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=$1 AND wait_event='advisory'",[name]))[0].n===2,'two admitted writes');
      const rejected=await post('retry-after-busy');expect(rejected.status).toBe(503);expect(rejected.headers.get('retry-after')).toBe('1');
      expect(await rejected.json()).toMatchObject({code:'SERVER_BUSY'});
      expect((await db.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(0);
      await guard.query('SELECT pg_advisory_unlock($1)',[key]);
      expect((await Promise.all(jobs)).map(r=>r.status)).toEqual([201,201]);
      await app.get(NotificationStore).markPublished((await db.query('SELECT measurement_id FROM notification_outbox')).map((r:any)=>r.measurement_id));
      await admission.refresh();
      const accepted=await post('retry-after-busy');expect(accepted.status).toBe(201);const saved=await accepted.json();
      const retry=await post('retry-after-busy');expect(retry.status).toBe(200);expect(await retry.json()).toEqual(saved);
      expect((await db.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(3);
    }finally{
      await guard.query('SELECT pg_advisory_unlock($1)',[key]);await Promise.all(jobs);
      await db.query('DROP TRIGGER block_ingestion ON measurements');await db.query('DROP FUNCTION block_ingestion()');await guard.release();
    }
  },15000);
  it('pauses new submissions for backlog, preserves history/access control, and resumes after drain',async()=>{
    expect((await post('backlog-one')).status).toBe(201);expect((await post('backlog-two')).status).toBe(201);await admission.refresh();
    expect((await post('backlog-three')).status).toBe(503);
    expect((await fetch(`${base}/health/ready`)).status).toBe(503);
    expect((await post('forbidden',clinicianToken)).status).toBe(403);
    const history=await fetch(`${base}/measurements/user-123`,{headers:{authorization:`Bearer ${clinicianToken}`}});
    expect(history.status).toBe(200);expect(await history.json()).toHaveLength(2);
    await app.get(NotificationStore).markPublished((await db.query('SELECT measurement_id FROM notification_outbox')).map((r:any)=>r.measurement_id));
    await admission.refresh();expect((await post('backlog-three')).status).toBe(201);
  });
  it('returns a safe draining response and preserves already stored records',async()=>{
    expect((await post('stored-before-drain')).status).toBe(201);
    await admission.onModuleDestroy();
    const response=await post('during-drain');expect(response.status).toBe(503);expect(response.headers.get('retry-after')).toBe('1');
    expect(await response.json()).toMatchObject({code:'SERVER_DRAINING'});
    expect((await fetch(`${base}/health/ready`)).status).toBe(503);
    expect((await db.query('SELECT count(*)::int AS n FROM measurements'))[0].n).toBe(1);
  });
  it.each(['occupied port','unavailable database'])('exits cleanly after startup failure: %s',async failure=>{
    const peers=new Set<import('node:net').Socket>();
    const occupied=createServer(socket=>{peers.add(socket);socket.resume();socket.on('close',()=>peers.delete(socket));});await new Promise<void>(resolve=>occupied.listen(0,'::',resolve));
    const failingDatabase=new URL(url); failingDatabase.port=String((occupied.address()as AddressInfo).port);
    const child=fork('dist/main.js',[],{execArgv:[],env:{...process.env,PORT:failure==='occupied port'?String((occupied.address()as AddressInfo).port):'0',DATABASE_URL:failure==='unavailable database'?failingDatabase.toString():url,AUTH_IDENTITIES:authConfig},stdio:['ignore','pipe','pipe','ipc']});
    let output='';for(const stream of[child.stdout!,child.stderr!])stream.on('data',value=>{output+=value;});
    try{await until(()=>child.exitCode!==null,'failed startup exit').catch(error=>{throw new Error(String(error)+' '+output);});expect(child.exitCode).toBe(1);expect(output).toContain('STARTUP_FAILED');}
    finally{if(child.exitCode===null)child.kill('SIGKILL');for(const socket of peers)socket.destroy();await new Promise<void>(resolve=>occupied.close(()=>resolve()));}
  },15000);
});
