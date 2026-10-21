import { verifyBackup } from '../scripts/verify-backup';
describe('Real pg_dump / pg_restore rehearsal',()=>{
  it('restores measurements, migrations, replay state, pending work and uniqueness',async()=>{
    const result=await verifyBackup({databaseUrl:process.env.TEST_DATABASE_URL??'postgres://interview:interview@127.0.0.1:55432/interview',composeProject:process.env.TEST_COMPOSE_PROJECT});
    expect(result).toEqual({measurements:2,replayEvents:2,pendingRecovered:1,migrations:3,uniquenessPreserved:true,temporaryDatabasesRemoved:true});
  },30000);
});
