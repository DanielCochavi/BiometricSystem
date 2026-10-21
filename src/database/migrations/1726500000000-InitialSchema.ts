import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1726500000000 implements MigrationInterface {
  name = 'InitialSchema1726500000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE "measurements" (
        "id" uuid NOT NULL DEFAULT gen_random_uuid(),
        "eventId" varchar(100) NOT NULL,
        "userId" varchar(100) NOT NULL,
        "timestamp" timestamptz NOT NULL,
        "heartRate" smallint NOT NULL,
        "hrv" double precision,
        "createdAt" timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT "PK_measurements_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(
      'CREATE INDEX "IDX_measurements_user_timestamp" ON "measurements" ("userId", "timestamp")',
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP TABLE "measurements"');
  }
}
