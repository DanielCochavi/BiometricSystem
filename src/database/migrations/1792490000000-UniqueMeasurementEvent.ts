import { MigrationInterface, QueryRunner } from 'typeorm';

export class UniqueMeasurementEvent1792490000000 implements MigrationInterface {
  name = 'UniqueMeasurementEvent1792490000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    // Existing duplicate keys fail migration; never silently discard measurements.
    await queryRunner.query(`ALTER TABLE "measurements"
      ADD CONSTRAINT "UQ_measurements_user_event" UNIQUE ("userId", "eventId")`);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('ALTER TABLE "measurements" DROP CONSTRAINT "UQ_measurements_user_event"');
  }
}
