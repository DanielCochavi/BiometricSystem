import { MigrationInterface, QueryRunner } from 'typeorm';

export class NotificationOutbox1792570000000 implements MigrationInterface {
  name = 'NotificationOutbox1792570000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE TABLE notification_outbox (
      measurement_id uuid PRIMARY KEY REFERENCES measurements(id) ON DELETE CASCADE,
      user_id varchar(100) NOT NULL,
      position bigint UNIQUE,
      queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      replay_ready_at timestamptz,
      published_at timestamptz,
      attempts integer NOT NULL DEFAULT 0,
      next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp() + interval '500 milliseconds'
    )`);
    await queryRunner.query('CREATE INDEX outbox_unindexed ON notification_outbox(queued_at, measurement_id) WHERE position IS NULL');
    await queryRunner.query('CREATE INDEX outbox_pending ON notification_outbox(next_attempt_at) WHERE published_at IS NULL');
    await queryRunner.query('CREATE INDEX outbox_replay ON notification_outbox(user_id, position) WHERE position IS NOT NULL');
    await queryRunner.query('CREATE TABLE notification_position (id integer PRIMARY KEY CHECK (id = 1), value bigint NOT NULL)');
    await queryRunner.query('INSERT INTO notification_position VALUES (1, 0)');
    await queryRunner.query('CREATE TABLE notification_replay_floor (user_id varchar(100) PRIMARY KEY, position bigint NOT NULL)');
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP TABLE notification_replay_floor, notification_outbox, notification_position');
  }
}
