import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, Unique } from 'typeorm';

export const EVENT_UNIQUE_CONSTRAINT = 'UQ_measurements_user_event';

@Entity({ name: 'measurements' })
@Index(['userId', 'timestamp'])
@Unique(EVENT_UNIQUE_CONSTRAINT, ['userId', 'eventId'])
export class Measurement {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'varchar', length: 100 })
  eventId!: string;

  @Column({ type: 'varchar', length: 100 })
  userId!: string;

  @Column({ type: 'timestamptz' })
  timestamp!: Date;

  @Column({ type: 'smallint' })
  heartRate!: number;

  @Column({ type: 'double precision', nullable: true })
  hrv!: number | null;

  @CreateDateColumn({ type: 'timestamptz' })
  createdAt!: Date;
}
