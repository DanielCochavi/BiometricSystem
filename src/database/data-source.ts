import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { Measurement } from '../measurements/measurement.entity';
import { InitialSchema1726500000000 } from './migrations/1726500000000-InitialSchema';
import { UniqueMeasurementEvent1792490000000 } from './migrations/1792490000000-UniqueMeasurementEvent';
import { NotificationOutbox1792570000000 } from './migrations/1792570000000-NotificationOutbox';

export default new DataSource({
  type: 'postgres',
  url:
    process.env.DATABASE_URL ??
    'postgres://interview:interview@127.0.0.1:55432/interview',
  entities: [Measurement],
  migrations: [InitialSchema1726500000000, UniqueMeasurementEvent1792490000000, NotificationOutbox1792570000000],
  extra: { connectionTimeoutMillis: 3000, statement_timeout: 5000, max: 10 },
  synchronize: false,
});
