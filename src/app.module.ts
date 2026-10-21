import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HealthController } from './health.controller';
import { Measurement } from './measurements/measurement.entity';
import { MeasurementsModule } from './measurements/measurements.module';
import { RealtimeModule } from './realtime/realtime.module';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      retryAttempts: 1,
      url:
        process.env.DATABASE_URL ??
        'postgres://interview:interview@127.0.0.1:55432/interview',
      entities: [Measurement],
      synchronize: false,
      extra: { connectionTimeoutMillis: 3000, statement_timeout: 5000, max: 10 },
    }),
    RealtimeModule,
    MeasurementsModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
