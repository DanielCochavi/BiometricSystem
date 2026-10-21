import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { RealtimeModule } from '../realtime/realtime.module';
import { Measurement } from './measurement.entity';
import { AuthModule } from '../auth/auth.module';
import { MeasurementsController } from './measurements.controller';
import { MeasurementsService } from './measurements.service';
import { IngestionAdmission } from './ingestion-admission';
import { NotificationStore } from '../notifications/notification-store';
import { OutboxPublisher } from '../notifications/outbox-publisher';

@Module({
  imports: [TypeOrmModule.forFeature([Measurement]), RealtimeModule, AuthModule],
  controllers: [MeasurementsController],
  providers: [MeasurementsService, NotificationStore, OutboxPublisher, IngestionAdmission],
  exports: [MeasurementsService, IngestionAdmission],
})
export class MeasurementsModule {}
