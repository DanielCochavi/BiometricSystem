import { Module } from '@nestjs/common';
import { RealtimeBus } from './realtime-bus.service';
import { RealtimeGateway } from './realtime.gateway';
import { AuthModule } from '../auth/auth.module';

@Module({
  imports: [AuthModule],
  providers: [RealtimeGateway, RealtimeBus],
  exports: [RealtimeGateway, RealtimeBus],
})
export class RealtimeModule {}
