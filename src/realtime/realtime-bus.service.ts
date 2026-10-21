import { Injectable } from '@nestjs/common';
import { Measurement } from '../measurements/measurement.entity';
import { RealtimeGateway } from './realtime.gateway';

export interface PublicationContext { position?: string; }

@Injectable()
export class RealtimeBus {
  constructor(private readonly gateway: RealtimeGateway) {}

  async publish(measurement: Measurement, context?: PublicationContext): Promise<void> {
    this.gateway.publishMeasurement(measurement, context);
  }
}
