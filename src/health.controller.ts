import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { IngestionAdmission } from './measurements/ingestion-admission';
import { DataSource } from 'typeorm';

@Controller('health')
export class HealthController {
  constructor(private readonly database: DataSource, private readonly admission: IngestionAdmission) {}

  @Get()
  health() {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready() {
    try { await this.database.query('SELECT 1'); }
    catch { throw new ServiceUnavailableException({ code: 'DATABASE_UNAVAILABLE', status: 'unavailable' }); }
    this.admission.assertReady();
    return { status: 'ready' };
  }
}
