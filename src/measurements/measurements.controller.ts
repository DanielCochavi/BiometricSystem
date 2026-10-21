import { Body, Controller, Get, HttpException, Param, Post, Query, Req, Res, UseGuards } from '@nestjs/common';
import { AuthGuard, AuthenticatedRequest } from '../auth/auth.guard';
import { AuthService } from '../auth/auth.service';
import { CreateMeasurementDto } from './dto/create-measurement.dto';
import { MeasurementsService } from './measurements.service';
import { HistoryQueryDto, PatientParamsDto } from './dto/history-query.dto';
import { ReplayQueryDto } from './dto/replay-query.dto';
import { NotificationStore } from '../notifications/notification-store';

@Controller('measurements')
@UseGuards(AuthGuard)
export class MeasurementsController {
  constructor(
    private readonly measurementsService: MeasurementsService,
    private readonly auth: AuthService,
    private readonly notifications: NotificationStore,
  ) {}

  @Post()
  async create(
    @Body() dto: CreateMeasurementDto,
    @Req() request: AuthenticatedRequest,
    @Res({ passthrough: true }) response: { status(code: number): unknown; setHeader(name: string, value: string): unknown },
  ) {
    this.auth.requirePermission(request.principal, dto.userId, 'write');
    let result;
    try { result = await this.measurementsService.ingest(dto); }
    catch (error) {
      if (error instanceof HttpException && error.getStatus() === 503) response.setHeader('Retry-After', '1');
      throw error;
    }
    response.status(result.created ? 201 : 200);
    return result.measurement;
  }

  @Get(':userId/events')
  replay(@Param() params: PatientParamsDto, @Query() query: ReplayQueryDto,
    @Req() request: AuthenticatedRequest) {
    this.auth.requirePermission(request.principal, params.userId, 'read');
    return this.notifications.replay(params.userId, query.after, query.limit);
  }

  @Get(':userId')
  findRecent(
    @Param() params: PatientParamsDto,
    @Query() query: HistoryQueryDto,
    @Req() request: AuthenticatedRequest,
  ) {
    this.auth.requirePermission(request.principal, params.userId, 'read');
    return this.measurementsService.findRecent(params.userId, query.limit);
  }
}
