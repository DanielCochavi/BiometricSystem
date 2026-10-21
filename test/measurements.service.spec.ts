import { ConflictException, InternalServerErrorException, Logger, ServiceUnavailableException } from '@nestjs/common';
import { QueryFailedError, Repository } from 'typeorm';
import { OutboxPublisher } from '../src/notifications/outbox-publisher';
import { EVENT_UNIQUE_CONSTRAINT, Measurement } from '../src/measurements/measurement.entity';
import { IngestionAdmission } from '../src/measurements/ingestion-admission';
import { MeasurementsService } from '../src/measurements/measurements.service';

describe('MeasurementsService', () => {
  const repository = {
    create: jest.fn(),
    query: jest.fn(),
    find: jest.fn(),
    findOneBy: jest.fn(),
  };
  const publisher = { wake: jest.fn() };
  const release = jest.fn();
  let service: MeasurementsService;
  let log: jest.SpyInstance;
  const dto = { eventId: 'evt-1', userId: 'user-1', timestamp: '2026-09-16T12:00:00.000Z', heartRate: 80, hrv: 40 };
  const saved = { ...dto, timestamp: new Date(dto.timestamp), id: 'measurement-1', createdAt: new Date('2026-09-16T12:00:01Z') } as Measurement;

  beforeEach(() => {
    jest.resetAllMocks();
    log = jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    repository.create.mockImplementation((value) => ({ ...value }));
    repository.query.mockResolvedValue([saved]);
    service = new MeasurementsService(repository as unknown as Repository<Measurement>, publisher as unknown as OutboxPublisher, { enter: () => release } as unknown as IngestionAdmission);
  });
  afterEach(() => log.mockRestore());

  it('commits storage before waking the publisher', async () => {
    let commit!: (result: Measurement[]) => void;
    repository.query.mockReturnValue(new Promise<Measurement[]>((resolve) => { commit = resolve; }));
    const result = service.ingest(dto);
    expect(publisher.wake).not.toHaveBeenCalled();
    const insertedValues = repository.query.mock.calls[0][1];
    commit([saved]);
    await expect(result).resolves.toEqual({ measurement: saved, created: true });
    expect(repository.create).toHaveBeenCalledWith({
      ...dto,
      timestamp: new Date(dto.timestamp),
      hrv: 40,
    });
    expect(insertedValues).toEqual([dto.eventId, dto.userId, new Date(dto.timestamp), dto.heartRate, dto.hrv]);
    expect(publisher.wake).toHaveBeenCalledWith();
    expect(release).toHaveBeenCalledTimes(1);
    expect(repository.query.mock.invocationCallOrder[0]).toBeLessThan(
      publisher.wake.mock.invocationCallOrder[0],
    );
  });

  it('does not publish when persistence fails', async () => {
    repository.query.mockRejectedValue(Object.assign(new Error('private database details'), { code: 'ECONNREFUSED' }));
    await expect(service.ingest(dto)).rejects.toThrow(ServiceUnavailableException);
    expect(publisher.wake).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledTimes(1);
  });

  it.each([undefined, null])('normalizes absent/null HRV to database null #%#', async (hrv) => {
    await service.ingest({ ...dto, hrv });
    expect(repository.create).toHaveBeenCalledWith(expect.objectContaining({ hrv: null }));
  });

  function pgFailure(code: string, constraint?: string) {
    return new QueryFailedError('INSERT private SQL', ['private-patient-data'],
      Object.assign(new Error('private database details'), { code, constraint }));
  }

  it('returns the existing measurement for an identical normalized retry without publication', async () => {
    repository.query.mockRejectedValue(pgFailure('23505', EVENT_UNIQUE_CONSTRAINT));
    repository.findOneBy.mockResolvedValue(saved);
    await expect(service.ingest({ ...dto, timestamp: '2026-09-16T14:00:00+02:00' }))
      .resolves.toEqual({ measurement: saved, created: false });
    expect(repository.findOneBy).toHaveBeenCalledWith({ userId: dto.userId, eventId: dto.eventId });
    expect(publisher.wake).not.toHaveBeenCalled();
  });

  it.each([{ heartRate: 81 }, { hrv: 41 }, { timestamp: '2026-09-16T12:00:01Z' }])('rejects conflicting payload #%#', async (change) => {
    repository.query.mockRejectedValue(pgFailure('23505', EVENT_UNIQUE_CONSTRAINT));
    repository.findOneBy.mockResolvedValue(saved);
    await expect(service.ingest({ ...dto, ...change })).rejects.toThrow(ConflictException);
    expect(publisher.wake).not.toHaveBeenCalled();
  });

  it('does not treat an unrelated unique constraint violation as an identical retry', async () => {
    repository.query.mockRejectedValue(pgFailure('23505', 'another_constraint'));
    await expect(service.ingest(dto)).rejects.toThrow(InternalServerErrorException);
    expect(repository.findOneBy).not.toHaveBeenCalled();
    expect(publisher.wake).not.toHaveBeenCalled();
  });

  it('returns committed storage without waiting for background publication', async () => {
    publisher.wake.mockReturnValue(undefined);
    await expect(service.ingest(dto)).resolves.toEqual({ measurement: saved, created: true });
    expect(repository.query).toHaveBeenCalledTimes(1);
    expect(publisher.wake).toHaveBeenCalledTimes(1);
  });

  it.each(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', '08006', '53300', '57P01', '57014'])('maps storage unavailability %s to a safe 503', async (code) => {
    repository.query.mockRejectedValue(pgFailure(code));
    try {
      await service.ingest(dto);
      throw new Error('Expected storage rejection');
    } catch (error) {
      expect(error).toBeInstanceOf(ServiceUnavailableException);
      expect((error as ServiceUnavailableException).getResponse()).toMatchObject({ code: 'DATABASE_UNAVAILABLE' });
      expect(JSON.stringify(error)).not.toContain('private');
    }
    expect(publisher.wake).not.toHaveBeenCalled();
  });

  it('sanitizes unexpected storage failures', async () => {
    repository.query.mockRejectedValue(pgFailure('42P01'));
    await expect(service.ingest(dto)).rejects.toThrow(InternalServerErrorException);
    expect(JSON.stringify(log.mock.calls)).not.toContain('private');
  });

  it('maps a failed retry lookup to storage unavailability without publishing', async () => {
    repository.query.mockRejectedValue(pgFailure('23505', EVENT_UNIQUE_CONSTRAINT));
    repository.findOneBy.mockRejectedValue(pgFailure('08006'));
    await expect(service.ingest(dto)).rejects.toThrow(ServiceUnavailableException);
    expect(publisher.wake).not.toHaveBeenCalled();
  });

  it('returns a safe internal error if the conflicting row disappears', async () => {
    repository.query.mockRejectedValue(pgFailure('23505', EVENT_UNIQUE_CONSTRAINT));
    repository.findOneBy.mockResolvedValue(null);
    await expect(service.ingest(dto)).rejects.toThrow(InternalServerErrorException);
  });

  it('reads bounded history with deterministic ordering', async () => {
    repository.find.mockResolvedValue([saved]);
    await expect(service.findRecent('user-1', 20)).resolves.toEqual([saved]);
    expect(repository.find).toHaveBeenCalledWith({ where: { userId: 'user-1' },
      order: { timestamp: 'DESC', id: 'DESC' }, take: 20 });
  });

  it('maps history database failures to 503', async () => {
    repository.find.mockRejectedValue(pgFailure('57P03'));
    await expect(service.findRecent('user-1', 20)).rejects.toThrow(ServiceUnavailableException);
  });
});
