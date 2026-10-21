import { DataSource } from 'typeorm';
import { IngestionAdmission } from '../src/measurements/ingestion-admission';

describe('Bounded ingestion admission', () => {
  const original = { active: process.env.INGEST_MAX_IN_FLIGHT, pending: process.env.INGEST_MAX_PENDING };
  let admission: IngestionAdmission;
  let query: jest.Mock;
  beforeEach(() => {
    process.env.INGEST_MAX_IN_FLIGHT = '2'; process.env.INGEST_MAX_PENDING = '3';
    query = jest.fn().mockResolvedValue([{ full: false }]);
    admission = new IngestionAdmission({ query } as unknown as DataSource);
  });
  afterEach(async () => {
    await admission.onModuleDestroy();
    for (const [key, value] of [['INGEST_MAX_IN_FLIGHT', original.active], ['INGEST_MAX_PENDING', original.pending]]) {
      if (value === undefined) delete process.env[key!]; else process.env[key!] = value;
    }
  });
  it('rejects excess concurrent work immediately and releases each slot once', () => {
    const first = admission.enter(), second = admission.enter();
    expect(() => admission.enter()).toThrow('Capacity temporarily exceeded');
    first(); first();
    const next = admission.enter();
    expect(() => admission.enter()).toThrow('Capacity temporarily exceeded');
    second(); next();
  });
  it('blocks a sampled full backlog and resumes when it drains', async () => {
    query.mockResolvedValueOnce([{ full: true }]); await admission.refresh();
    expect(() => admission.enter()).toThrow('Capacity temporarily exceeded');
    expect(query.mock.calls[0][1]).toEqual([2]);
    await admission.refresh(); admission.enter()();
  });
  it('fails closed during storage interruption and recovers after a successful sample', async () => {
    query.mockRejectedValueOnce(new Error('private database details')); await admission.refresh();
    expect(() => admission.enter()).toThrow('Storage temporarily unavailable');
    await admission.refresh(); admission.enter()();
  });
  it('does not overlap slow backlog queries', async () => {
    let release!: (rows: unknown[]) => void;
    query.mockReturnValueOnce(new Promise(resolve => { release = resolve; }));
    const a = admission.refresh(), b = admission.refresh();
    expect(query).toHaveBeenCalledTimes(1);
    release([{full:false}]); await Promise.all([a,b]);
  });
  it('rejects new work during shutdown and waits for admitted work to finish', async () => {
    const release = admission.enter();
    let stopped = false;
    const stopping = admission.onModuleDestroy().then(() => { stopped = true; });
    await Promise.resolve();
    expect(() => admission.enter()).toThrow('Server is stopping');
    expect(stopped).toBe(false);
    release(); await stopping; expect(stopped).toBe(true);
  });
  it('bounds shutdown waiting even if admitted work never settles', async () => {
    jest.useFakeTimers();
    const release = admission.enter();
    try {
      const stopping = admission.onModuleDestroy();
      await Promise.resolve();
      jest.advanceTimersByTime(10000);
      await stopping;
      expect(() => admission.enter()).toThrow('Server is stopping');
    } finally { release(); jest.useRealTimers(); }
  });
  it.each(['0','1.5','1000001'])('rejects invalid backlog configuration %s', value => {
    process.env.INGEST_MAX_PENDING = value;
    expect(() => new IngestionAdmission({ query } as unknown as DataSource)).toThrow('INGEST_CONFIG_INVALID');
  });
  it.each(['0','-1','1.5','not-a-number','10001'])('rejects invalid concurrency configuration %s', value => {
    process.env.INGEST_MAX_IN_FLIGHT = value;
    expect(() => new IngestionAdmission({ query } as unknown as DataSource)).toThrow('INGEST_CONFIG_INVALID');
  });
});
