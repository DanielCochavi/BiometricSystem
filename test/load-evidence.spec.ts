import { PublicationLogEvidence, recoveryResponseMatches, reconcileDeliveries } from '../scripts/load-evidence';

describe('Load-test recovery evidence', () => {
  const payload = { eventId: 'event-1', userId: 'patient-1', timestamp: '2026-10-20T10:00:00.120Z', heartRate: 80, hrv: 42 };
  const response = { ...payload, id: 'saved-row-id', createdAt: '2026-10-20T10:00:01.000Z' };
  const row = { ...response, timestamp: new Date(response.timestamp), createdAt: new Date(response.createdAt) };

  it('accepts the exact stored row returned after recovery', () => {
    expect(recoveryResponseMatches(response, row, payload)).toBe(true);
  });
  it.each([
    { id: 'another-row' }, { userId: 'another-patient' }, { eventId: 'another-event' },
    { timestamp: '2026-10-20T10:00:00.121Z' }, { heartRate: 81 }, { hrv: 43 },
    { createdAt: '2026-10-20T10:00:02.000Z' }, { createdAt: undefined },
  ])('rejects a success response with incorrect record data #%#', change => {
    expect(recoveryResponseMatches({ ...response, ...change }, row, payload)).toBe(false);
  });
  it.each([undefined, null, [], { status: 201 }])('rejects missing or malformed recovery evidence #%#', value => {
    expect(recoveryResponseMatches(value, row, payload)).toBe(false);
  });
  it('rejects a response when its stored row is absent', () => {
    expect(recoveryResponseMatches(response, undefined, payload)).toBe(false);
  });
});

describe('Publication failure evidence', () => {
  it('remembers an earlier failure after the diagnostic tail rolls over', () => {
    const log = new PublicationLogEvidence();
    log.append('REALTIME_PUBLISH_FAILED');
    log.append('later log output '.repeat(2000));
    expect(log.publicationFailureSeen).toBe(true);
  });
  it('recognizes a failure marker split across output chunks', () => {
    const log = new PublicationLogEvidence();
    log.append(Buffer.from('REALTIME_PUB'));
    log.append(Buffer.from('LISH_FAILED'));
    expect(log.publicationFailureSeen).toBe(true);
  });
  it('does not claim a publication failure for unrelated output', () => {
    const log = new PublicationLogEvidence();
    log.append('Server started');
    expect(log.publicationFailureSeen).toBe(false);
  });
});

describe('Delivery accounting during rejected requests', () => {
  const subscribers = Array(5).fill('patient-1') as string[];
  it('separates rejected attempts from missing updates for committed rows', () => {
    const result = reconcileDeliveries([
      { identity: 'accepted', patient: 'patient-1', received: 5 },
      { identity: 'rejected', patient: 'patient-1', received: 0 },
    ], new Set(['accepted']), subscribers);
    expect(result).toEqual({ requestedDeliveries: 10, expectedDeliveries: 5, receivedDeliveries: 5,
      missingEvents: 0, notificationsForUnstoredRows: 0, undeliveredRequestedEvents: 5 });
  });
  it('still detects missing updates for a committed row', () => {
    expect(reconcileDeliveries([{ identity: 'accepted', patient: 'patient-1', received: 1 }],
      new Set(['accepted']), subscribers)).toMatchObject({ missingEvents: 4 });
  });
  it('rejects evidence of notifying about a row that was never committed', () => {
    expect(reconcileDeliveries([{ identity: 'rejected', patient: 'patient-1', received: 1 }],
      new Set(), subscribers)).toMatchObject({ expectedDeliveries: 0, missingEvents: 0, notificationsForUnstoredRows: 1 });
  });
  it('reproduces the real overload report without hiding its 312 rejected requests', () => {
    const records = Array.from({ length: 3021 }, (_, i) => ({ identity: `${i}`, patient: 'patient-1', received: i < 2709 ? 5 : 0 }));
    const committed = new Set(records.slice(0, 2709).map(record => record.identity));
    expect(reconcileDeliveries(records, committed, subscribers)).toEqual({ requestedDeliveries: 15105,
      expectedDeliveries: 13545, receivedDeliveries: 13545, missingEvents: 0,
      notificationsForUnstoredRows: 0, undeliveredRequestedEvents: 1560 });
  });
});
