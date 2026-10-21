interface EvidenceRecord {
  id?: unknown;
  eventId?: unknown;
  userId?: unknown;
  timestamp?: unknown;
  heartRate?: unknown;
  hrv?: unknown;
  createdAt?: unknown;
}
function object(value: unknown): EvidenceRecord | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as EvidenceRecord : undefined;
}
function instant(value: unknown): number {
  return value instanceof Date ? value.getTime() : typeof value === 'string' ? Date.parse(value) : NaN;
}

// Recovery success must prove which record was returned, not just an HTTP success status.
export function recoveryResponseMatches(response: unknown, row: unknown, expected: unknown): boolean {
  const actual = object(response), stored = object(row), payload = object(expected);
  if (!actual || !stored || !payload || typeof stored.id !== 'string') return false;
  const timestamp = instant(payload.timestamp), createdAt = instant(stored.createdAt);
  return Number.isFinite(timestamp) && Number.isFinite(createdAt) &&
    actual.id === stored.id && instant(actual.createdAt) === createdAt &&
    actual.userId === payload.userId && actual.eventId === payload.eventId &&
    instant(actual.timestamp) === timestamp && actual.heartRate === payload.heartRate &&
    (actual.hrv ?? null) === (payload.hrv ?? null);
}

export class PublicationLogEvidence {
  private tail = '';
  publicationFailureSeen = false;

  append(chunk: string | Buffer): void {
    const combined = this.tail + chunk.toString();
    this.publicationFailureSeen ||= combined.includes('REALTIME_PUBLISH_FAILED');
    this.tail = combined.slice(-10000);
  }
}

interface DeliveryRecord {
  identity: string;
  patient: string;
  received: number;
}

export function reconcileDeliveries(records: DeliveryRecord[], committed: Set<string>, subscribers: string[]) {
  let requestedDeliveries = 0, expectedDeliveries = 0, receivedDeliveries = 0;
  let missingEvents = 0, notificationsForUnstoredRows = 0;
  for (const record of records) {
    const audience = subscribers.filter(patient => patient === record.patient).length;
    requestedDeliveries += audience;
    if (committed.has(record.identity)) {
      expectedDeliveries += audience;
      receivedDeliveries += record.received;
      missingEvents += Math.max(0, audience - record.received);
    } else notificationsForUnstoredRows += record.received;
  }
  return { requestedDeliveries, expectedDeliveries, receivedDeliveries, missingEvents,
    notificationsForUnstoredRows, undeliveredRequestedEvents: requestedDeliveries - receivedDeliveries };
}
