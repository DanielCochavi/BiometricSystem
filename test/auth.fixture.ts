export const deviceToken = 'test-device-token-123456789';
export const clinicianToken = 'test-clinician-token-123456';
export const authConfig = JSON.stringify([
  { id: 'device', token: deviceToken, readUserIds: [], writeUserIds: ['user-123'] },
  { id: 'clinician', token: clinicianToken, readUserIds: ['user-123'], writeUserIds: [] },
]);
