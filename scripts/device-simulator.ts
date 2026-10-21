const userId = process.argv[2] ?? 'user-123';
const endpoint = process.env.API_URL ?? 'http://localhost:3000/measurements';
const token = process.env.API_TOKEN;
if (!token) throw new Error('API_TOKEN is required');

async function send() {
  const payload = {
    eventId: `evt-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    userId,
    timestamp: new Date().toISOString(),
    heartRate: Math.floor(65 + Math.random() * 35),
    hrv: Number((25 + Math.random() * 35).toFixed(1)),
  };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify(payload),
  });

  console.log(response.status, await response.text());
}

setInterval(() => void send().catch(console.error), 1000);
void send().catch(console.error);
