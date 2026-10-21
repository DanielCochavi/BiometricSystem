# Biometric Measurements System

Receives wearable measurements, validates and stores them in PostgreSQL, and sends live updates through a background publisher to dashboards subscribed to the correct patient.

## Technologies

• Node.js, TypeScript, and NestJS.  
• PostgreSQL 16, TypeORM, and database migrations.  
• Socket.IO for patient-specific live updates.  
• Jest and Docker Compose for testing and local setup.

## Prerequisites

• Node.js 22.9+ and npm.  
• Docker Desktop running, with Docker Compose available.  
• Ports **3000** (backend) and **55432** (database) available.  
• Run commands from the project folder containing `package.json`.

## How to run

### 1. Install and configure — Terminal 1

On first setup:

```bash
cp .env.example .env
npm ci
```

`.env` contains local settings and demo credentials. The device can write `user-123`; the clinician can read its history and live updates. Use private credentials outside local development.

### 2. Start PostgreSQL and create tables — Terminal 1

```bash
docker compose up -d --wait postgres
npm run migration:run
```

Wait for PostgreSQL to become healthy and migrations to finish. Run migrations again after schema changes.

### 3. Start the backend — Terminal 1

```bash
npm run start:dev
```

Leave it running. For a compiled build, use `npm run build`, then `npm run start:prod` instead.

### 4. Check the server — Terminal 2

```bash
curl http://localhost:3000/health
curl http://localhost:3000/health/ready
```

Expect `{"status":"ok"}` and `{"status":"ready"}`.

## Small example flow

### 1. Listen for patient updates — Terminal 2

```bash
npm run listen -- user-123
```

Wait for `subscribed: true`, then leave the listener running.

### 2. Submit a measurement — Terminal 3

```bash
measurement='{"eventId":"demo-event-1","userId":"user-123","timestamp":"2026-10-20T10:00:00.120Z","heartRate":80,"hrv":42}'
curl -i -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer demo-device-token-local-only' \
  -d "$measurement" http://localhost:3000/measurements
```

• New event: **201**, with the saved record; the listener prints the measurement.  
• Repeat the same command: **200**, with the same record and no additional notification.  
• Change `heartRate` while keeping the same patient and event ID: **409**.

If this example was already stored, use a new `eventId` to see a new notification.

### 3. Read patient history — Terminal 3

```bash
curl -H 'Authorization: Bearer demo-clinician-token-local-only' \
  'http://localhost:3000/measurements/user-123?limit=20'
```

Expect the saved measurement in a newest-first array. For continuous sample data, run `npm run simulate -- user-123`; stop it with **Ctrl+C**.

### 4. Stop and restart

Stop the backend and listener with **Ctrl+C** in their terminals, then stop PostgreSQL:

```bash
docker compose stop postgres
```

Saved measurements remain. To restart, start PostgreSQL and the backend using steps 2–4 above; reconnect the listener to receive new updates.

## API endpoints

Base URL: `http://localhost:3000`. Protected requests require `Authorization: Bearer <token>`.

| Method | Path | What it does |
| --- | --- | --- |
| GET | `/health` | Checks whether the server is running. |
| GET | `/health/ready` | Checks PostgreSQL and whether ingestion is accepting work. |
| POST | `/measurements` | Validates and stores one measurement, then queues live delivery to its patient's subscribers; requires write permission. |
| GET | `/measurements/:userId` | Returns recent measurements for the patient; requires read permission and accepts `?limit=1` through `100` (default `20`). |
| GET | `/measurements/:userId/events` | Returns a starting checkpoint, or missed updates after `?after=<checkpoint>&limit=20`; requires read permission. |

Run `npm run listen -- user-123` for live updates and automatic catch-up. The listener saves its recent view and checkpoint in `.runtime/receivers/`; use a second argument such as `receiver-2` for an independent listener.

## Useful error codes

• **400 / `VALIDATION_FAILED`** — Invalid measurement, patient ID, or query options.  
• **401 / `UNAUTHENTICATED`** — Missing or invalid credentials.  
• **403 / `PATIENT_ACCESS_DENIED`** — Credential lacks the required patient permission.  
• **400 / `REPLAY_CURSOR_INVALID`** — Invalid checkpoint or checkpoint for another patient.  
• **410 / `REPLAY_EXPIRED`** — Refresh history and use a new listener name to explicitly reset recovery.  
• **409 / `EVENT_CONFLICT`** — Event ID already exists for this patient with different values.  
• **503 / `DATABASE_UNAVAILABLE`** — Temporary storage failure; retry the same request.  
• **503 / `SERVER_BUSY`** — Ingestion or notification backlog is full; retry the same event after `Retry-After`.  
• **503 / `SERVER_DRAINING`** — Server is stopping; retry the same event after restart.  
• **500 / `INTERNAL_ERROR`** — Unexpected storage failure; check backend logs.  
• **`INGEST_CONFIG_INVALID`** — Ingestion limits are invalid.  
• **`AUTH_CONFIG_INVALID`** — Startup credentials are missing or malformed.  
• **`NOTIFICATION_CONFIG_INVALID`** — Replay retention configuration is invalid.  
• **`STARTUP_FAILED`** — Check configuration, PostgreSQL availability, and the backend port.  
• **`BACKUP_RESTORE_FAILED`** — Check local PostgreSQL, Docker Compose, and database permissions.  
• **`INVALID_SUBSCRIPTION`** — Subscription must contain a valid `userId` only.  
• **`REALTIME_PUBLISH_FAILED`** — Backend log: notification send failed; retained for retry.  
• **`OUTBOX_RETRY_FAILED`** — Backend log: background work failed; durable notifications remain available for retry.

## Tests

Start PostgreSQL first, then run:

```bash
npm run build
npm test
```

Tests use local Docker Compose PostgreSQL on port **55432**; backup tests require its admin account (the demo default).

For a different test database or named Compose project, set `TEST_DATABASE_URL` or `TEST_COMPOSE_PROJECT` in your terminal. Load checks also use `TEST_DATABASE_URL`.

### Backup/restore rehearsal

```bash
npm run verify:backup
```

Creates synthetic data in temporary databases, checks real `pg_dump`/`pg_restore`, replay, pending notifications, and uniqueness, then cleans up. It does not back up patient data. For a named Compose project, add `-- --compose-project <name>`.

### Load testing

```bash
npm run build
npm run test:load
```

Uses synthetic patients and a temporary database. Results are saved in `load-results/`; an unmet target returns a nonzero exit code. The default run does not stop PostgreSQL.

For both 1,000/second ingestion and live-receipt checks, use `npm run test:load -- --profile phase5`.

## Configuration and design

• Settings and ingestion limits: [.env.example](.env.example). If changing `POSTGRES_PORT`, also update `DATABASE_URL`.  
• A 503 does not acknowledge storage; retry using the same event ID and values.  
• Architecture, assumptions, and optional scaling: [design.md](design.md). Redis is optional.
