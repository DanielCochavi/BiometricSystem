require('reflect-metadata');
const { NestFactory } = require('@nestjs/core');
const { AppModule } = require('../../dist/app.module');
const { createValidationPipe } = require('../../dist/common/validation');
const { RealtimeBus } = require('../../dist/realtime/realtime-bus.service');
const { OutboxPublisher } = require('../../dist/notifications/outbox-publisher');
const { NotificationStore } = require('../../dist/notifications/notification-store');
(async () => {
  const app = await NestFactory.create(AppModule, { logger: false });
  app.useGlobalPipes(createValidationPipe());
  app.enableShutdownHooks();
  if (process.env.CRASH_BOUNDARY === 'missed-wake') app.get(OutboxPublisher).wake = () => {};
  // Test-only gates let the parent kill a real process at exact durability boundaries.
  if (process.env.CRASH_BOUNDARY === 'committed') {
    const bus = app.get(RealtimeBus);
    const publish = bus.publish.bind(bus);
    bus.publish = async (measurement, context) => {
      process.send?.({ boundary: 'committed', measurement });
      await new Promise(resolve => process.once('message', resolve));
      await publish(measurement, context);
    };
  }
  if (process.env.CRASH_BOUNDARY === 'emitted') {
    app.get(NotificationStore).markPublished = async ids => {
      if (!ids.length) return;
      process.send?.({ boundary: 'emitted', ids });
      await new Promise(() => {});
    };
  }
  await app.listen(Number(process.env.PORT), '127.0.0.1');
  process.send?.({ ready: true });
})().catch(() => { process.send?.({ startupFailed: true }); process.exitCode = 1; });
