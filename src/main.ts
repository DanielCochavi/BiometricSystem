import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { createValidationPipe } from './common/validation';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { abortOnError: false });
  app.enableShutdownHooks();
  app.useGlobalPipes(createValidationPipe());

  try { await app.listen(Number(process.env.PORT ?? 3000)); }
  catch (error) { await app.close(); throw error; }
}

bootstrap().catch(() => {
  console.error('STARTUP_FAILED: Check configuration and database availability');
  process.exit(1);
});
