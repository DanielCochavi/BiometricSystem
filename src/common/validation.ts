import { BadRequestException, ValidationPipe } from '@nestjs/common';
import { ValidateBy } from 'class-validator';

export function isIdentifier(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 100 &&
    value.trim() === value && !/[\p{Cc}\p{Cs}]/u.test(value);
}

export function IsIdentifier(): PropertyDecorator {
  return ValidateBy({
    name: 'isIdentifier',
    validator: {
      validate: isIdentifier,
      defaultMessage: (args) => `${args?.property} must be a valid nonblank identifier of at most 100 characters without surrounding whitespace or control characters`,
    },
  });
}

export function createValidationPipe(): ValidationPipe {
  return new ValidationPipe({
    whitelist: true,
    forbidNonWhitelisted: true,
    transform: true,
    exceptionFactory: (errors) => new BadRequestException({
      code: 'VALIDATION_FAILED',
      message: 'Request validation failed',
      errors: errors.map((error) => ({
        field: error.property,
        messages: Object.values(error.constraints ?? {}),
      })),
    }),
  });
}
