import {
  IsInt,
  IsNumber,
  IsOptional,
  Max,
  Min,
  ValidateBy,
  isISO8601,
} from 'class-validator';
import { IsIdentifier } from '../../common/validation';

function IsMeasurementTimestamp(): PropertyDecorator {
  return ValidateBy({
    name: 'isMeasurementTimestamp',
    validator: {
      validate: (value: unknown) => typeof value === 'string' &&
        /^(?!0000)\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
        isISO8601(value, { strict: true, strictSeparator: true }) &&
        Number.isFinite(Date.parse(value)),
      defaultMessage: () => 'timestamp must be a valid ISO 8601 datetime with timezone and at most millisecond precision',
    },
  });
}

export class CreateMeasurementDto {
  @IsIdentifier()
  eventId!: string;

  @IsIdentifier()
  userId!: string;

  @IsMeasurementTimestamp()
  timestamp!: string;

  @IsInt()
  @Min(20)
  @Max(250)
  heartRate!: number;

  @IsNumber({ allowNaN: false, allowInfinity: false })
  @Min(0)
  @IsOptional()
  hrv?: number | null;
}
