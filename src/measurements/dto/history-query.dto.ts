import { Transform } from 'class-transformer';
import { IsInt, Max, Min } from 'class-validator';
import { IsIdentifier } from '../../common/validation';

export class HistoryQueryDto {
  @Transform(({ value }) => typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value)
  @IsInt()
  @Min(1)
  @Max(100)
  limit = 20;
}

export class PatientParamsDto {
  @IsIdentifier()
  userId!: string;
}
