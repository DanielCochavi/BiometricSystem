import { IsOptional, IsString, MaxLength, MinLength } from 'class-validator';
import { HistoryQueryDto } from './history-query.dto';

export class ReplayQueryDto extends HistoryQueryDto {
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(1024)
  after?: string;
}
