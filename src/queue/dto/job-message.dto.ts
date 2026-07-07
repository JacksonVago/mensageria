import { JobsStatus } from "@prisma/client";
import { Transform } from "class-transformer";
import { IsDate, IsEnum, IsNumber, IsOptional, IsString } from "class-validator";

export class jobMessageDto {
  @IsString()
  id: string

  @Transform(({ value }) => Number(value))
  @IsNumber()
  empresaId: number

  @Transform(({ value }) => Number(value))
  @IsNumber()
  alertaId: number

  @IsString()
  descAlerta: string

  @Transform(({ value }) => Number(value))
  @IsOptional()
  @IsNumber()
  pessoaId: number

  @Transform(({ value }) => Number(value))
  @IsOptional()
  @IsNumber()
  imovelId: number

  @Transform(({ value }) => Number(value))
  @IsOptional()
  @IsNumber()
  locacaoId: number

  @IsOptional()
  @IsString()
  str_email: string

  @IsOptional()
  @IsString()
  str_message: string

  @IsOptional()
  @IsString()
  str_error: string

  @IsOptional()
  @IsString()
  str_start_date: string

  @IsOptional()
  @IsString()
  str_end_date: string

  @IsOptional()
  @IsString()
  str_start_time: string

  @IsOptional()
  @IsString()
  str_end_time: string

  @IsOptional()
  @IsString()
  str_cron: string

  @Transform(({ value }) => Number(value))
  @IsOptional()
  @IsNumber()
  int_delay: number

  @Transform(({ value }) => new Date(value))
  @IsDate()
  @IsOptional()
  dtm_created: Date

  @Transform(({ value }) => new Date(value))
  @IsDate()
  @IsOptional()
  dtm_updated: Date

  @IsEnum(JobsStatus)
  status: JobsStatus

  @IsString()
  @IsOptional()
  userId: string
}

