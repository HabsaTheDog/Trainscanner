import { IsString, IsOptional, IsNumber, IsDateString, Min, Max } from 'class-validator';

export class SearchRequestDto {
    @IsString()
    origin: string;

    @IsString()
    destination: string;

    @IsDateString()
    departure: string;

    @IsOptional()
    @IsDateString()
    arrival?: string;

    @IsOptional()
    @IsNumber()
    @Min(0)
    @Max(5)
    max_transfers?: number;

    @IsOptional()
    @IsNumber()
    @Min(1)
    @Max(20)
    max_results?: number;
}
