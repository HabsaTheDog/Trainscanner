import { Controller, Post, Body, Get, Query } from '@nestjs/common';
import { RoutingService } from './routing.service';
import { SearchRequestDto } from '../common/dto/search-request.dto';
import { SearchResponse, Station } from '../common/types';

@Controller('api')
export class RoutingController {
    constructor(private readonly routingService: RoutingService) { }

    /**
     * Search for train routes between two locations.
     * This is the main endpoint that orchestrates MOTIS + OJP queries.
     */
    @Post('routes')
    async searchRoutes(@Body() dto: SearchRequestDto): Promise<SearchResponse> {
        return this.routingService.search(dto);
    }

    /**
     * Search for stations by name (autocomplete).
     */
    @Get('stations')
    async searchStations(@Query('q') query: string): Promise<Station[]> {
        return this.routingService.searchStations(query || '');
    }
}
