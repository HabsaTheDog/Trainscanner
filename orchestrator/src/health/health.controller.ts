import { Controller, Get } from '@nestjs/common';
import { CacheService } from '../cache/cache.service';
import { MotisService } from '../motis/motis.service';

@Controller()
export class HealthController {
    constructor(
        private readonly cacheService: CacheService,
        private readonly motisService: MotisService,
    ) { }

    @Get('health')
    async health() {
        const motisAvailable = await this.motisService.isAvailable();

        return {
            status: 'ok',
            timestamp: new Date().toISOString(),
            services: {
                orchestrator: true,
                redis: this.cacheService.isConnected(),
                motis: motisAvailable,
            },
            version: '0.1.0',
            mode: process.env.OJP_MODE || 'mock',
        };
    }
}
