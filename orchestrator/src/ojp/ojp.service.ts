import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CacheService } from '../cache/cache.service';
import { RateLimiterService } from '../rate-limiter/rate-limiter.service';
import { Segment, SegmentStop } from '../common/types';
import { MOCK_OJP_RESPONSES } from './mock/mock-ojp.data';

export type OjpCountry = 'de' | 'ch' | 'at';

@Injectable()
export class OjpService {
    private readonly logger = new Logger(OjpService.name);
    private readonly ojpMode: string;
    private readonly endpoints: Record<OjpCountry, string>;

    constructor(
        private readonly configService: ConfigService,
        private readonly cacheService: CacheService,
        private readonly rateLimiter: RateLimiterService,
    ) {
        this.ojpMode = this.configService.get<string>('OJP_MODE', 'mock');
        this.endpoints = {
            de: this.configService.get<string>('OJP_ENDPOINT_DE', ''),
            ch: this.configService.get<string>('OJP_ENDPOINT_CH', ''),
            at: this.configService.get<string>('OJP_ENDPOINT_AT', ''),
        };

        if (this.ojpMode === 'mock') {
            this.logger.log('OJP running in MOCK mode. Set OJP_MODE=live for real API calls.');
        }
    }

    /** Find feeder connections from origin to a hub station */
    async findFeederConnections(
        originName: string,
        hubName: string,
        country: OjpCountry,
        departure: Date,
    ): Promise<Segment[]> {
        const cacheKey = `ojp:feeder:${country}:${originName}:${hubName}:${departure.toISOString()}`;
        const cached = await this.cacheService.get<Segment[]>(cacheKey);
        if (cached) {
            this.logger.debug(`Cache hit for OJP feeder: ${cacheKey}`);
            return cached;
        }

        let segments: Segment[];

        if (this.ojpMode === 'mock') {
            segments = this.getMockFeederSegments(originName, hubName, country, departure);
        } else {
            segments = await this.queryOjpLive(originName, hubName, country, departure);
        }

        await this.cacheService.set(cacheKey, segments, 900); // 15 min TTL
        return segments;
    }

    /** Find distributor connections from a hub to the final destination */
    async findDistributorConnections(
        hubName: string,
        destinationName: string,
        country: OjpCountry,
        arrival: Date,
    ): Promise<Segment[]> {
        const cacheKey = `ojp:dist:${country}:${hubName}:${destinationName}:${arrival.toISOString()}`;
        const cached = await this.cacheService.get<Segment[]>(cacheKey);
        if (cached) return cached;

        let segments: Segment[];

        if (this.ojpMode === 'mock') {
            segments = this.getMockDistributorSegments(hubName, destinationName, country, arrival);
        } else {
            segments = await this.queryOjpLive(hubName, destinationName, country, arrival);
        }

        await this.cacheService.set(cacheKey, segments, 900);
        return segments;
    }

    /** Live OJP API query (placeholder for real implementation) */
    private async queryOjpLive(
        origin: string,
        destination: string,
        country: OjpCountry,
        dateTime: Date,
    ): Promise<Segment[]> {
        // Rate limit check
        const allowed = await this.rateLimiter.tryAcquire(country);
        if (!allowed) {
            this.logger.warn(`Rate limit exceeded for ${country}. Using cached/mock data.`);
            return this.getMockFeederSegments(origin, destination, country, dateTime);
        }

        const endpoint = this.endpoints[country];
        if (!endpoint) {
            this.logger.warn(`No OJP endpoint configured for ${country}`);
            return [];
        }

        // TODO: Implement real OJP XML request/response parsing
        // For now, return mock data
        this.logger.debug(`OJP live query would go to: ${endpoint}`);
        return this.getMockFeederSegments(origin, destination, country, dateTime);
    }

    /** Generate mock feeder segments */
    private getMockFeederSegments(
        origin: string,
        hub: string,
        country: OjpCountry,
        departure: Date,
    ): Segment[] {
        // Check if we have specific mock data for this route
        const mockKey = `${origin.toLowerCase()}-${hub.toLowerCase()}`;
        const mockData = MOCK_OJP_RESPONSES[mockKey];
        if (mockData) {
            return mockData.map((m) => ({
                ...m,
                departure: new Date(departure.getTime() + (m.offset_minutes || 0) * 60000).toISOString(),
                arrival: new Date(
                    departure.getTime() + ((m.offset_minutes || 0) + m.duration_minutes) * 60000,
                ).toISOString(),
            }));
        }

        // Generic mock feeder
        const depTime = new Date(departure);
        const arrTime = new Date(depTime.getTime() + 45 * 60 * 1000); // 45 min

        const operators: Record<OjpCountry, string> = {
            de: 'DB Regio',
            ch: 'SBB',
            at: 'ÖBB Regionalverkehr',
        };

        return [
            {
                type: 'feeder',
                source: 'ojp',
                origin: { name: origin },
                destination: { name: hub },
                departure: depTime.toISOString(),
                arrival: arrTime.toISOString(),
                duration_minutes: 45,
                train_number: `RE ${1000 + Math.floor(Math.random() * 9000)}`,
                operator: operators[country],
                route_type: 'RE',
            },
        ];
    }

    /** Generate mock distributor segments */
    private getMockDistributorSegments(
        hub: string,
        destination: string,
        country: OjpCountry,
        arrival: Date,
    ): Segment[] {
        const arrTime = new Date(arrival);
        const depTime = new Date(arrTime.getTime() - 30 * 60 * 1000); // 30 min before arrival

        const operators: Record<OjpCountry, string> = {
            de: 'DB Regio',
            ch: 'SBB',
            at: 'ÖBB Regionalverkehr',
        };

        return [
            {
                type: 'feeder', // distributor is also a feeder type
                source: 'ojp',
                origin: { name: hub },
                destination: { name: destination },
                departure: depTime.toISOString(),
                arrival: arrTime.toISOString(),
                duration_minutes: 30,
                train_number: `RB ${2000 + Math.floor(Math.random() * 8000)}`,
                operator: operators[country],
                route_type: 'RB',
            },
        ];
    }
}
