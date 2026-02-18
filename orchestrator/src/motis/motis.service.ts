import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CacheService } from '../cache/cache.service';
import { Segment, SegmentStop } from '../common/types';

interface MotisConnection {
    stops: Array<{
        station: { id: string; name: string; pos: { lat: number; lng: number } };
        arrival: { time: number; schedule_time: number };
        departure: { time: number; schedule_time: number };
    }>;
    trips: Array<{
        id: { station_id: string; train_nr: number; line_id: string };
        range: { from: number; to: number };
    }>;
}

@Injectable()
export class MotisService {
    private readonly logger = new Logger(MotisService.name);
    private readonly motisUrl: string;

    constructor(
        private readonly configService: ConfigService,
        private readonly cacheService: CacheService,
    ) {
        const host = this.configService.get<string>('MOTIS_HOST', 'localhost');
        const port = this.configService.get<number>('MOTIS_PORT', 8080);
        this.motisUrl = `http://${host}:${port}`;
    }

    /** Query MOTIS for backbone routing between two hub stations */
    async findConnections(
        originUic: string,
        destinationUic: string,
        departure: Date,
        maxResults: number = 5,
    ): Promise<Segment[]> {
        const cacheKey = `motis:${originUic}:${destinationUic}:${departure.toISOString()}`;
        const cached = await this.cacheService.get<Segment[]>(cacheKey);
        if (cached) {
            this.logger.debug(`Cache hit for MOTIS query: ${cacheKey}`);
            return cached;
        }

        try {
            const response = await this.queryMotis(originUic, destinationUic, departure, maxResults);
            await this.cacheService.set(cacheKey, response, 900); // 15 min TTL
            return response;
        } catch (error) {
            this.logger.error(`MOTIS query failed: ${error}`);
            return this.getMockBackboneSegments(originUic, destinationUic, departure);
        }
    }

    private async queryMotis(
        originId: string,
        destId: string,
        departure: Date,
        maxResults: number,
    ): Promise<Segment[]> {
        const body = {
            destination: { type: 'Module', target: '/routing' },
            content_type: 'RoutingRequest',
            content: {
                start_type: 'OntripStationStart',
                start: {
                    station: { id: originId, name: '' },
                    departure_time: Math.floor(departure.getTime() / 1000),
                },
                destination: { id: destId, name: '' },
                search_type: 'Default',
                search_dir: 'Forward',
                via: [],
                additional_edges: [],
            },
        };

        const response = await fetch(`${this.motisUrl}/api/v1`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });

        if (!response.ok) {
            throw new Error(`MOTIS returned ${response.status}: ${await response.text()}`);
        }

        const data = await response.json();
        return this.parseMotisResponse(data);
    }

    private parseMotisResponse(data: { content?: { connections?: MotisConnection[] } }): Segment[] {
        const connections = data?.content?.connections || [];
        return connections.map((conn) => this.connectionToSegment(conn));
    }

    private connectionToSegment(conn: MotisConnection): Segment {
        const stops = conn.stops || [];
        const firstStop = stops[0];
        const lastStop = stops[stops.length - 1];
        const trip = conn.trips?.[0];

        const origin: SegmentStop = {
            name: firstStop?.station?.name || 'Unknown',
            uic: firstStop?.station?.id,
            coords: firstStop?.station?.pos
                ? [firstStop.station.pos.lat, firstStop.station.pos.lng]
                : undefined,
        };

        const destination: SegmentStop = {
            name: lastStop?.station?.name || 'Unknown',
            uic: lastStop?.station?.id,
            coords: lastStop?.station?.pos
                ? [lastStop.station.pos.lat, lastStop.station.pos.lng]
                : undefined,
        };

        const depTime = new Date((firstStop?.departure?.time || 0) * 1000);
        const arrTime = new Date((lastStop?.arrival?.time || 0) * 1000);
        const durationMin = Math.round((arrTime.getTime() - depTime.getTime()) / 60000);

        return {
            type: 'backbone',
            source: 'motis',
            origin,
            destination,
            departure: depTime.toISOString(),
            arrival: arrTime.toISOString(),
            duration_minutes: durationMin,
            train_number: trip?.id?.line_id || `${trip?.id?.train_nr || ''}`,
            operator: 'DB/ÖBB/SBB', // Will be enriched later
        };
    }

    /** Mock backbone segments for development without MOTIS running */
    private getMockBackboneSegments(
        originUic: string,
        destUic: string,
        departure: Date,
    ): Segment[] {
        this.logger.debug('Using mock backbone segments (MOTIS not available)');

        const depTime = new Date(departure);
        const arrTime = new Date(depTime.getTime() + 4 * 60 * 60 * 1000); // +4h

        return [
            {
                type: 'backbone',
                source: 'motis',
                origin: { name: 'Origin Hub', uic: originUic },
                destination: { name: 'Destination Hub', uic: destUic },
                departure: depTime.toISOString(),
                arrival: arrTime.toISOString(),
                duration_minutes: 240,
                train_number: 'ICE 1234',
                operator: 'DB Fernverkehr',
                route_type: 'ICE',
            },
        ];
    }

    /** Check if MOTIS is reachable */
    async isAvailable(): Promise<boolean> {
        try {
            const response = await fetch(`${this.motisUrl}/`, { signal: AbortSignal.timeout(3000) });
            return response.ok;
        } catch {
            return false;
        }
    }
}
