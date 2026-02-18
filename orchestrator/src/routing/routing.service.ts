import { Injectable, Logger } from '@nestjs/common';
import { MotisService } from '../motis/motis.service';
import { OjpService, OjpCountry } from '../ojp/ojp.service';
import { StationsService } from '../stations/stations.service';
import { StitchingService } from '../stitching/stitching.service';
import { SearchRequestDto } from '../common/dto/search-request.dto';
import { SearchResponse, Station, ResolvedStation } from '../common/types';

@Injectable()
export class RoutingService {
    private readonly logger = new Logger(RoutingService.name);

    constructor(
        private readonly motisService: MotisService,
        private readonly ojpService: OjpService,
        private readonly stationsService: StationsService,
        private readonly stitchingService: StitchingService,
    ) { }

    /**
     * Main orchestration flow:
     * 1. Resolve origin/destination stations
     * 2. Identify nearest hubs
     * 3. Query OJP for feeder/distributor segments (parallel)
     * 4. Query MOTIS for backbone segments
     * 5. Stitch everything together
     */
    async search(dto: SearchRequestDto): Promise<SearchResponse> {
        const startTime = Date.now();

        // 1. Resolve stations
        const originStation = this.stationsService.findByName(dto.origin);
        const destStation = this.stationsService.findByName(dto.destination);

        const resolvedOrigin: ResolvedStation = {
            name: dto.origin,
            uic: originStation?.uic,
            coords: originStation?.coords || [0, 0],
            nearest_hub: undefined,
        };

        const resolvedDest: ResolvedStation = {
            name: dto.destination,
            uic: destStation?.uic,
            coords: destStation?.coords || [0, 0],
            nearest_hub: undefined,
        };

        // 2. Determine if origin/destination are hubs or need feeders
        const originIsHub = originStation?.type === 'hub';
        const destIsHub = destStation?.type === 'hub';

        // Find nearest hubs if origin/dest are not hubs themselves
        let originHub = originStation;
        let destHub = destStation;

        if (!originIsHub && originStation) {
            originHub = this.stationsService.findNearestHub(
                originStation.coords[0],
                originStation.coords[1],
            );
            resolvedOrigin.nearest_hub = originHub?.name;
        } else if (!originStation) {
            // Unknown station — use first hub as fallback for demo
            const hubs = this.stationsService.getHubs();
            originHub = hubs[0];
            resolvedOrigin.nearest_hub = originHub?.name;
        }

        if (!destIsHub && destStation) {
            destHub = this.stationsService.findNearestHub(
                destStation.coords[0],
                destStation.coords[1],
            );
            resolvedDest.nearest_hub = destHub?.name;
        } else if (!destStation) {
            const hubs = this.stationsService.getHubs();
            destHub = hubs.length > 1 ? hubs[1] : hubs[0];
            resolvedDest.nearest_hub = destHub?.name;
        }

        const departure = new Date(dto.departure);
        const maxResults = dto.max_results || 5;

        this.logger.log(
            `Searching: ${dto.origin} → ${dto.destination} | ` +
            `Hubs: ${originHub?.name} → ${destHub?.name} | ` +
            `Departure: ${departure.toISOString()}`,
        );

        // 3. Query in parallel: OJP feeders + MOTIS backbone
        const originCountry = (originStation?.country?.toLowerCase() || 'de') as OjpCountry;
        const destCountry = (destStation?.country?.toLowerCase() || 'de') as OjpCountry;

        const [feederSegments, backboneSegments, distributorSegments, motisAvailable] =
            await Promise.all([
                // Feeder: origin → origin hub
                !originIsHub && originHub
                    ? this.ojpService.findFeederConnections(
                        dto.origin,
                        originHub.name,
                        originCountry,
                        departure,
                    )
                    : Promise.resolve([]),

                // Backbone: origin hub → dest hub
                originHub && destHub
                    ? this.motisService.findConnections(
                        originHub.uic,
                        destHub.uic,
                        departure,
                        maxResults,
                    )
                    : Promise.resolve([]),

                // Distributor: dest hub → destination
                !destIsHub && destHub
                    ? this.ojpService.findDistributorConnections(
                        destHub.name,
                        dto.destination,
                        destCountry,
                        new Date(departure.getTime() + 4 * 60 * 60 * 1000), // ~4h after departure
                    )
                    : Promise.resolve([]),

                // Check MOTIS availability
                this.motisService.isAvailable(),
            ]);

        // 4. Stitch segments together
        const routes = this.stitchingService.stitchRoutes(
            {
                feederSegments,
                backboneSegments,
                distributorSegments,
            },
            maxResults,
        );

        const computationMs = Date.now() - startTime;
        this.logger.log(
            `Found ${routes.length} routes in ${computationMs}ms ` +
            `(feeders: ${feederSegments.length}, backbone: ${backboneSegments.length}, ` +
            `distributors: ${distributorSegments.length})`,
        );

        return {
            routes,
            search: {
                origin: resolvedOrigin,
                destination: resolvedDest,
                departure: departure.toISOString(),
                searched_at: new Date().toISOString(),
            },
            meta: {
                motis_available: motisAvailable,
                ojp_mode: process.env.OJP_MODE || 'mock',
                total_results: routes.length,
                computation_ms: computationMs,
            },
        };
    }

    /** Search for stations by name (for autocomplete) */
    searchStations(query: string): Station[] {
        if (!query || query.length < 2) return [];
        return this.stationsService.searchByName(query).slice(0, 10);
    }
}
