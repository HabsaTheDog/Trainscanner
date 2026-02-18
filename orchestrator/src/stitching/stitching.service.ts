import { Injectable, Logger } from '@nestjs/common';
import { StationsService } from '../stations/stations.service';
import { Segment, Route, RouteFlag, Attribution } from '../common/types';
import { randomUUID } from 'crypto';

interface StitchInput {
    feederSegments: Segment[];
    backboneSegments: Segment[];
    distributorSegments: Segment[];
}

@Injectable()
export class StitchingService {
    private readonly logger = new Logger(StitchingService.name);

    constructor(private readonly stationsService: StationsService) { }

    /**
     * Combine feeder, backbone, and distributor segments into complete routes.
     * Validates transfer times and ranks results.
     */
    stitchRoutes(input: StitchInput, maxResults: number = 5): Route[] {
        const { feederSegments, backboneSegments, distributorSegments } = input;

        const routes: Route[] = [];

        // For each backbone segment, try to combine with feeders and distributors
        for (const backbone of backboneSegments) {
            // Find compatible feeder (arrives before backbone departs)
            const compatibleFeeders = this.findCompatiblePreceding(
                feederSegments,
                backbone,
            );

            // Find compatible distributors (departs after backbone arrives)
            const compatibleDistributors = this.findCompatibleFollowing(
                backbone,
                distributorSegments,
            );

            // If no feeders/distributors needed (hub-to-hub), create route with just backbone
            if (feederSegments.length === 0 && distributorSegments.length === 0) {
                routes.push(this.createRoute([backbone]));
                continue;
            }

            // Combine: feeder(s) + backbone + distributor(s)
            for (const feeder of compatibleFeeders.length > 0 ? compatibleFeeders : [null]) {
                for (const distributor of compatibleDistributors.length > 0 ? compatibleDistributors : [null]) {
                    const segments: Segment[] = [];
                    const flags: RouteFlag[] = [];

                    if (feeder) {
                        segments.push(feeder);
                        const transferFlags = this.validateTransfer(feeder, backbone);
                        flags.push(...transferFlags);
                    }

                    segments.push(backbone);

                    if (distributor) {
                        const transferFlags = this.validateTransfer(backbone, distributor);
                        flags.push(...transferFlags);
                        segments.push(distributor);
                    }

                    // Only add if no invalid transfers
                    if (!flags.some((f) => f.type === 'tight_connection' && f.message.includes('REJECTED'))) {
                        routes.push(this.createRoute(segments, flags));
                    }
                }
            }
        }

        // If no backbone segments (direct feeder route)
        if (backboneSegments.length === 0 && feederSegments.length > 0) {
            for (const feeder of feederSegments) {
                routes.push(this.createRoute([feeder]));
            }
        }

        // Rank and limit
        return this.rankRoutes(routes).slice(0, maxResults);
    }

    /** Find feeder segments that arrive before the backbone departs, respecting transfer time */
    private findCompatiblePreceding(
        feeders: Segment[],
        backbone: Segment,
    ): Segment[] {
        return feeders.filter((feeder) => {
            const feederArrival = new Date(feeder.arrival).getTime();
            const backboneDeparture = new Date(backbone.departure).getTime();
            const gap = (backboneDeparture - feederArrival) / 60000; // minutes

            const minTransfer = this.getMinTransferTime(feeder.destination.uic);
            return gap >= minTransfer; // Must have at least minimum transfer time
        });
    }

    /** Find distributor segments that depart after the backbone arrives, respecting transfer time */
    private findCompatibleFollowing(
        backbone: Segment,
        distributors: Segment[],
    ): Segment[] {
        return distributors.filter((dist) => {
            const backboneArrival = new Date(backbone.arrival).getTime();
            const distDeparture = new Date(dist.departure).getTime();
            const gap = (distDeparture - backboneArrival) / 60000;

            const minTransfer = this.getMinTransferTime(dist.origin.uic);
            return gap >= minTransfer;
        });
    }

    /** Validate transfer between two consecutive segments */
    private validateTransfer(arriving: Segment, departing: Segment): RouteFlag[] {
        const flags: RouteFlag[] = [];
        const arrivalTime = new Date(arriving.arrival).getTime();
        const departureTime = new Date(departing.departure).getTime();
        const gap = (departureTime - arrivalTime) / 60000; // minutes

        const stationName = arriving.destination.name || departing.origin.name;
        const minTransfer = this.getMinTransferTime(arriving.destination.uic);

        if (gap < minTransfer) {
            flags.push({
                type: 'tight_connection',
                message: `REJECTED: Only ${Math.round(gap)} min transfer at ${stationName} (min: ${minTransfer} min)`,
                at_station: stationName,
            });
        } else if (gap < minTransfer * 1.2) {
            flags.push({
                type: 'tight_connection',
                message: `Tight connection: ${Math.round(gap)} min at ${stationName} (min: ${minTransfer} min)`,
                at_station: stationName,
            });
        }

        if (gap > 120) {
            flags.push({
                type: 'long_wait',
                message: `Long wait: ${Math.round(gap)} min at ${stationName}`,
                at_station: stationName,
            });
        }

        return flags;
    }

    /** Create a Route object from segments */
    private createRoute(segments: Segment[], extraFlags: RouteFlag[] = []): Route {
        const first = segments[0];
        const last = segments[segments.length - 1];

        const departure = first.departure;
        const arrival = last.arrival;
        const totalDuration = Math.round(
            (new Date(arrival).getTime() - new Date(departure).getTime()) / 60000,
        );
        const totalTransfers = Math.max(0, segments.length - 1);

        // Calculate reliability score
        const tightConnections = extraFlags.filter(
            (f) => f.type === 'tight_connection' && !f.message.includes('REJECTED'),
        ).length;
        const reliabilityScore = Math.max(0, 1.0 - tightConnections * 0.2);

        // Detect cross-border
        const flags = [...extraFlags];
        const countries = new Set(
            segments
                .map((s) => this.detectCountry(s))
                .filter(Boolean),
        );
        if (countries.size > 1) {
            flags.push({
                type: 'cross_border',
                message: `Cross-border: ${Array.from(countries).join(' → ')}`,
            });
        }

        // Build attribution
        const attribution = this.buildAttribution(segments);

        return {
            id: randomUUID(),
            segments,
            total_duration_minutes: totalDuration,
            total_transfers: totalTransfers,
            departure,
            arrival,
            flags,
            reliability_score: reliabilityScore,
            attribution,
        };
    }

    /** Rank routes by total time, transfers, and reliability */
    private rankRoutes(routes: Route[]): Route[] {
        return routes.sort((a, b) => {
            // Primary: fewer transfers
            if (a.total_transfers !== b.total_transfers) {
                return a.total_transfers - b.total_transfers;
            }
            // Secondary: shorter total time
            if (a.total_duration_minutes !== b.total_duration_minutes) {
                return a.total_duration_minutes - b.total_duration_minutes;
            }
            // Tertiary: higher reliability
            return b.reliability_score - a.reliability_score;
        });
    }

    private getMinTransferTime(stationUic?: string): number {
        if (!stationUic) return 5; // Default
        return this.stationsService.getMinTransferTime(stationUic);
    }

    private detectCountry(segment: Segment): string | null {
        const name = segment.operator || '';
        if (name.includes('DB') || name.includes('Regio Bayern')) return 'DE';
        if (name.includes('ÖBB')) return 'AT';
        if (name.includes('SBB') || name.includes('BLS')) return 'CH';
        return null;
    }

    private buildAttribution(segments: Segment[]): Attribution[] {
        const attributions: Attribution[] = [];
        const sources = new Set<string>();

        for (const seg of segments) {
            if (seg.source === 'motis' && !sources.has('motis')) {
                sources.add('motis');
                attributions.push({
                    source: 'MOTIS',
                    license: 'MIT License',
                    url: 'https://motis-project.de',
                });
            }
            if (seg.source === 'ojp') {
                const country = this.detectCountry(seg);
                if (country === 'DE' && !sources.has('de')) {
                    sources.add('de');
                    attributions.push({
                        source: 'DB / Mobilithek',
                        license: 'CC-BY 4.0',
                        url: 'https://mobilithek.info',
                    });
                }
                if (country === 'CH' && !sources.has('ch')) {
                    sources.add('ch');
                    attributions.push({
                        source: 'SBB / opentransportdata.swiss',
                        license: 'Nutzungsbedingungen',
                        url: 'https://opentransportdata.swiss',
                    });
                }
                if (country === 'AT' && !sources.has('at')) {
                    sources.add('at');
                    attributions.push({
                        source: 'ÖBB / mobilitaetsdaten.gv.at',
                        license: 'Open Data',
                        url: 'https://mobilitaetsdaten.gv.at',
                    });
                }
            }
        }

        return attributions;
    }
}
