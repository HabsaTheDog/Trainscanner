import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import { Station } from '../common/types';

@Injectable()
export class StationsService implements OnModuleInit {
    private readonly logger = new Logger(StationsService.name);
    private stations: Station[] = [];
    private stationsByUic: Map<string, Station> = new Map();
    private stationsByName: Map<string, Station> = new Map();

    onModuleInit() {
        this.loadStations();
    }

    private loadStations() {
        const stationMapPath = path.resolve(
            process.env.STATION_MAP_PATH || '/app/data/station_map.json',
        );

        // Fallback to local dev path
        const paths = [
            stationMapPath,
            path.resolve(__dirname, '../../../data/station_map.json'),
            path.resolve(process.cwd(), 'data/station_map.json'),
            path.resolve(process.cwd(), '../data/station_map.json'),
        ];

        let loaded = false;
        for (const p of paths) {
            if (fs.existsSync(p)) {
                const raw = fs.readFileSync(p, 'utf-8');
                this.stations = JSON.parse(raw);
                loaded = true;
                this.logger.log(`Loaded ${this.stations.length} stations from ${p}`);
                break;
            }
        }

        if (!loaded) {
            this.logger.warn('No station_map.json found. Using empty station list.');
            this.stations = [];
        }

        // Build indexes
        for (const station of this.stations) {
            this.stationsByUic.set(station.uic, station);
            this.stationsByName.set(station.name.toLowerCase(), station);
        }
    }

    /** Get all stations */
    getAll(): Station[] {
        return this.stations;
    }

    /** Get all hub stations */
    getHubs(): Station[] {
        return this.stations.filter((s) => s.type === 'hub');
    }

    /** Find a station by UIC code */
    findByUic(uic: string): Station | undefined {
        return this.stationsByUic.get(uic);
    }

    /** Find a station by name (case-insensitive, partial match) */
    findByName(name: string): Station | undefined {
        const lower = name.toLowerCase();

        // Exact match first
        if (this.stationsByName.has(lower)) {
            return this.stationsByName.get(lower);
        }

        // Partial match
        for (const station of this.stations) {
            if (station.name.toLowerCase().includes(lower)) {
                return station;
            }
        }

        return undefined;
    }

    /** Search stations by name (returns all partial matches) */
    searchByName(query: string): Station[] {
        const lower = query.toLowerCase();
        return this.stations.filter((s) =>
            s.name.toLowerCase().includes(lower),
        );
    }

    /** Find nearest hub to given coordinates */
    findNearestHub(lat: number, lon: number): Station | undefined {
        const hubs = this.getHubs();
        if (hubs.length === 0) return undefined;

        let nearest: Station | undefined;
        let minDist = Infinity;

        for (const hub of hubs) {
            const dist = this.haversineDistance(lat, lon, hub.coords[0], hub.coords[1]);
            if (dist < minDist) {
                minDist = dist;
                nearest = hub;
            }
        }

        return nearest;
    }

    /** Calculate haversine distance in km */
    private haversineDistance(
        lat1: number, lon1: number,
        lat2: number, lon2: number,
    ): number {
        const R = 6371; // Earth radius in km
        const dLat = this.toRad(lat2 - lat1);
        const dLon = this.toRad(lon2 - lon1);
        const a =
            Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(this.toRad(lat1)) *
            Math.cos(this.toRad(lat2)) *
            Math.sin(dLon / 2) *
            Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private toRad(deg: number): number {
        return deg * (Math.PI / 180);
    }

    /** Get GTFS ID for a station in a specific country */
    getGtfsId(station: Station, country: string): string | undefined {
        return station.gtfs_ids[country.toLowerCase()];
    }

    /** Get minimum transfer time at a station (in minutes) */
    getMinTransferTime(stationUic: string): number {
        const station = this.stationsByUic.get(stationUic);
        return station?.min_transfer_minutes ?? 5; // Default 5 min
    }
}
