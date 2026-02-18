// ── Shared TypeScript types for the Rail Meta-Router ──

/** A station from station_map.json */
export interface Station {
    name: string;
    uic: string;
    country: string;
    gtfs_ids: Record<string, string>;
    ojp_ref: string;
    coords: [number, number]; // [lat, lon]
    min_transfer_minutes: number;
    type: 'hub' | 'stop';
}

/** A single leg/segment of a journey */
export interface Segment {
    type: 'feeder' | 'backbone';
    source: 'ojp' | 'motis';
    origin: SegmentStop;
    destination: SegmentStop;
    departure: string; // ISO 8601
    arrival: string;   // ISO 8601
    duration_minutes: number;
    train_number?: string;
    operator?: string;
    operator_url?: string; // Deep-link to booking
    route_type?: string;   // ICE, IC, RE, S, etc.
}

export interface SegmentStop {
    name: string;
    uic?: string;
    coords?: [number, number];
    platform?: string;
}

/** A complete route (combination of segments) */
export interface Route {
    id: string;
    segments: Segment[];
    total_duration_minutes: number;
    total_transfers: number;
    departure: string;
    arrival: string;
    flags: RouteFlag[];
    reliability_score: number; // 0.0 - 1.0
    attribution: Attribution[];
}

export interface RouteFlag {
    type: 'tight_connection' | 'long_wait' | 'cross_border' | 'limited_coverage';
    message: string;
    at_station?: string;
}

export interface Attribution {
    source: string;
    license: string;
    url?: string;
}

/** Search request */
export interface SearchRequest {
    origin: string;
    destination: string;
    departure: string;   // ISO 8601 datetime
    arrival?: string;     // ISO 8601 datetime (for arrive-by searches)
    max_transfers?: number;
    max_results?: number;
}

/** Search response */
export interface SearchResponse {
    routes: Route[];
    search: {
        origin: ResolvedStation;
        destination: ResolvedStation;
        departure: string;
        searched_at: string;
    };
    meta: {
        motis_available: boolean;
        ojp_mode: string;
        total_results: number;
        computation_ms: number;
    };
}

export interface ResolvedStation {
    name: string;
    uic?: string;
    coords: [number, number];
    nearest_hub?: string;
}
