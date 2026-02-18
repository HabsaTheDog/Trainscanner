// ── Shared Types (mirroring orchestrator types) ──

export interface Station {
    name: string;
    uic: string;
    country: string;
    coords: [number, number];
    type: 'hub' | 'stop';
}

export interface Segment {
    type: 'feeder' | 'backbone';
    source: 'ojp' | 'motis';
    origin: SegmentStop;
    destination: SegmentStop;
    departure: string;
    arrival: string;
    duration_minutes: number;
    train_number?: string;
    operator?: string;
    route_type?: string;
}

export interface SegmentStop {
    name: string;
    uic?: string;
    coords?: [number, number];
    platform?: string;
}

export interface Route {
    id: string;
    segments: Segment[];
    total_duration_minutes: number;
    total_transfers: number;
    departure: string;
    arrival: string;
    flags: RouteFlag[];
    reliability_score: number;
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

export interface SearchResponse {
    routes: Route[];
    search: {
        origin: { name: string; coords: [number, number]; nearest_hub?: string };
        destination: { name: string; coords: [number, number]; nearest_hub?: string };
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
