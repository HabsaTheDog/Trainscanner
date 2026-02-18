import type { SearchResponse, Station } from '../types';

const API_URL = import.meta.env.VITE_API_URL || '';

/** Search for routes between two stations */
export async function searchRoutes(
    origin: string,
    destination: string,
    departure: string,
): Promise<SearchResponse> {
    const response = await fetch(`${API_URL}/api/routes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ origin, destination, departure }),
    });

    if (!response.ok) {
        throw new Error(`Search failed: ${response.status} ${response.statusText}`);
    }

    return response.json();
}

/** Search stations for autocomplete */
export async function searchStations(query: string): Promise<Station[]> {
    if (!query || query.length < 2) return [];

    const response = await fetch(
        `${API_URL}/api/stations?q=${encodeURIComponent(query)}`,
    );

    if (!response.ok) return [];
    return response.json();
}

/** Check API health */
export async function checkHealth(): Promise<{
    status: string;
    services: Record<string, boolean>;
}> {
    const response = await fetch(`${API_URL}/health`);
    if (!response.ok) throw new Error('Health check failed');
    return response.json();
}
