import { useState, useCallback } from 'react';
import SearchForm from '../components/SearchForm';
import RouteList from '../components/RouteList';
import MapView from '../components/MapView';
import { searchRoutes } from '../services/api';
import type { SearchResponse, Attribution } from '../types';

interface Props {
    onAttributionsChange: (attributions: Attribution[]) => void;
}

export default function SearchPage({ onAttributionsChange }: Props) {
    const [results, setResults] = useState<SearchResponse | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleSearch = useCallback(
        async (origin: string, destination: string, departure: string) => {
            setLoading(true);
            setError(null);
            setResults(null);

            try {
                const response = await searchRoutes(origin, destination, departure);
                setResults(response);

                // Aggregate attributions from all routes
                const allAttr: Attribution[] = [];
                const seen = new Set<string>();
                for (const route of response.routes) {
                    for (const attr of route.attribution) {
                        if (!seen.has(attr.source)) {
                            seen.add(attr.source);
                            allAttr.push(attr);
                        }
                    }
                }
                onAttributionsChange(allAttr);
            } catch (err) {
                setError(
                    err instanceof Error ? err.message : 'Verbindung fehlgeschlagen',
                );
            } finally {
                setLoading(false);
            }
        },
        [onAttributionsChange],
    );

    return (
        <div className="container">
            <section className="hero">
                <h1>Zugverbindungen finden</h1>
                <p>
                    Europaweiter Zugplaner mit Virtual Interlining — kombiniert
                    verschiedene Betreiber zu einer optimalen Route.
                </p>
            </section>

            <SearchForm onSearch={handleSearch} loading={loading} />

            {error && (
                <div
                    style={{
                        textAlign: 'center',
                        padding: '2rem',
                        color: 'var(--danger)',
                    }}
                >
                    ⚠️ {error}
                </div>
            )}

            {loading && (
                <div className="loading">
                    <div className="spinner" />
                </div>
            )}

            {results && (
                <>
                    <MapView routes={results.routes} />
                    <RouteList
                        routes={results.routes}
                        meta={results.meta}
                        search={results.search}
                    />
                </>
            )}
        </div>
    );
}
