import type { Route, SearchResponse } from '../types';
import RouteCard from './RouteCard';

interface Props {
    routes: Route[];
    meta: SearchResponse['meta'];
    search: SearchResponse['search'];
}

export default function RouteList({ routes, meta, search }: Props) {
    if (routes.length === 0) {
        return (
            <section className="results-section">
                <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                    Keine Verbindungen gefunden für {search.origin.name} → {search.destination.name}
                </div>
            </section>
        );
    }

    return (
        <section className="results-section">
            <div className="results-header">
                <h2>
                    {search.origin.name} → {search.destination.name}
                </h2>
                <span className="results-meta">
                    {meta.total_results} Verbindung{meta.total_results !== 1 ? 'en' : ''} in{' '}
                    {meta.computation_ms}ms
                    {!meta.motis_available && ' (Mock-Modus)'}
                </span>
            </div>

            {routes.map((route) => (
                <RouteCard key={route.id} route={route} />
            ))}
        </section>
    );
}
