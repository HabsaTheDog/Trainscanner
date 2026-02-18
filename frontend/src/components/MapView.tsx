import { useEffect, useRef } from 'react';
import type { Route } from '../types';

interface Props {
    routes: Route[];
}

export default function MapView({ routes }: Props) {
    const mapRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!mapRef.current || routes.length === 0) return;

        // Collect all coordinates from route segments
        const coords: [number, number][] = [];
        for (const route of routes) {
            for (const seg of route.segments) {
                if (seg.origin.coords) coords.push(seg.origin.coords);
                if (seg.destination.coords) coords.push(seg.destination.coords);
            }
        }

        // Try to load MapLibre (graceful degradation if not available)
        import('maplibre-gl')
            .then(({ default: maplibregl }) => {
                if (!mapRef.current) return;

                // Clear any existing map
                mapRef.current.innerHTML = '';

                const map = new maplibregl.Map({
                    container: mapRef.current,
                    style: {
                        version: 8,
                        sources: {
                            'osm-tiles': {
                                type: 'raster',
                                tiles: [
                                    'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                                ],
                                tileSize: 256,
                                attribution:
                                    '© <a href="https://www.openstreetmap.org">OpenStreetMap</a> contributors, © <a href="https://carto.com">CARTO</a>',
                            },
                        },
                        layers: [
                            {
                                id: 'osm-tiles',
                                type: 'raster',
                                source: 'osm-tiles',
                                minzoom: 0,
                                maxzoom: 19,
                            },
                        ],
                    },
                    center: [10.5, 49.5], // Center of DACH region
                    zoom: 5,
                });

                map.addControl(new maplibregl.NavigationControl(), 'top-right');

                // Add markers for stations
                if (coords.length > 0) {
                    for (const [lat, lon] of coords) {
                        new maplibregl.Marker({ color: '#3b82f6' })
                            .setLngLat([lon, lat])
                            .addTo(map);
                    }

                    // Fit bounds
                    if (coords.length >= 2) {
                        const lngs = coords.map(([, lon]) => lon);
                        const lats = coords.map(([lat]) => lat);
                        map.fitBounds(
                            [
                                [Math.min(...lngs) - 0.5, Math.min(...lats) - 0.5],
                                [Math.max(...lngs) + 0.5, Math.max(...lats) + 0.5],
                            ],
                            { padding: 50, maxZoom: 10 },
                        );
                    }

                    // Draw route lines
                    map.on('load', () => {
                        const lineCoords = coords.map(([lat, lon]) => [lon, lat]);
                        if (lineCoords.length >= 2) {
                            map.addSource('route', {
                                type: 'geojson',
                                data: {
                                    type: 'Feature',
                                    properties: {},
                                    geometry: {
                                        type: 'LineString',
                                        coordinates: lineCoords,
                                    },
                                },
                            });
                            map.addLayer({
                                id: 'route-line',
                                type: 'line',
                                source: 'route',
                                layout: { 'line-join': 'round', 'line-cap': 'round' },
                                paint: {
                                    'line-color': '#3b82f6',
                                    'line-width': 3,
                                    'line-opacity': 0.8,
                                    'line-dasharray': [2, 2],
                                },
                            });
                        }
                    });
                }

                return () => map.remove();
            })
            .catch(() => {
                // MapLibre not available — show fallback
                if (mapRef.current) {
                    mapRef.current.innerHTML = `
            <div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);font-size:var(--font-size-sm);">
              🗺️ Karte nicht verfügbar (MapLibre wird geladen...)
            </div>
          `;
                }
            });
    }, [routes]);

    return <div ref={mapRef} className="map-container" />;
}
