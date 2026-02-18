import { useState } from 'react';
import type { Route } from '../types';

interface Props {
    route: Route;
}

function formatTime(iso: string): string {
    const d = new Date(iso);
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function formatDuration(minutes: number): string {
    const h = Math.floor(minutes / 60);
    const m = minutes % 60;
    if (h === 0) return `${m} Min`;
    return `${h}h ${m}m`;
}

export default function RouteCard({ route }: Props) {
    const [expanded, setExpanded] = useState(false);

    const firstSegment = route.segments[0];
    const lastSegment = route.segments[route.segments.length - 1];

    return (
        <div className="route-card" onClick={() => setExpanded(!expanded)}>
            <div className="route-summary">
                <div className="route-times">
                    <div className="route-time">
                        <div className="time">{formatTime(route.departure)}</div>
                        <div className="station">{firstSegment.origin.name}</div>
                    </div>

                    <div className="route-arrow">
                        <span className="duration">{formatDuration(route.total_duration_minutes)}</span>
                        <div className="line" />
                        <span className="transfers">
                            {route.total_transfers === 0
                                ? 'Direkt'
                                : `${route.total_transfers} Umstieg${route.total_transfers > 1 ? 'e' : ''}`}
                        </span>
                    </div>

                    <div className="route-time">
                        <div className="time">{formatTime(route.arrival)}</div>
                        <div className="station">{lastSegment.destination.name}</div>
                    </div>
                </div>

                <div className="route-stats">
                    {route.reliability_score >= 0.8 && (
                        <span className="route-badge badge-reliable">✓ Zuverlässig</span>
                    )}
                    {route.flags
                        .filter((f) => f.type === 'tight_connection')
                        .map((f, i) => (
                            <span key={`tight-${i}`} className="route-badge badge-warning">
                                ⚡ {f.at_station}
                            </span>
                        ))}
                    {route.flags
                        .filter((f) => f.type === 'long_wait')
                        .map((f, i) => (
                            <span key={`wait-${i}`} className="route-badge badge-info">
                                ⏳ Wartezeit
                            </span>
                        ))}
                    {route.flags
                        .filter((f) => f.type === 'cross_border')
                        .map((f, i) => (
                            <span key={`border-${i}`} className="route-badge badge-info">
                                🌍 {f.message}
                            </span>
                        ))}
                </div>
            </div>

            {expanded && (
                <div className="segments">
                    {route.segments.map((seg, i) => (
                        <div key={i} className="segment">
                            <div className="segment-icon">
                                {seg.route_type === 'ICE'
                                    ? '🚄'
                                    : seg.route_type === 'IC'
                                        ? '🚃'
                                        : seg.route_type === 'S'
                                            ? '🚈'
                                            : '🚂'}
                            </div>
                            <div className="segment-details">
                                <div className="segment-train">
                                    {seg.train_number || 'Zug'}{' '}
                                    {seg.operator && (
                                        <span style={{ color: 'var(--text-muted)', fontWeight: 400 }}>
                                            ({seg.operator})
                                        </span>
                                    )}
                                </div>
                                <div className="segment-route">
                                    {seg.origin.name} → {seg.destination.name}
                                </div>
                                <div className="segment-time">
                                    {formatTime(seg.departure)} – {formatTime(seg.arrival)} ·{' '}
                                    {formatDuration(seg.duration_minutes)}
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
