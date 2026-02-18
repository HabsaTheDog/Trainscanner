import { useState } from 'react';

interface Props {
    onSearch: (origin: string, destination: string, departure: string) => void;
    loading: boolean;
}

export default function SearchForm({ onSearch, loading }: Props) {
    const [origin, setOrigin] = useState('');
    const [destination, setDestination] = useState('');
    const [date, setDate] = useState(() => {
        const now = new Date();
        return now.toISOString().split('T')[0];
    });
    const [time, setTime] = useState(() => {
        const now = new Date();
        return `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    });

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (!origin || !destination) return;
        const departure = `${date}T${time}:00`;
        onSearch(origin, destination, departure);
    };

    const swapStations = () => {
        setOrigin(destination);
        setDestination(origin);
    };

    return (
        <form className="search-card" onSubmit={handleSubmit}>
            <div className="search-fields">
                <div className="search-field">
                    <label htmlFor="origin">Von</label>
                    <input
                        id="origin"
                        type="text"
                        placeholder="z.B. München Hbf"
                        value={origin}
                        onChange={(e) => setOrigin(e.target.value)}
                        required
                    />
                </div>

                <div className="search-field" style={{ position: 'relative' }}>
                    <label htmlFor="destination">Nach</label>
                    <input
                        id="destination"
                        type="text"
                        placeholder="z.B. Wien Hbf"
                        value={destination}
                        onChange={(e) => setDestination(e.target.value)}
                        required
                    />
                    <button
                        type="button"
                        onClick={swapStations}
                        title="Stationen tauschen"
                        style={{
                            position: 'absolute',
                            left: '-28px',
                            top: '50%',
                            background: 'var(--bg-glass)',
                            border: '1px solid var(--border)',
                            borderRadius: 'var(--radius-full)',
                            width: '28px',
                            height: '28px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            cursor: 'pointer',
                            color: 'var(--text-secondary)',
                            fontSize: '14px',
                            transition: 'all var(--transition-fast)',
                        }}
                    >
                        ⇄
                    </button>
                </div>

                <div className="search-field">
                    <label htmlFor="date">Datum</label>
                    <input
                        id="date"
                        type="date"
                        value={date}
                        onChange={(e) => setDate(e.target.value)}
                        required
                    />
                </div>

                <div className="search-field">
                    <label htmlFor="time">Uhrzeit</label>
                    <input
                        id="time"
                        type="time"
                        value={time}
                        onChange={(e) => setTime(e.target.value)}
                        required
                    />
                </div>
            </div>

            <button
                type="submit"
                className="btn btn-primary search-btn"
                disabled={loading || !origin || !destination}
            >
                {loading ? '🔍 Suche läuft...' : '🚂 Verbindung suchen'}
            </button>
        </form>
    );
}
