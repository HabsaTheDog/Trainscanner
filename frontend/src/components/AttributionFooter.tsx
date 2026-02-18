import type { Attribution } from '../types';

interface Props {
    attributions: Attribution[];
}

export default function AttributionFooter({ attributions }: Props) {
    const defaultAttributions: Attribution[] = [
        {
            source: 'OpenStreetMap',
            license: 'ODbL',
            url: 'https://www.openstreetmap.org',
        },
        {
            source: 'MOTIS',
            license: 'MIT',
            url: 'https://motis-project.de',
        },
    ];

    const allAttributions = [
        ...defaultAttributions,
        ...attributions.filter(
            (a) => !defaultAttributions.some((d) => d.source === a.source),
        ),
    ];

    return (
        <footer className="attribution-footer">
            <div className="container attribution-content">
                <span>Datenquellen:</span>
                {allAttributions.map((attr) => (
                    <span key={attr.source} className="attribution-item">
                        ©{' '}
                        {attr.url ? (
                            <a href={attr.url} target="_blank" rel="noopener noreferrer">
                                {attr.source}
                            </a>
                        ) : (
                            attr.source
                        )}{' '}
                        ({attr.license})
                    </span>
                ))}
            </div>
        </footer>
    );
}
