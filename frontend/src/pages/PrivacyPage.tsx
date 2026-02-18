export default function PrivacyPage() {
    return (
        <div className="container privacy-page">
            <h1>Datenschutzerklärung</h1>

            <h2>1. Verantwortlicher</h2>
            <p>
                [Name/Unternehmen]<br />
                [Adresse]<br />
                E-Mail: [email@example.com]
            </p>

            <h2>2. Zweck der Datenverarbeitung</h2>
            <p>
                Diese Website dient der Berechnung von Zugverbindungen in Europa.
                Dabei werden ausschließlich die für die Routenberechnung notwendigen
                Daten verarbeitet.
            </p>

            <h2>3. Verarbeitete Daten</h2>
            <ul>
                <li>
                    <strong>Suchanfragen:</strong> Start- und Zielort sowie gewünschte
                    Abfahrtszeit. Diese Daten werden ausschließlich zur
                    Routenberechnung verwendet und nicht gespeichert.
                </li>
                <li>
                    <strong>Standortdaten:</strong> Falls die Standortfunktion des
                    Browsers genutzt wird, werden Standortdaten ausschließlich
                    clientseitig verarbeitet. Es erfolgt keine Übertragung an unseren
                    Server.
                </li>
                <li>
                    <strong>Technische Daten:</strong> IP-Adresse (in Server-Logs,
                    automatisch nach 24h gelöscht), Browser-Typ, Betriebssystem.
                </li>
            </ul>

            <h2>4. Cookies</h2>
            <p>
                Diese Website verwendet ausschließlich technisch notwendige Cookies:
            </p>
            <ul>
                <li>
                    <strong>cookies_accepted:</strong> Speichert, ob der Cookie-Banner
                    bestätigt wurde (localStorage, kein Server-Zugriff).
                </li>
            </ul>
            <p>
                Es werden keine Tracking-Cookies, Analyse-Cookies oder
                Werbe-Cookies eingesetzt.
            </p>

            <h2>5. Rechtsgrundlage</h2>
            <p>
                Die Verarbeitung erfolgt auf Grundlage von Art. 6 Abs. 1 lit. f
                DSGVO (berechtigtes Interesse an der Bereitstellung eines
                Routenplaners).
            </p>

            <h2>6. Datenweitergabe an Dritte</h2>
            <p>
                Zur Routenberechnung werden Anfragen an folgende offizielle
                Datenquellen weitergeleitet:
            </p>
            <ul>
                <li>Mobilithek (Deutsche Bahn) — mobilithek.info</li>
                <li>opentransportdata.swiss (SBB) — opentransportdata.swiss</li>
                <li>mobilitaetsdaten.gv.at (ÖBB) — mobilitaetsdaten.gv.at</li>
            </ul>
            <p>
                Dabei werden ausschließlich die Suchanfrage-Parameter (Start, Ziel,
                Zeit) übermittelt, keine personenbezogenen Daten.
            </p>

            <h2>7. Ihre Rechte</h2>
            <p>Sie haben das Recht auf:</p>
            <ul>
                <li>Auskunft über die verarbeiteten Daten (Art. 15 DSGVO)</li>
                <li>Berichtigung unrichtiger Daten (Art. 16 DSGVO)</li>
                <li>Löschung Ihrer Daten (Art. 17 DSGVO)</li>
                <li>Einschränkung der Verarbeitung (Art. 18 DSGVO)</li>
                <li>Widerspruch gegen die Verarbeitung (Art. 21 DSGVO)</li>
                <li>
                    Beschwerde bei einer Aufsichtsbehörde (Art. 77 DSGVO)
                </li>
            </ul>

            <h2>8. Aufbewahrungsdauer</h2>
            <p>
                Aggregierte Nutzungsstatistiken (ohne Personenbezug) werden maximal
                30 Tage aufbewahrt. Server-Logs werden nach 24 Stunden automatisch
                gelöscht.
            </p>

            <p style={{ marginTop: '2rem', color: 'var(--text-muted)' }}>
                Stand: Februar 2026
            </p>
        </div>
    );
}
