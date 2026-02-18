# Project: European Rail Meta-Router (Compliance-Edition 2026)
**Status:** Concept (Legal/Open Data) | **Engine:** MOTIS v2 | **Architecture:** Hybrid Hub-and-Spoke

---

## 1. Executive Summary
Entwicklung eines ressourceneffizienten, europaweiten Zug-Routenplaners mit Fokus auf "Virtual Interlining" (Kombination verschiedener Betreiber).
Im Gegensatz zu Grauzonen-Lösungen (Scraping) setzt diese Architektur auf **offizielle Open-Data-Standards (GTFS/OJP)** und die **National Access Points (NAPs)** der EU, um Rechtssicherheit zu gewährleisten und IP-Sperren zu vermeiden.

**Kern-Strategie:**
* **Long-Haul (Backbone):** Self-hosted MOTIS Instanz basierend auf statischen GTFS-Daten (Fernverkehr).
* **Last-Mile (Feeder):** Live-Abfrage offizieller APIs via OJP (Open Journey Planner) Standard.
* **Compliance:** Nutzung von Open Data Lizenzen (CC-BY, ODbL) und Deep-Linking statt Ticket-Reselling.

**Rollout-Strategie:** Phasenbasiert mit **DACH-First MVP** (Deutschland, Österreich, Schweiz), da diese Länder die ausgereifteste OJP/GTFS-Infrastruktur bieten.

---

## 2. System Architektur

Wir nutzen einen **hybriden Ansatz**, um Serverkosten zu minimieren und Legalität zu maximieren.

### A. Der Backbone (Self-Hosted MOTIS)
* **Zweck:** Berechnet das Grundgerüst der Reise (z.B. Hamburg -> Mailand).
* **Datenbasis:** Kuratierte GTFS-Feeds (nur Rail/Long-Distance) + Island-OSM (Bahnhofsumgebungen).
* **Technik:** MOTIS v2 (C++) mit `routing` und `intermodal` Modulen.

### B. Die Feeder (External Compliance APIs)
* **Zweck:** Findet den Weg vom Dorf zum Hub (z.B. Hintertupfingen -> München Hbf).
* **Protokoll:** **OJP (Open Journey Planner)** / TRIAS.
* **Quellen:** Offizielle National Access Points (z.B. Mobilithek.de, transport.data.gouv.fr).
* **Vorteil:** Offiziell erlaubt, dokumentiert, keine "Katz-und-Maus"-Spiele mit Firewalls.
* **Fallback:** Für Länder ohne OJP-Endpunkt → statische GTFS-Daten direkt in MOTIS laden (reduzierte Funktionalität, aber volle Abdeckung).

### C. Der Orchestrator (Middleware)
* **Tech:** TypeScript (NestJS).
* **Aufgabe:**
    1.  Empfängt User-Anfrage `(Start -> Ziel)`.
    2.  Identifiziert "Hubs" via PostGIS.
    3.  Fragt parallel OJP-Schnittstellen für Zubringer an.
    4.  Fragt MOTIS für die Hauptstrecke an.
    5.  Kombiniert Segmente via **Stitching Engine** (→ siehe Abschnitt 6).
    6.  Prüft Echtzeit-Status via GTFS-RT.

### D. Station Resolution (Manuell kuratiert + GTFS-Diff)
* **Zweck:** Löst das fundamentale Problem inkompatibler Stations-IDs zwischen Betreibern.
* **Ansatz:** **Manuell kuratierte JSON-Datei** (`station_map.json`) im Repository.
* **Begründung:** Bahnhöfe und Stationen ändern sich extrem selten (wenige Neueröffnungen pro Jahr im gesamten DACH-Raum). Eine Datenbank-Lösung wäre Over-Engineering — ein manuell gepflegtes Mapping-File ist zuverlässiger, transparenter und zu 100% korrekt.
* **Mapping pro Eintrag:**
    ```json
    {
      "name": "München Hbf",
      "uic": "8000261",
      "gtfs_ids": { "de": "8000261", "at": "...", "ch": "..." },
      "ojp_ref": "de:09162:6",
      "coords": [48.1402, 11.5600],
      "min_transfer_minutes": 10,
      "type": "hub"
    }
    ```
* **Laufende Pflege:** Über ein **GTFS-Diff-Script** (→ siehe §4.3), das Änderungen in den Feeds automatisch erkennt und neue/geänderte Stops zur manuellen Kuratierung flaggt.
* **Aufwand:** Initiales Mapping DACH ~2–3 Tage, laufende Pflege ~5 Min. pro Update-Zyklus.

---

## 3. Tech Stack Übersicht

| Komponente | Technologie | Details & Begründung |
| :--- | :--- | :--- |
| **Routing Engine** | **MOTIS** (C++) | Core-System. Module: `routing`, `intermodal`, `ppr` (Pedestrian), `rt` (Realtime). |
| **Orchestrator** | **TypeScript (NestJS)** | Typsichere API-Logik. Ersetzt `hafas-client` durch `ojp-js` Adapter. |
| **Protocol** | **OJP / GTFS-RT** | Offizielle EU-Standards für Fahrplanauskunft und Echtzeitdaten. |
| **ID-Resolver** | **JSON-Datei (manuell kuratiert)** | `station_map.json` — manuell gepflegtes Multi-Key Mapping. GTFS-Diff-Script flaggt Änderungen. |
| **Caching** | **Redis** | Cacht externe OJP-Anfragen (TTL: 15min) und Route-Lookups. |
| **Geocoding** | **Photon** | OSM-basierter Geocoder (Lizenzfrei, ODbL). |
| **Frontend Map** | **MapLibre GL JS** | Vektor-Tiles via Protomaps (Serverless, kosteneffizient). |
| **Rate Limiter** | **Redis + Bull Queue** | Pro-Endpunkt Rate-Limiting für OJP-APIs (→ siehe Abschnitt 8). |

---

## 4. Daten-Pipeline (ETL & Optimierung)

Um den RAM-Verbrauch gering zu halten (< 32GB) und rechtlich sauber zu bleiben:

### 4.1 GTFS Pre-Processing ("The Legal Filter")
* **Input:** Offizielle Feeds von Mobility Database / TransitFeeds.
* **Filter:**
    * `KEEP`: `route_type = 2` (Rail), `1` (Metro), `100-109` (Regio).
    * `DROP`: Bus, Tram (spart ~60% RAM).
* **Tooling:** `gtfs-filter` oder Python-Skripte (Pandas).

### 4.2 OSM Pre-Processing ("Island Strategy")
* **Problem:** Ganz Europa routingfähig in OSM sprengt den Speicher.
* **Lösung:**
    1.  Extrahiere Geo-Koordinaten aller Bahnhöfe aus GTFS.
    2.  Erstelle 2km Bounding-Box pro Bahnhof.
    3.  Nutze `osmium-tool`, um nur diese "Inseln" aus `europe-latest.osm.pbf` zu schneiden.
* **Lizenz:** ODbL (Attribution erforderlich: "© OpenStreetMap contributors").

### 4.3 GTFS-Diff & Stations-Pflege

> [!NOTE]
> Bahnhöfe ändern sich selten. Das Mapping wird einmalig manuell erstellt und danach nur noch inkrementell gepflegt.

**Initialer Import (einmalig):**
1. Alle `stops.txt` aus DACH-GTFS-Feeds einlesen.
2. Semi-automatisches Matching via Name + Koordinaten-Nähe → ergibt ~90% korrekte Zuordnungen.
3. Restliche ~10% manuell kuratieren und in `station_map.json` eintragen.

**Laufende Pflege (bei jedem GTFS-Update):**
```
gtfs-diff.sh (automatisiert):
1. Lade neue Version von stops.txt
2. Diff gegen vorherige Version:
   - NEUE stop_ids     → Status: "NEW — needs mapping"     → Log + Notification
   - GELÖSCHTE stop_ids → Status: "REMOVED — check mapping" → Log + Notification
   - GEÄNDERTE Namen/Koordinaten → Status: "MODIFIED — verify" → Log + Notification
3. Nur geflaggte Einträge erfordern manuelle Kuratierung
4. Output: Bericht mit Änderungen (erwartungsgemäß 0–5 Einträge pro Monat)
```

**Erwarteter Aufwand:** ~5 Minuten pro monatlichem Update-Zyklus.

---

## 5. OJP-Abdeckung & Fallback-Strategie

> [!WARNING]
> Nicht alle EU-Länder bieten funktionsfähige OJP-Endpunkte. Die Abdeckung ist heterogen.

### 5.1 Abdeckungs-Matrix (Stand: 2026)

| Land | NAP | OJP-Status | Fallback |
| :--- | :--- | :--- | :--- |
| 🇩🇪 Deutschland | Mobilithek | ✅ Produktiv | — |
| 🇨🇭 Schweiz | opentransportdata.swiss | ✅ Produktiv | — |
| 🇦🇹 Österreich | mobilitaetsdaten.gv.at | ✅ Produktiv | — |
| 🇫🇷 Frankreich | transport.data.gouv.fr | ⚠️ GTFS only | GTFS in MOTIS |
| 🇮🇹 Italien | dati.mit.gov.it | ⚠️ GTFS only | GTFS in MOTIS |
| 🇳🇱 Niederlande | ndovloket.nl | ⚠️ BISON/GTFS | GTFS in MOTIS |
| 🇧🇪 Belgien | transportdata.be | ⚠️ GTFS only | GTFS in MOTIS |
| 🇨🇿 Tschechien | data.pid.cz | ⚠️ Teilweise | GTFS in MOTIS |
| 🇵🇱 Polen | — | 🔴 Nicht verfügbar | GTFS (PKP Intercity) |
| 🇪🇸 Spanien | nap.mitma.es | ⚠️ Teilweise | GTFS in MOTIS |

### 5.2 Fallback-Hierarchie
1. **OJP Live-Abfrage** (bevorzugt, inkl. Echtzeit).
2. **GTFS-RT Feed** direkt in MOTIS laden (statisch + Echtzeit-Overlay).
3. **Statischer GTFS-Feed** in MOTIS (nur Fahrplan, keine Echtzeit).
4. **Markierung als "eingeschränkte Abdeckung"** im Frontend.

---

## 6. Stitching Engine (Segment-Kombination)

> [!IMPORTANT]
> Das Zusammenfügen von Feeder- und Backbone-Segmenten ist die technisch komplexeste Komponente.

### 6.1 Herausforderungen
* **Minimale Umsteigezeiten** variieren pro Bahnhof (2 Min. Kleinstadtbahnhof vs. 15 Min. Frankfurt Hbf).
* **Puffer-Logik:** Verbindungen mit < 120% der minimalen Umsteigezeit als "riskant" markieren.
* **Zeitzonenwechsel** bei grenzüberschreitenden Verbindungen korrekt handhaben.

### 6.2 Algorithmus
```
1. Empfange: Feeder-Segmente (OJP) + Backbone-Segmente (MOTIS)
2. Für jeden Hub-Übergang:
   a. Lade `min_transfer_time` aus SRS für den Hub-Bahnhof
   b. Berechne tatsächliche Umsteigezeit:
      gap = backbone.departure - feeder.arrival
   c. Wenn gap < min_transfer_time → Verbindung verwerfen
   d. Wenn gap < min_transfer_time * 1.2 → Flag: "tight_connection"
   e. Wenn gap > 120 min → Flag: "long_wait" (Alternative suchen)
3. Ranking der kombinierten Routen:
   - Gesamtreisezeit
   - Anzahl Umstiege
   - Connection-Reliability-Score (basierend auf tight_connection flags)
4. Output: Top-N Verbindungen mit Metadaten
```

### 6.3 Echtzeit-Handling
* **Pre-Trip:** Stitching nutzt Soll-Fahrplan.
* **Near-Realtime:** GTFS-RT Delays werden eingerechnet → Umsteigezeit neu bewertet.
* **Broken Connection Alert:** Wenn Delay eines Feeder-Zugs die Umsteigezeit am Hub überschreitet → Alternative automatisch berechnen und dem User anzeigen.

### 6.4 Betreiber-API Enrichment (Optionaler Anreicherungs-Layer)

> [!NOTE]
> Die OJP/GTFS-Schicht liefert das Routing. Direkte Betreiber-APIs liefern **Zusatzinformationen**, die das Nutzererlebnis deutlich verbessern — ohne das Kern-Routing zu beeinflussen.

**Abgrenzung:** Preisdaten werden **bewusst ausgeschlossen**. Die Preis-APIs der Betreiber sind entweder nicht öffentlich oder lizenzrechtlich für Drittanbieter gesperrt. Das Projekt beschränkt sich auf **Deep-Linking** zu Betreiber-Buchungsseiten (→ §7.1).

#### 6.4.1 Verfügbare Betreiber-APIs

| Betreiber | API-Portal | Nutzbare Endpunkte | Registrierung | Lizenz |
| :--- | :--- | :--- | :--- | :--- |
| 🇩🇪 **DB** | developer.deutschebahn.com | Wagenreihung, Auslastung, Bahnhofsinfos, Aufzüge/Rolltreppen (FaSta) | ✅ Kostenlos | Open Data (CC-BY 4.0) |
| 🇦🇹 **ÖBB** | data.oebb.at / mobilitaetsdaten.gv.at | Echtzeit-Abfahrten, Störungsmeldungen, Haltestellendaten | ✅ Kostenlos | Open Data |
| 🇨🇭 **SBB** | opentransportdata.swiss | Echtzeit-Abfahrten, Auslastungsprognosen, Störungen, Haltestelleninfos | ✅ Kostenlos (Token) | Nutzungsbedingungen |

#### 6.4.2 Enrichment-Daten im Detail

| Datentyp | Quelle | Nutzen für User | Priorität |
| :--- | :--- | :--- | :--- |
| **Wagenreihung** | DB Wagenreihungs-API | Zeigt Position des Wagens am Gleis → schnelleres Umsteigen | Medium |
| **Auslastungsprognose** | DB, SBB | „1. Klasse leer, 2. Klasse voll" → bessere Reiseplanung | Hoch |
| **Aufzüge & Rolltreppen** | DB FaSta-API | Barrierefreiheit: defekte Aufzüge am Umsteigebahnhof anzeigen | Hoch |
| **Störungsmeldungen** | ÖBB, SBB, DB | „Strecke X gesperrt, Schienenersatzverkehr" → proaktive Warnung | Hoch |
| **Bahnhofsinfos** | DB StaDa-API | Ausstattung, Öffnungszeiten, Gleisanzahl → bei unbekannten Hubs | Niedrig |

#### 6.4.3 Integration in die Architektur

```
User-Anfrage
    ↓
Orchestrator
    ├── OJP + MOTIS → Routing (Kern)
    └── Betreiber-APIs → Enrichment (Optional)
            ├── Wagenreihung (on-demand, nur bei Detailansicht)
            ├── Auslastung (gecacht, TTL: 30min)
            ├── Störungen (gecacht, TTL: 5min)
            └── Barrierefreiheit (gecacht, TTL: 60min)
```

**Prinzip: Graceful Degradation** — Wenn eine Betreiber-API nicht erreichbar ist oder das Rate-Limit überschritten wird, werden die Enrichment-Daten einfach weggelassen. Das Routing bleibt davon vollständig unberührt.

#### 6.4.4 Rollout der Enrichment-Features

* **Phase 1 (DACH MVP):** Störungsmeldungen + Auslastung (größter Nutzen, geringster Aufwand).
* **Phase 2:** Wagenreihung (DB) + Barrierefreiheit (FaSta).
* **Phase 3+:** Weitere Betreiber-APIs je nach Verfügbarkeit und Nachfrage.

---

## 7. Rechtliche Compliance

### 7.1 Rechtsgrundlage
* **EU Delegierte VO 2017/1926:** Verpflichtet Mitgliedstaaten zur Bereitstellung von Fahrplandaten über NAPs.
* **PSI-Richtlinie (2019/1024):** Regelt die Weiterverwendung offener Daten des öffentlichen Sektors.
* **Kein Ticket-Reselling:** Ausschließlich Deep-Linking zu Betreiber-Websites → keine Vermittlerlizenz nötig.

### 7.2 Lizenz-Attribution (Frontend-Pflicht)

> [!CAUTION]
> ODbL und CC-BY verlangen sichtbare Quellenangaben. Fehlende Attribution = Lizenzverstoß.

Im Frontend muss ein persistenter **Attribution-Footer** vorhanden sein:
```
Kartendaten: © OpenStreetMap contributors (ODbL)
Fahrplandaten: [Dynamisch pro angezeigter Verbindung]
 - DE: © DB / Mobilithek (CC-BY 4.0)
 - CH: © SBB / opentransportdata.swiss (Nutzungsbedingungen)
 - AT: © ÖBB / mobilitaetsdaten.gv.at
Routing: MOTIS (MIT License)
```
* Attribution muss **dynamisch** je nach verwendeten Datenquellen der aktuellen Verbindung generiert werden.
* Jeder GTFS-Feed hat eine eigene Lizenz → im ETL-Prozess mitführen und in der `station_master`-Tabelle verlinken.

### 7.3 API-Nutzungsbedingungen & Rate Limits

| Endpunkt | Registrierung | Rate Limit | Besonderheiten |
| :--- | :--- | :--- | :--- |
| Mobilithek (DE) | ✅ Erforderlich | ~100 req/min | API-Key via Portal |
| opentransportdata.swiss (CH) | ✅ Erforderlich | ~60 req/min | Token-basiert |
| mobilitaetsdaten.gv.at (AT) | ⚠️ Teilweise | Variabel | Je nach Datensatz |

**Implementierung:**
* Pro-Endpunkt Rate-Limiter in Redis (Token-Bucket-Algorithmus).
* Request-Queue (Bull) für Burst-Handling bei vielen parallelen User-Anfragen.
* Automatischer Fallback auf gecachte Daten bei Rate-Limit-Überschreitung.

### 7.4 DSGVO-Konformität

> [!IMPORTANT]
> Sobald User-Daten verarbeitet werden, greifen die Pflichten der DSGVO (EU 2016/679).

| Aspekt | Maßnahme |
| :--- | :--- |
| **Suchanfragen** | Kein Logging von IP + Suchanfrage kombiniert. Anonymisierung nach Verarbeitung. |
| **Standortdaten** | Geolocation nur client-seitig (Browser API). Keine Speicherung auf dem Server. |
| **Cookies/Tracking** | Keine Third-Party-Cookies. Nur technisch notwendige Session-Cookies. |
| **Rechtsgrundlage** | Art. 6(1)(f) DSGVO — berechtigtes Interesse (Routenberechnung). |
| **Datenschutzerklärung** | Pflicht-Seite mit Angaben zu Verantwortlichem, Verarbeitungszwecken, Rechten der Betroffenen. |
| **Aufbewahrung** | Aggregierte Nutzungsstatistiken (ohne Personenbezug) maximal 30 Tage. |

---

## 8. Configuration Example (`docker-compose.yml`)

```yaml
version: "3.9"
services:
  orchestrator:
    image: my-rail-orchestrator:legal-v1
    environment:
      - MOTIS_HOST=motis
      - REDIS_URL=redis://redis:6379
      - DB_URL=postgres://user:pass@db:5432/stations
      - OJP_ENDPOINT_DE=https://api.mobilithek.info/ojp
      - OJP_ENDPOINT_CH=https://api.opentransportdata.swiss/ojp
      - OJP_ENDPOINT_AT=https://api.mobilitaetsdaten.gv.at/ojp
    ports:
      - "3000:3000"
    depends_on:
      - motis
      - redis

  motis:
    image: motis-project/motis:latest
    container_name: motis_backbone
    restart: unless-stopped
    shm_size: '16gb' 
    ports:
      - "8080:8080"
    volumes:
      - ./data/gtfs_filtered:/input/gtfs:ro
      - ./data/osm/island_extract.pbf:/input/osm/europe.osm.pbf:ro
      - ./config/config.ini:/motis/config.ini
    command:
      - --config=/motis/config.ini

  redis:
    image: redis:alpine
    command: redis-server --save 60 1 --loglevel warning

  db:
    image: postgis/postgis:16-3.4-alpine
    environment:
      - POSTGRES_DB=stations
    volumes:
      - pg_data:/var/lib/postgresql/data

volumes:
  pg_data:
```

---

## 9. Rollout-Phasen

| Phase | Scope | Ziel | Geschätzter Aufwand |
| :--- | :--- | :--- | :--- |
| **Phase 1: DACH MVP** | DE + CH + AT | Funktionierender Prototyp mit OJP + MOTIS. Proof-of-Concept: „Dorf in DE → Stadt in AT". | 8–12 Wochen |
| **Phase 2: Westeuropa** | + FR, IT, NL, BE | GTFS-Fallback für Länder ohne OJP. Stitching-Optimierung. | 6–8 Wochen |
| **Phase 3: Gesamteuropa** | + ES, PL, CZ, SE, etc. | Volle Abdeckung mit Fallback-Hierarchie. Community-Beiträge für ID-Matching. | Ongoing |
| **Phase 4: Extras** | Seat-Linking, Preis-Vergleich | Deep-Links zu Buchungsseiten mit Pre-filled Parametern. | TBD |

### Phase 1 — Meilensteine (Detail)
1. ✅ MOTIS mit DACH GTFS-Feeds aufsetzen (Docker).
2. ✅ Station Resolution Service: UIC ↔ GTFS-ID Mapping für DACH.
3. ✅ OJP-Integration: Mobilithek (DE) + opentransportdata.swiss (CH).
4. ✅ Stitching Engine v1: Einfache Segment-Kombination mit Transfer-Zeit-Validierung.
5. ✅ Frontend: Suchmaske + Kartenansicht + Ergebnisliste mit Attribution.
6. ✅ DSGVO: Datenschutzerklärung + Cookie-Banner (nur technisch notwendig).