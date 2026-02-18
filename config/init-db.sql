-- ============================================
-- European Rail Meta-Router — Database Init
-- ============================================

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Stations table for hub identification and geo-queries
CREATE TABLE IF NOT EXISTS stations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    uic VARCHAR(20) UNIQUE,
    country_code CHAR(2) NOT NULL,
    gtfs_id_de VARCHAR(50),
    gtfs_id_at VARCHAR(50),
    gtfs_id_ch VARCHAR(50),
    ojp_ref VARCHAR(100),
    min_transfer_minutes INTEGER DEFAULT 5,
    station_type VARCHAR(20) DEFAULT 'stop', -- 'hub' or 'stop'
    location GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Spatial index for nearest-hub queries
CREATE INDEX IF NOT EXISTS idx_stations_location ON stations USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_stations_uic ON stations(uic);
CREATE INDEX IF NOT EXISTS idx_stations_type ON stations(station_type);
CREATE INDEX IF NOT EXISTS idx_stations_country ON stations(country_code);

-- Insert DACH hub stations
INSERT INTO stations (name, uic, country_code, gtfs_id_de, gtfs_id_at, gtfs_id_ch, ojp_ref, min_transfer_minutes, station_type, location)
VALUES
    ('München Hbf', '8000261', 'DE', '8000261', NULL, NULL, 'de:09162:6', 10, 'hub', ST_SetSRID(ST_MakePoint(11.5600, 48.1402), 4326)),
    ('Berlin Hbf', '8011160', 'DE', '8011160', NULL, NULL, 'de:11000:900003201', 10, 'hub', ST_SetSRID(ST_MakePoint(13.3694, 52.5251), 4326)),
    ('Frankfurt (Main) Hbf', '8000105', 'DE', '8000105', NULL, NULL, 'de:06412:1', 12, 'hub', ST_SetSRID(ST_MakePoint(8.6632, 50.1071), 4326)),
    ('Hamburg Hbf', '8002549', 'DE', '8002549', NULL, NULL, 'de:02000:10902', 10, 'hub', ST_SetSRID(ST_MakePoint(10.0069, 53.5530), 4326)),
    ('Köln Hbf', '8000207', 'DE', '8000207', NULL, NULL, 'de:05315:11001', 8, 'hub', ST_SetSRID(ST_MakePoint(6.9590, 50.9430), 4326)),
    ('Stuttgart Hbf', '8000096', 'DE', '8000096', NULL, NULL, 'de:08111:6115', 10, 'hub', ST_SetSRID(ST_MakePoint(9.1829, 48.7843), 4326)),
    ('Nürnberg Hbf', '8000284', 'DE', '8000284', NULL, NULL, 'de:09564:3', 8, 'hub', ST_SetSRID(ST_MakePoint(11.0829, 49.4466), 4326)),
    ('Dresden Hbf', '8010085', 'DE', '8010085', NULL, NULL, 'de:14612:7', 8, 'hub', ST_SetSRID(ST_MakePoint(13.7329, 51.0401), 4326)),
    ('Leipzig Hbf', '8010205', 'DE', '8010205', NULL, NULL, 'de:14713:10', 10, 'hub', ST_SetSRID(ST_MakePoint(12.3822, 51.3455), 4326)),
    ('Hannover Hbf', '8000152', 'DE', '8000152', NULL, NULL, 'de:03241:2510', 8, 'hub', ST_SetSRID(ST_MakePoint(9.7411, 52.3768), 4326)),
    ('Düsseldorf Hbf', '8000085', 'DE', '8000085', NULL, NULL, 'de:05111:18235', 8, 'hub', ST_SetSRID(ST_MakePoint(6.7942, 51.2200), 4326)),
    ('Wien Hbf', '8100003', 'AT', NULL, '8100003', NULL, 'at:49:711', 12, 'hub', ST_SetSRID(ST_MakePoint(16.3757, 48.1854), 4326)),
    ('Salzburg Hbf', '8100085', 'AT', NULL, '8100085', NULL, 'at:45:502', 8, 'hub', ST_SetSRID(ST_MakePoint(13.0458, 47.8129), 4326)),
    ('Innsbruck Hbf', '8100108', 'AT', NULL, '8100108', NULL, 'at:70:711', 8, 'hub', ST_SetSRID(ST_MakePoint(11.4009, 47.2632), 4326)),
    ('Linz Hbf', '8100013', 'AT', NULL, '8100013', NULL, 'at:40:420', 8, 'hub', ST_SetSRID(ST_MakePoint(14.2917, 48.2902), 4326)),
    ('Graz Hbf', '8100173', 'AT', NULL, '8100173', NULL, 'at:60:893', 8, 'hub', ST_SetSRID(ST_MakePoint(15.4173, 47.0728), 4326)),
    ('Zürich HB', '8503000', 'CH', NULL, NULL, '8503000', 'ch:1:sloid:3000', 10, 'hub', ST_SetSRID(ST_MakePoint(8.5402, 47.3783), 4326)),
    ('Bern', '8507000', 'CH', NULL, NULL, '8507000', 'ch:1:sloid:7000', 8, 'hub', ST_SetSRID(ST_MakePoint(7.4396, 46.9489), 4326)),
    ('Basel SBB', '8500010', 'CH', NULL, NULL, '8500010', 'ch:1:sloid:10', 10, 'hub', ST_SetSRID(ST_MakePoint(7.5896, 47.5476), 4326)),
    ('Bregenz', '8100110', 'AT', NULL, '8100110', NULL, 'at:80:212', 5, 'hub', ST_SetSRID(ST_MakePoint(9.7480, 47.5030), 4326))
ON CONFLICT (uic) DO NOTHING;
