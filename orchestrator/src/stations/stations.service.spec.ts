import { StationsService } from './stations.service';

describe('StationsService', () => {
    let service: StationsService;

    beforeEach(() => {
        // Set env to find station_map.json in the project
        process.env.STATION_MAP_PATH = require('path').resolve(
            __dirname,
            '../../../data/station_map.json',
        );
        service = new StationsService();
        service.onModuleInit();
    });

    it('should load stations from station_map.json', () => {
        const stations = service.getAll();
        expect(stations.length).toBeGreaterThan(0);
    });

    it('should find station by UIC code', () => {
        const station = service.findByUic('8000261'); // München Hbf
        expect(station).toBeDefined();
        expect(station?.name).toBe('München Hbf');
    });

    it('should find station by name (case-insensitive)', () => {
        const station = service.findByName('münchen hbf');
        expect(station).toBeDefined();
        expect(station?.uic).toBe('8000261');
    });

    it('should find station by partial name', () => {
        const station = service.findByName('München');
        expect(station).toBeDefined();
    });

    it('should return all hubs', () => {
        const hubs = service.getHubs();
        expect(hubs.length).toBeGreaterThan(0);
        expect(hubs.every((h) => h.type === 'hub')).toBe(true);
    });

    it('should find nearest hub to coordinates', () => {
        // Coordinates near München
        const hub = service.findNearestHub(48.15, 11.58);
        expect(hub).toBeDefined();
        expect(hub?.name).toBe('München Hbf');
    });

    it('should get min transfer time for a station', () => {
        const time = service.getMinTransferTime('8000261'); // München Hbf
        expect(time).toBe(10);
    });

    it('should return default transfer time for unknown station', () => {
        const time = service.getMinTransferTime('9999999');
        expect(time).toBe(5);
    });

    it('should search stations by name', () => {
        const results = service.searchByName('Hbf');
        expect(results.length).toBeGreaterThan(5); // Many stations have Hbf
    });
});
