import { StitchingService } from './stitching.service';
import { StationsService } from '../stations/stations.service';
import { Segment } from '../common/types';

describe('StitchingService', () => {
    let service: StitchingService;
    let stationsService: Partial<StationsService>;

    beforeEach(() => {
        stationsService = {
            getMinTransferTime: jest.fn().mockReturnValue(10),
        };
        service = new StitchingService(stationsService as StationsService);
    });

    const makeSegment = (
        type: 'feeder' | 'backbone',
        depOffset: number,
        durationMin: number,
    ): Segment => {
        const dep = new Date('2026-03-01T08:00:00Z');
        dep.setMinutes(dep.getMinutes() + depOffset);
        const arr = new Date(dep);
        arr.setMinutes(arr.getMinutes() + durationMin);
        return {
            type,
            source: type === 'backbone' ? 'motis' : 'ojp',
            origin: { name: 'Origin', uic: '8000001' },
            destination: { name: 'Destination', uic: '8000002' },
            departure: dep.toISOString(),
            arrival: arr.toISOString(),
            duration_minutes: durationMin,
        };
    };

    it('should create a route from backbone-only segments', () => {
        const backbone = makeSegment('backbone', 0, 240);

        const routes = service.stitchRoutes({
            feederSegments: [],
            backboneSegments: [backbone],
            distributorSegments: [],
        });

        expect(routes).toHaveLength(1);
        expect(routes[0].segments).toHaveLength(1);
        expect(routes[0].total_transfers).toBe(0);
    });

    it('should combine feeder + backbone with valid transfer', () => {
        const feeder = makeSegment('feeder', -60, 40); // Arrives at -20 min
        const backbone = makeSegment('backbone', 0, 240); // Departs at 0 → 20 min gap

        const routes = service.stitchRoutes({
            feederSegments: [feeder],
            backboneSegments: [backbone],
            distributorSegments: [],
        });

        expect(routes).toHaveLength(1);
        expect(routes[0].segments).toHaveLength(2);
        expect(routes[0].total_transfers).toBe(1);
    });

    it('should reject connections with too little transfer time', () => {
        const feeder = makeSegment('feeder', -12, 10); // Arrives at -2 min
        const backbone = makeSegment('backbone', 0, 240); // Departs at 0 → 2 min gap

        const routes = service.stitchRoutes({
            feederSegments: [feeder],
            backboneSegments: [backbone],
            distributorSegments: [],
        });

        // Should be empty (rejected) because gap (2 min) < min_transfer_time (10 min)
        expect(routes).toHaveLength(0);
    });

    it('should flag tight connections', () => {
        const feeder = makeSegment('feeder', -21, 10); // Arrives at -11 min
        const backbone = makeSegment('backbone', 0, 240); // Departs at 0 → 11 min gap

        const routes = service.stitchRoutes({
            feederSegments: [feeder],
            backboneSegments: [backbone],
            distributorSegments: [],
        });

        expect(routes).toHaveLength(1);
        const tightFlags = routes[0].flags.filter((f) => f.type === 'tight_connection');
        expect(tightFlags).toHaveLength(1);
    });

    it('should flag long waits', () => {
        const feeder = makeSegment('feeder', -180, 30); // Arrives at -150 min
        const backbone = makeSegment('backbone', 0, 240); // Departs at 0 → 150 min gap

        const routes = service.stitchRoutes({
            feederSegments: [feeder],
            backboneSegments: [backbone],
            distributorSegments: [],
        });

        expect(routes).toHaveLength(1);
        const longWaitFlags = routes[0].flags.filter((f) => f.type === 'long_wait');
        expect(longWaitFlags).toHaveLength(1);
    });

    it('should rank routes by transfers then duration', () => {
        const backbone1 = makeSegment('backbone', 0, 240);
        const backbone2 = makeSegment('backbone', 0, 180);

        const routes = service.stitchRoutes({
            feederSegments: [],
            backboneSegments: [backbone1, backbone2],
            distributorSegments: [],
        });

        expect(routes).toHaveLength(2);
        // Both have 0 transfers, so shorter one should be first
        expect(routes[0].total_duration_minutes).toBeLessThanOrEqual(
            routes[1].total_duration_minutes,
        );
    });
});
