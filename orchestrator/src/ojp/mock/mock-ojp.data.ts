import { Segment } from '../../common/types';

interface MockSegmentTemplate extends Omit<Segment, 'departure' | 'arrival'> {
    departure: string;
    arrival: string;
    offset_minutes?: number;
}

/**
 * Pre-defined mock OJP responses for common DACH routes.
 * Keys are lowercase `origin-destination` station names.
 */
export const MOCK_OJP_RESPONSES: Record<string, MockSegmentTemplate[]> = {
    // === GERMANY ===
    'rosenheim-münchen hbf': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Rosenheim' },
            destination: { name: 'München Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 38,
            offset_minutes: 0,
            train_number: 'RE 30310',
            operator: 'DB Regio Bayern',
            route_type: 'RE',
        },
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Rosenheim' },
            destination: { name: 'München Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 42,
            offset_minutes: 30,
            train_number: 'RB 30312',
            operator: 'DB Regio Bayern',
            route_type: 'RB',
        },
    ],

    'augsburg hbf-münchen hbf': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Augsburg Hbf' },
            destination: { name: 'München Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 30,
            offset_minutes: 0,
            train_number: 'RE 57410',
            operator: 'DB Regio Bayern',
            route_type: 'RE',
        },
    ],

    'potsdam hbf-berlin hbf': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Potsdam Hbf' },
            destination: { name: 'Berlin Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 25,
            offset_minutes: 0,
            train_number: 'RE 1',
            operator: 'DB Regio Nordost',
            route_type: 'RE',
        },
    ],

    // === AUSTRIA ===
    'st. pölten hbf-wien hbf': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'St. Pölten Hbf' },
            destination: { name: 'Wien Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 27,
            offset_minutes: 0,
            train_number: 'REX 3010',
            operator: 'ÖBB',
            route_type: 'REX',
        },
    ],

    'kufstein-innsbruck hbf': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Kufstein' },
            destination: { name: 'Innsbruck Hbf' },
            departure: '',
            arrival: '',
            duration_minutes: 35,
            offset_minutes: 0,
            train_number: 'REX 5214',
            operator: 'ÖBB',
            route_type: 'REX',
        },
    ],

    // === SWITZERLAND ===
    'winterthur-zürich hb': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Winterthur' },
            destination: { name: 'Zürich HB' },
            departure: '',
            arrival: '',
            duration_minutes: 22,
            offset_minutes: 0,
            train_number: 'S 12',
            operator: 'SBB',
            route_type: 'S',
        },
    ],

    'thun-bern': [
        {
            type: 'feeder',
            source: 'ojp',
            origin: { name: 'Thun' },
            destination: { name: 'Bern' },
            departure: '',
            arrival: '',
            duration_minutes: 20,
            offset_minutes: 0,
            train_number: 'RE 3320',
            operator: 'BLS',
            route_type: 'RE',
        },
    ],
};
