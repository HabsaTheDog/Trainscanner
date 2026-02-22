const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildCuratedProjectionRowsV1
} = require('../../src/domains/qa/curated-projection');

test('buildCuratedProjectionRowsV1 creates one merge entity from selected stations', () => {
  const rows = buildCuratedProjectionRowsV1({
    clusterId: 'clu_1',
    decision: {
      operation: 'merge',
      selectedStationIds: ['cstn_a', 'cstn_b'],
      groups: [],
      renameTo: 'Merged Alpha',
      requestedBy: 'tester'
    }
  });

  assert.equal(rows.entities.length, 1);
  assert.equal(rows.members.length, 2);
  assert.equal(rows.fieldProvenance.length, 1);
  assert.equal(rows.lineage.length, 1);
  assert.equal(rows.entities[0].display_name, 'Merged Alpha');
  assert.equal(rows.members[0].member_role, 'primary');
  assert.equal(rows.members[0].canonical_station_id, 'cstn_a');
});

test('buildCuratedProjectionRowsV1 creates one split entity from selected stations without explicit groups', () => {
  const rows = buildCuratedProjectionRowsV1({
    clusterId: 'clu_2',
    decision: {
      operation: 'split',
      selectedStationIds: ['cstn_a', 'cstn_b'],
      groups: [],
      renameTo: '',
      requestedBy: 'tester'
    }
  });

  assert.equal(rows.entities.length, 1);
  assert.equal(rows.members.length, 2);
  assert.equal(rows.lineage.length, 1);
  assert.equal(rows.members[0].member_role, 'primary');
});

test('buildCuratedProjectionRowsV1 creates section-aware entities for split groups', () => {
  const rows = buildCuratedProjectionRowsV1({
    clusterId: 'clu_3',
    decision: {
      operation: 'split',
      selectedStationIds: ['cstn_a', 'cstn_b'],
      renameTo: '',
      requestedBy: 'tester',
      groups: [
        {
          groupLabel: 'Main Hall',
          memberStationIds: ['cstn_a'],
          targetCanonicalStationId: 'cstn_a',
          sectionType: 'main',
          sectionName: 'Main Hall'
        },
        {
          groupLabel: 'Bus Terminal',
          memberStationIds: ['cstn_b'],
          targetCanonicalStationId: 'cstn_b',
          sectionType: 'bus',
          sectionName: 'Bus Terminal'
        }
      ]
    }
  });

  assert.equal(rows.entities.length, 2);
  assert.equal(rows.members.length, 2);
  assert.equal(rows.fieldProvenance.length, 2);
  assert.equal(rows.lineage.length, 2);
  assert.equal(rows.entities[0].metadata.section_type, 'main');
  assert.equal(rows.entities[1].metadata.section_type, 'bus');
});
