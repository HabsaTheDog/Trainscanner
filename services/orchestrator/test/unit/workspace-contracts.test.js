const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createEmptyWorkspace,
  expandRefMembers,
  normalizeResolveRequest,
  normalizeWorkspaceMutationInput,
  normalizeWorkspacePayload,
} = require("../../src/domains/qa/workspace-contracts");

test("normalizeWorkspacePayload accepts merge and group composites", () => {
  const payload = normalizeWorkspacePayload({
    merges: [
      {
        entity_id: "merge-main",
        member_refs: ["raw:gstn_a", "raw:gstn_b"],
        display_name: "Alpha Hub",
      },
    ],
    groups: [
      {
        entity_id: "group-main",
        member_refs: ["merge:merge-main", "raw:gstn_c"],
        display_name: "Alpha Interchange",
        internal_nodes: [
          {
            node_id: "node-a",
            source_ref: "merge:merge-main",
            member_global_station_ids: ["gstn_a", "gstn_b"],
            label: "Rail",
          },
          {
            node_id: "node-b",
            source_ref: "raw:gstn_c",
            member_global_station_ids: ["gstn_c"],
            label: "Bus",
          },
        ],
        transfer_matrix: [
          {
            from_node_id: "node-a",
            to_node_id: "node-b",
            min_walk_seconds: 180,
            bidirectional: true,
          },
        ],
      },
    ],
    renames: [{ ref: "raw:gstn_c", display_name: "Alpha Bus" }],
    keep_separate_sets: [{ refs: ["raw:gstn_d", "raw:gstn_e"] }],
    note: "reviewed",
  });

  assert.equal(payload.merges.length, 1);
  assert.equal(payload.groups.length, 1);
  assert.equal(payload.groups[0].internal_nodes.length, 2);
  assert.equal(payload.entities.length >= 2, true);
});

test("normalizeWorkspacePayload rejects groups with unknown merge refs", () => {
  assert.throws(
    () =>
      normalizeWorkspacePayload({
        groups: [
          {
            entity_id: "group-main",
            member_refs: ["merge:missing", "raw:gstn_c"],
            display_name: "Broken",
            internal_nodes: [
              {
                node_id: "node-a",
                source_ref: "merge:missing",
                member_global_station_ids: ["gstn_a"],
                label: "Rail",
              },
              {
                node_id: "node-b",
                source_ref: "raw:gstn_c",
                member_global_station_ids: ["gstn_c"],
                label: "Bus",
              },
            ],
          },
        ],
      }),
    /unknown merge entity/,
  );
});

test("normalizeWorkspaceMutationInput carries updated_by and workspace payload", () => {
  const result = normalizeWorkspaceMutationInput({
    workspace: {
      merges: [
        {
          entity_id: "merge-main",
          member_refs: ["raw:gstn_a", "raw:gstn_b"],
          display_name: "Alpha Hub",
        },
      ],
    },
    updated_by: "qa_tester",
  });

  assert.equal(result.updatedBy, "qa_tester");
  assert.equal(result.workspace.merges[0].entity_id, "merge-main");
});

test("normalizeResolveRequest validates final status choices", () => {
  assert.equal(
    normalizeResolveRequest({ status: "resolved" }).status,
    "resolved",
  );
  assert.equal(
    normalizeResolveRequest({ status: "dismissed" }).status,
    "dismissed",
  );
  assert.throws(
    () => normalizeResolveRequest({ status: "open" }),
    /resolve status must be either 'resolved' or 'dismissed'/,
  );
});

test("expandRefMembers unwraps merge refs into raw station ids", () => {
  const workspace = normalizeWorkspacePayload({
    merges: [
      {
        entity_id: "merge-main",
        member_refs: ["raw:gstn_a", "raw:gstn_b"],
        display_name: "Alpha Hub",
      },
    ],
  });

  assert.deepEqual(expandRefMembers("merge:merge-main", workspace), [
    "gstn_a",
    "gstn_b",
  ]);
  assert.deepEqual(createEmptyWorkspace(), {
    entities: [],
    merges: [],
    groups: [],
    renames: [],
    keep_separate_sets: [],
    note: "",
  });
});
