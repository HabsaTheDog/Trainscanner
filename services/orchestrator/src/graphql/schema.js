const { buildSchema } = require("graphql");

const schema = buildSchema(`
  type Query {
    health: String
    globalClusters(country: String, status: String): GlobalMergeClusterConnection!
    globalCluster(id: ID!): GlobalMergeClusterDetail
  }

  type Mutation {
    requestAiScore(clusterId: ID!): AiScoreResult
    submitGlobalMergeDecision(clusterId: ID!, input: GlobalMergeDecisionInput!): GlobalMergeDecisionResult!
  }

  input GlobalMergeDecisionInput {
    operation: String!
    selected_global_station_ids: [String!]
    groups: [GlobalDecisionGroupInput!]
    note: String
    requested_by: String
    rename_targets: [GlobalRenameTargetInput!]
  }

  input GlobalDecisionGroupInput {
    group_label: String
    member_global_station_ids: [String!]
    rename_to: String
  }

  input GlobalRenameTargetInput {
    global_station_id: String!
    rename_to: String!
  }

  type GlobalMergeDecisionResult {
    ok: Boolean!
    cluster_id: ID!
    decision_id: ID
    operation: String!
  }

  type GlobalMergeCluster {
    cluster_id: ID!
    status: String
    severity: String
    scope_tag: String
    display_name: String
    candidate_count: Int
    issue_count: Int
    country_tags: [String!]
    candidates: [GlobalClusterCandidate!]
  }

  type GlobalMergeClusterConnection {
    items: [GlobalMergeCluster!]!
    total_count: Int!
    limit: Int!
  }

  type GlobalMergeClusterDetail {
    cluster_id: ID!
    status: String
    severity: String
    scope_tag: String
    display_name: String
    summary: JSON
    candidate_count: Int
    issue_count: Int
    country_tags: [String!]
    candidates: [GlobalClusterCandidate!]
    evidence: [GlobalEvidence!]
    decisions: [GlobalDecision!]
    edit_history: [GlobalEditHistory!]
  }

  type GlobalClusterCandidate {
    global_station_id: String
    display_name: String
    candidate_rank: Int
    lat: Float
    lon: Float
    country: String
    provider_labels: [String!]
  }

  type GlobalEvidence {
    evidence_type: String
    source_global_station_id: String
    target_global_station_id: String
    score: Float
    details: JSON
  }

  type GlobalDecision {
    decision_id: ID
    operation: String
    note: String
    requested_by: String
    created_at: String
    members: [GlobalDecisionMember!]
  }

  type GlobalDecisionMember {
    global_station_id: String
    action: String
    group_label: String
    metadata: JSON
  }

  type GlobalEditHistory {
    event_type: String
    requested_by: String
    created_at: String
  }

  type AiScoreResult {
    cluster_id: ID!
    confidence_score: Float
    suggested_action: String
    reasoning: String
  }

  scalar JSON
`);

module.exports = { schema };
