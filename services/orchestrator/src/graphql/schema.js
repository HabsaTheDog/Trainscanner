const { buildSchema } = require("graphql");

const schema = buildSchema(`
  type Query {
    health: String
    clusters(country: String, status: String): [Cluster]
    cluster(id: ID!): ClusterDetail
    lowConfidenceQueue(limit: Int, offset: Int): LowConfidenceQueueResult!
  }

  type Mutation {
    requestAiScore(clusterId: ID!): AiScoreResult
    approveAiMatch(clusterId: ID!, evidenceId: ID!): AiMatchDecisionResult!
    rejectAiMatch(clusterId: ID!, evidenceId: ID!): AiMatchDecisionResult!
    overrideAiMatch(clusterId: ID!, evidenceId: ID!, targetClusterId: ID!): AiMatchDecisionResult!
    setMegaHubWalkTime(hubId: ID!, walkMinutes: Int!): WalkTimeOverrideResult!
    submitClusterDecision(clusterId: ID!, input: ClusterDecisionInput!): ClusterDecisionResult!
  }

  input ClusterDecisionInput {
    operation: String!
    selected_station_ids: [String]
    groups: [DecisionGroupInput]
    note: String
    requested_by: String
    rename_to: String
    rename_targets: [RenameTargetInput]
  }

  input DecisionGroupInput {
    group_label: String
    target_canonical_station_id: String
    member_station_ids: [String]
    rename_to: String
    section_type: String
    section_name: String
    segment_action: SegmentActionInput
  }

  input SegmentActionInput {
    walk_links: [WalkLinkInput]
  }

  input WalkLinkInput {
    from_segment_id: String!
    to_segment_id: String!
    min_walk_minutes: Int
    bidirectional: Boolean
  }

  input RenameTargetInput {
    canonical_station_id: String!
    rename_to: String!
  }

  type ClusterDecisionResult {
    ok: Boolean!
    cluster_id: ID!
    decision_id: ID
    operation: String!
  }

  type Cluster {
    cluster_id: ID!
    country: String
    status: String
    display_name: String
    severity: String
    candidate_count: Int
    issue_count: Int
    scope_tag: String
    member_nodes: [ClusterNode]
    member_count: Int
  }

  type ClusterNode {
    canonical_station_id: String
    name: String
    lat: Float
    lon: Float
  }

  type ClusterDetail {
    cluster_id: ID!
    country: String
    status: String
    scope_tag: String
    severity: String
    display_name: String
    candidates: [ClusterCandidate]
    evidence: [Evidence]
    decisions: [Decision]
    edit_history: [EditHistory]
  }

  type Evidence {
    evidence_type: String
    source_canonical_station_id: String
    target_canonical_station_id: String
    score: Float
  }

  type Decision {
    operation: String
    requested_by: String
    created_at: String
  }

  type EditHistory {
    event_type: String
    requested_by: String
    created_at: String
  }

  type ClusterCandidate {
    canonical_station_id: String
    display_name: String
    candidate_rank: Int
    aliases: [String]
    provider_labels: [String]
    lat: Float
    lon: Float
    service_context: ServiceContext
    segment_context: SegmentContext
  }

  type ServiceContext {
    lines: [String]
    incoming: [String]
    outgoing: [String]
  }

  type SegmentContext {
    segment_id: String
    segment_name: String
    segment_type: String
  }
  
  type AiScoreResult {
     cluster_id: ID!
     confidence_score: Float
     suggested_action: String
     reasoning: String
  }

  type LowConfidenceQueueResult {
    total: Int!
    items: [LowConfidenceItem!]!
  }

  type LowConfidenceItem {
    evidence_id: ID!
    cluster_id: String!
    source_canonical_station_id: String!
    target_canonical_station_id: String!
    evidence_type: String!
    ai_confidence: Float
    ai_suggested_action: String
    cluster_display_name: String
    source_lat: Float
    source_lon: Float
    target_lat: Float
    target_lon: Float
  }

  type WalkTimeOverrideResult {
    ok: Boolean!
    rule_id: ID!
    hub_id: String!
    walk_minutes: Int!
  }

  type AiMatchDecisionResult {
    ok: Boolean!
    decision_id: ID!
    cluster_id: String!
    operation: String!
  }
`);

module.exports = { schema };
