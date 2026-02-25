const { buildSchema } = require("graphql");

const schema = buildSchema(`
  type Query {
    health: String
    clusters(country: String): [Cluster]
    cluster(id: ID!): ClusterDetail
    lowConfidenceQueue(limit: Int, offset: Int): LowConfidenceQueueResult!
  }

  type Mutation {
    requestAiScore(clusterId: ID!): AiScoreResult
    approveAiMatch(clusterId: ID!, evidenceId: ID!): AiMatchDecisionResult!
    rejectAiMatch(clusterId: ID!, evidenceId: ID!): AiMatchDecisionResult!
    overrideAiMatch(clusterId: ID!, evidenceId: ID!, targetClusterId: ID!): AiMatchDecisionResult!
  }

  type Cluster {
    cluster_id: ID!
    country: String
    status: String
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
    name: String
    lat: Float
    lon: Float
    service_context: ServiceContext
  }

  type ServiceContext {
    lines: [String]
    incoming: [String]
    outgoing: [String]
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
  }

  type AiMatchDecisionResult {
    ok: Boolean!
    decision_id: ID!
    cluster_id: String!
    operation: String!
  }
`);

module.exports = { schema };
