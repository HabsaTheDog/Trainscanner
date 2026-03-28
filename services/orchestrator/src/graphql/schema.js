const { buildSchema } = require("graphql");

const schema = buildSchema(`
  type Query {
    health: String
    globalClusters(country: String, status: String): GlobalMergeClusterConnection!
    globalCluster(id: ID!): GlobalMergeClusterDetail
    globalReferenceViewport(
      minLat: Float!
      minLon: Float!
      maxLat: Float!
      maxLon: Float!
      sourceIds: [String!]
      limit: Int
    ): [GlobalReferencePoint!]!
  }

  type Mutation {
    requestAiScore(clusterId: ID!): AiScoreResult
    submitGlobalMergeDecision(clusterId: ID!, input: GlobalMergeDecisionInput!): GlobalMergeDecisionResult!
    saveGlobalClusterWorkspace(clusterId: ID!, input: GlobalClusterWorkspaceInput!): GlobalClusterWorkspaceResult!
    undoGlobalClusterWorkspace(clusterId: ID!, input: GlobalClusterWorkspaceActorInput): GlobalClusterWorkspaceResult!
    resetGlobalClusterWorkspace(clusterId: ID!, input: GlobalClusterWorkspaceActorInput): GlobalClusterWorkspaceResult!
    reopenGlobalCluster(clusterId: ID!, input: GlobalClusterWorkspaceActorInput): GlobalClusterWorkspaceResult!
    resolveGlobalCluster(clusterId: ID!, input: ResolveGlobalClusterInput!): GlobalClusterResolveResult!
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

  input GlobalClusterWorkspaceInput {
    workspace: JSON!
    updated_by: String
  }

  input GlobalClusterWorkspaceActorInput {
    updated_by: String
  }

  input ResolveGlobalClusterInput {
    status: String!
    note: String
    requested_by: String
    clear_workspace_on_dismiss: Boolean
  }

  type GlobalMergeDecisionResult {
    ok: Boolean!
    cluster_id: ID!
    decision_id: ID
    operation: String!
  }

  type GlobalClusterWorkspaceResult {
    ok: Boolean!
    cluster_id: ID!
    workspace_version: Int!
    effective_status: String
    workspace: JSON
  }

  type GlobalClusterResolveResult {
    ok: Boolean!
    cluster_id: ID!
    decision_id: ID
    status: String!
    next_cluster_id: ID
  }

  type GlobalMergeCluster {
    cluster_id: ID!
    status: String
    effective_status: String
    has_workspace: Boolean
    workspace_version: Int
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
    effective_status: String
    workspace_version: Int
    has_workspace: Boolean
    workspace: JSON
    severity: String
    scope_tag: String
    display_name: String
    summary: JSON
    candidate_count: Int
    issue_count: Int
    country_tags: [String!]
    candidates: [GlobalClusterCandidate!]
    reference_overlay: [GlobalReferencePoint!]
    evidence: [GlobalEvidence!]
    evidence_summary: JSON
    pair_summaries: [GlobalPairSummary!]
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
    aliases: [String!]
    coord_status: String
    network_context: GlobalCandidateNetworkContext
    network_summary: GlobalCandidateNetworkSummary
    provenance: GlobalCandidateProvenance
    external_reference_summary: GlobalCandidateExternalReferenceSummary
    external_reference_matches: [GlobalCandidateExternalReferenceMatch!]
  }

  type GlobalCandidateExternalReferenceSummary {
    source_counts: JSON
    primary_match_count: Int
    strong_match_count: Int
    probable_match_count: Int
  }

  type GlobalCandidateExternalReferenceMatch {
    source_id: String
    external_id: String
    display_name: String
    category: String
    lat: Float
    lon: Float
    distance_meters: Float
    match_status: String
    match_confidence: Float
    source_url: String
    is_primary: Boolean
  }

  type GlobalReferencePoint {
    source_id: String
    external_id: String
    display_name: String
    category: String
    lat: Float
    lon: Float
    source_url: String
    matched_candidate_ids: [String!]
    match_count: Int
  }

  type GlobalCandidateProvenance {
    has_active_source_mappings: Boolean
    active_source_ids: [String!]
    active_source_labels: [String!]
    active_stop_place_refs: [String!]
    historical_source_ids: [String!]
    historical_source_labels: [String!]
    historical_stop_place_refs: [String!]
    coord_input_stop_place_refs: [String!]
  }

  type GlobalCandidateNetworkRoute {
    label: String
    transport_mode: String
    pattern_hits: Int
  }

  type GlobalCandidateNetworkNeighbor {
    station_name: String
    pattern_hits: Int
  }

  type GlobalCandidateNetworkContext {
    routes: [GlobalCandidateNetworkRoute!]
    incoming: [GlobalCandidateNetworkNeighbor!]
    outgoing: [GlobalCandidateNetworkNeighbor!]
    stop_points: [String!]
  }

  type GlobalCandidateNetworkSummary {
    route_pattern_count: Int
    incoming_neighbor_count: Int
    outgoing_neighbor_count: Int
    stop_point_count: Int
    provider_source_count: Int
  }

  type GlobalEvidence {
    evidence_type: String
    source_global_station_id: String
    target_global_station_id: String
    category: String
    is_seed_rule: Boolean
    seed_reasons: [String!]
    status: String
    score: Float
    raw_value: Float
    details: JSON
  }

  type GlobalPairSummary {
    source_global_station_id: String
    target_global_station_id: String
    supporting_count: Int
    warning_count: Int
    missing_count: Int
    informational_count: Int
    score: Float
    summary: String
    categories: [String!]
    seed_reasons: [String!]
    highlights: JSON
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
