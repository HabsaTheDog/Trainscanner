import PropTypes from "prop-types";

export const refSetShape = PropTypes.shape({
  has: PropTypes.func.isRequired,
  size: PropTypes.number,
});

export const filtersShape = PropTypes.shape({
  country: PropTypes.string,
  status: PropTypes.string,
});

export const candidateShape = PropTypes.shape({
  global_station_id: PropTypes.string,
  coord_status: PropTypes.string,
  provider_labels: PropTypes.arrayOf(PropTypes.string),
  provenance: PropTypes.shape({
    has_active_source_mappings: PropTypes.bool,
    active_source_ids: PropTypes.arrayOf(PropTypes.string),
    active_source_labels: PropTypes.arrayOf(PropTypes.string),
    active_stop_place_refs: PropTypes.arrayOf(PropTypes.string),
    historical_source_ids: PropTypes.arrayOf(PropTypes.string),
    historical_source_labels: PropTypes.arrayOf(PropTypes.string),
    historical_stop_place_refs: PropTypes.arrayOf(PropTypes.string),
    coord_input_stop_place_refs: PropTypes.arrayOf(PropTypes.string),
  }),
  service_context: PropTypes.shape({
    lines: PropTypes.arrayOf(PropTypes.string),
    incoming: PropTypes.arrayOf(PropTypes.string),
    outgoing: PropTypes.arrayOf(PropTypes.string),
    stop_points: PropTypes.arrayOf(PropTypes.string),
    transport_modes: PropTypes.arrayOf(PropTypes.string),
  }),
  context_summary: PropTypes.shape({
    stop_point_count: PropTypes.number,
    route_count: PropTypes.number,
  }),
});

export const groupNodeShape = PropTypes.shape({
  node_id: PropTypes.string,
  label: PropTypes.string,
  source_ref: PropTypes.string,
});

export const transferEdgeShape = PropTypes.shape({
  from_node_id: PropTypes.string,
  to_node_id: PropTypes.string,
  min_walk_seconds: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
});

export const mergeShape = PropTypes.shape({
  entity_id: PropTypes.string,
  display_name: PropTypes.string,
  member_refs: PropTypes.arrayOf(PropTypes.string),
});

export const groupShape = PropTypes.shape({
  entity_id: PropTypes.string,
  display_name: PropTypes.string,
  member_refs: PropTypes.arrayOf(PropTypes.string),
  internal_nodes: PropTypes.arrayOf(groupNodeShape),
  transfer_matrix: PropTypes.arrayOf(transferEdgeShape),
});

export const workspaceShape = PropTypes.shape({
  note: PropTypes.string,
  groups: PropTypes.arrayOf(groupShape),
  merges: PropTypes.arrayOf(mergeShape),
});

export const railItemShape = PropTypes.shape({
  ref: PropTypes.string.isRequired,
  kind: PropTypes.string,
  display_name: PropTypes.string,
  member_refs: PropTypes.arrayOf(PropTypes.string),
  internal_nodes: PropTypes.arrayOf(groupNodeShape),
  provider_labels: PropTypes.arrayOf(PropTypes.string),
  lat: PropTypes.number,
  lon: PropTypes.number,
  map_kind: PropTypes.string,
  candidate: candidateShape,
});

export const clusterListItemShape = PropTypes.shape({
  cluster_id: PropTypes.string.isRequired,
  display_name: PropTypes.string,
  severity: PropTypes.string,
  effective_status: PropTypes.string,
  status: PropTypes.string,
  candidate_count: PropTypes.number,
  has_workspace: PropTypes.bool,
  workspace_version: PropTypes.number,
});

export const evidenceRowShape = PropTypes.shape({
  evidence_type: PropTypes.string,
  source_global_station_id: PropTypes.string,
  target_global_station_id: PropTypes.string,
  status: PropTypes.string,
  score: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  raw_value: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  category: PropTypes.string,
  is_seed_rule: PropTypes.bool,
  seed_reasons: PropTypes.arrayOf(PropTypes.string),
  details: PropTypes.object,
});

export const pairSummaryShape = PropTypes.shape({
  source_global_station_id: PropTypes.string,
  target_global_station_id: PropTypes.string,
  summary: PropTypes.string,
  score: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  categories: PropTypes.arrayOf(PropTypes.string),
  seed_reasons: PropTypes.arrayOf(PropTypes.string),
  supporting_count: PropTypes.number,
  warning_count: PropTypes.number,
  missing_count: PropTypes.number,
});

export const clusterDetailShape = PropTypes.shape({
  cluster_id: PropTypes.string,
  display_name: PropTypes.string,
  status: PropTypes.string,
  effective_status: PropTypes.string,
  workspace_version: PropTypes.number,
  has_workspace: PropTypes.bool,
  candidates: PropTypes.arrayOf(candidateShape),
  evidence: PropTypes.arrayOf(evidenceRowShape),
  pair_summaries: PropTypes.arrayOf(pairSummaryShape),
  evidence_summary: PropTypes.object,
  edit_history: PropTypes.arrayOf(
    PropTypes.shape({
      event_type: PropTypes.string,
      requested_by: PropTypes.string,
      created_at: PropTypes.string,
    }),
  ),
});
