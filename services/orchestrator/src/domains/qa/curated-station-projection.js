const crypto = require("node:crypto");

function toCleanString(value) {
  return String(value || "").trim();
}

function normalizeStringArray(raw) {
  const list = Array.isArray(raw) ? raw : [];
  const out = [];
  const seen = new Set();

  for (const item of list) {
    const clean = toCleanString(item);
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }

  return out;
}

function hashStableId(prefix, input) {
  const digest = crypto
    .createHash("sha1")
    .update(String(input || ""))
    .digest("hex")
    .slice(0, 20);
  return `${prefix}_${digest}`;
}

function buildEntityFromMembers({
  clusterId,
  operation,
  requestedBy,
  groupLabel,
  renameTo,
  memberStationIds,
  targetCanonicalStationId,
  metadata = {},
}) {
  const members = normalizeStringArray(memberStationIds);
  if (members.length === 0) {
    return null;
  }

  const targetId = toCleanString(targetCanonicalStationId) || members[0];
  const label = toCleanString(groupLabel) || "selected";
  const displayName =
    toCleanString(renameTo) || label || `Curated ${operation}`;

  const curatedStationId = hashStableId(
    "curst",
    `${clusterId}|${operation}|${label}|${members.join(",")}|${displayName}`,
  );

  const namingReason = toCleanString(renameTo)
    ? `Derived from ${operation} rename_to`
    : `Derived from ${operation} candidate grouping`;

  const entity = {
    curated_station_id: curatedStationId,
    derived_operation: operation,
    display_name: displayName,
    naming_reason: namingReason,
    metadata: {
      ...metadata,
      group_label: label,
      target_canonical_station_id: targetId,
    },
  };

  const memberRows = members.map((stationId, index) => ({
    curated_station_id: curatedStationId,
    canonical_station_id: stationId,
    member_role: stationId === targetId ? "primary" : "member",
    member_rank: index + 1,
    contribution: {
      selected: true,
      operation,
      group_label: label,
    },
  }));

  const fieldRows = [
    {
      curated_station_id: curatedStationId,
      field_name: "display_name",
      field_value: displayName,
      source_kind: toCleanString(renameTo) ? "manual_decision" : "derived",
      source_ref: toCleanString(renameTo) ? requestedBy : targetId,
      metadata: {
        operation,
        group_label: label,
      },
    },
  ];

  const lineageRows = [
    {
      curated_station_id: curatedStationId,
      operation,
    },
  ];

  return {
    entity,
    memberRows,
    fieldRows,
    lineageRows,
  };
}

function pushProjectionRows(target, candidateRows) {
  if (!candidateRows) {
    return;
  }
  target.entities.push(candidateRows.entity);
  target.members.push(...candidateRows.memberRows);
  target.fieldProvenance.push(...candidateRows.fieldRows);
  target.lineage.push(...candidateRows.lineageRows);
}

function emptyProjectionRows() {
  return {
    entities: [],
    members: [],
    fieldProvenance: [],
    lineage: [],
  };
}

function buildCuratedProjectionRows(input = {}) {
  const clusterId = toCleanString(input.clusterId);
  const decision =
    input.decision && typeof input.decision === "object" ? input.decision : {};
  const operation = toCleanString(decision.operation);
  const requestedBy = toCleanString(decision.requestedBy) || "curation_tool";
  const selectedStationIds = normalizeStringArray(decision.selectedStationIds);
  const groups = Array.isArray(decision.groups) ? decision.groups : [];
  const renameTo = toCleanString(decision.renameTo);

  const rows = emptyProjectionRows();

  if (!clusterId || !operation) {
    return rows;
  }

  if (groups.length > 0) {
    for (let idx = 0; idx < groups.length; idx += 1) {
      const group =
        groups[idx] && typeof groups[idx] === "object" ? groups[idx] : {};
      const candidateRows = buildEntityFromMembers({
        clusterId,
        operation,
        requestedBy,
        groupLabel: group.groupLabel || `group-${idx + 1}`,
        renameTo: group.renameTo || renameTo,
        memberStationIds: group.memberStationIds || [],
        targetCanonicalStationId: group.targetCanonicalStationId || "",
        metadata: {
          source: "qa-decision",
          section_type: toCleanString(group.sectionType),
          section_name: toCleanString(group.sectionName),
        },
      });
      pushProjectionRows(rows, candidateRows);
    }

    return rows;
  }

  const targetCanonicalStationId = selectedStationIds[0] || "";
  const fallbackLabel = operation === "merge" ? "merge-selected" : "selected";
  const candidateRows = buildEntityFromMembers({
    clusterId,
    operation,
    requestedBy,
    groupLabel: fallbackLabel,
    renameTo,
    memberStationIds: selectedStationIds,
    targetCanonicalStationId,
    metadata: {
      source: "qa-decision",
    },
  });

  pushProjectionRows(rows, candidateRows);
  return rows;
}

function persistCuratedProjection(tx, input = {}) {
  if (!tx || typeof tx.add !== "function") {
    return;
  }

  const clusterId = toCleanString(input.clusterId);
  const country = toCleanString(input.country).toUpperCase();
  const requestedBy = toCleanString(input.requestedBy) || "curation_tool";
  const decisionPayload =
    input.decisionPayload && typeof input.decisionPayload === "object"
      ? input.decisionPayload
      : {};
  const entities = Array.isArray(input.entities) ? input.entities : [];
  const members = Array.isArray(input.members) ? input.members : [];
  const fieldProvenance = Array.isArray(input.fieldProvenance)
    ? input.fieldProvenance
    : [];
  const lineage = Array.isArray(input.lineage) ? input.lineage : [];

  if (!clusterId || !country || entities.length === 0) {
    return;
  }

  tx.add(
    `
    UPDATE qa_curated_stations
    SET
      status = 'superseded',
      updated_by = :'requested_by',
      updated_at = now()
    WHERE primary_cluster_id = :'cluster_id'
      AND status = 'active'
    `,
    {
      cluster_id: clusterId,
      requested_by: requestedBy,
    },
  );

  tx.add(
    `
    INSERT INTO qa_curated_stations (
      curated_station_id,
      country,
      status,
      primary_cluster_id,
      latest_decision_id,
      derived_operation,
      display_name,
      naming_reason,
      metadata,
      created_by,
      updated_by,
      created_at,
      updated_at
    )
    SELECT
      e.curated_station_id,
      :'country'::char(2),
      'active',
      :'cluster_id',
      (SELECT decision_id FROM _decision_ctx LIMIT 1),
      e.derived_operation,
      e.display_name,
      NULLIF(e.naming_reason, ''),
      COALESCE(e.metadata, '{}'::jsonb),
      :'requested_by',
      :'requested_by',
      now(),
      now()
    FROM jsonb_to_recordset(:'entities'::jsonb) AS e(
      curated_station_id text,
      derived_operation text,
      display_name text,
      naming_reason text,
      metadata jsonb
    )
    ON CONFLICT (curated_station_id)
    DO UPDATE SET
      country = EXCLUDED.country,
      status = 'active',
      primary_cluster_id = EXCLUDED.primary_cluster_id,
      latest_decision_id = EXCLUDED.latest_decision_id,
      derived_operation = EXCLUDED.derived_operation,
      display_name = EXCLUDED.display_name,
      naming_reason = EXCLUDED.naming_reason,
      metadata = qa_curated_stations.metadata || EXCLUDED.metadata,
      updated_by = EXCLUDED.updated_by,
      updated_at = now()
    `,
    {
      cluster_id: clusterId,
      country,
      requested_by: requestedBy,
      entities: JSON.stringify(entities),
    },
  );

  if (members.length > 0) {
    tx.add(
      `
      DELETE FROM qa_curated_station_members m
      USING (
        SELECT DISTINCT x.curated_station_id
        FROM jsonb_to_recordset(:'members'::jsonb) AS x(
          curated_station_id text,
          canonical_station_id text,
          member_role text,
          member_rank integer,
          contribution jsonb
        )
      ) d
      WHERE m.curated_station_id = d.curated_station_id
      `,
      {
        members: JSON.stringify(members),
      },
    );

    tx.add(
      `
      INSERT INTO qa_curated_station_members (
        curated_station_id,
        canonical_station_id,
        member_role,
        member_rank,
        contribution,
        created_at
      )
      SELECT
        m.curated_station_id,
        m.canonical_station_id,
        CASE WHEN m.member_role = 'primary' THEN 'primary' ELSE 'member' END,
        GREATEST(1, COALESCE(m.member_rank, 1)),
        COALESCE(m.contribution, '{}'::jsonb),
        now()
      FROM jsonb_to_recordset(:'members'::jsonb) AS m(
        curated_station_id text,
        canonical_station_id text,
        member_role text,
        member_rank integer,
        contribution jsonb
      )
      WHERE NULLIF(m.curated_station_id, '') IS NOT NULL
        AND NULLIF(m.canonical_station_id, '') IS NOT NULL
      ON CONFLICT (curated_station_id, canonical_station_id)
      DO UPDATE SET
        member_role = EXCLUDED.member_role,
        member_rank = EXCLUDED.member_rank,
        contribution = EXCLUDED.contribution
      `,
      {
        members: JSON.stringify(members),
      },
    );
  }

  if (fieldProvenance.length > 0) {
    tx.add(
      `
      DELETE FROM qa_curated_station_field_provenance p
      USING (
        SELECT DISTINCT x.curated_station_id
        FROM jsonb_to_recordset(:'field_rows'::jsonb) AS x(
          curated_station_id text,
          field_name text,
          field_value text,
          source_kind text,
          source_ref text,
          metadata jsonb
        )
      ) d
      WHERE p.curated_station_id = d.curated_station_id
        AND p.field_name = 'display_name'
      `,
      {
        field_rows: JSON.stringify(fieldProvenance),
      },
    );

    tx.add(
      `
      INSERT INTO qa_curated_station_field_provenance (
        curated_station_id,
        field_name,
        field_value,
        source_kind,
        source_ref,
        metadata,
        created_at
      )
      SELECT
        p.curated_station_id,
        p.field_name,
        p.field_value,
        p.source_kind,
        COALESCE(p.source_ref, ''),
        COALESCE(p.metadata, '{}'::jsonb),
        now()
      FROM jsonb_to_recordset(:'field_rows'::jsonb) AS p(
        curated_station_id text,
        field_name text,
        field_value text,
        source_kind text,
        source_ref text,
        metadata jsonb
      )
      WHERE NULLIF(p.curated_station_id, '') IS NOT NULL
        AND NULLIF(p.field_name, '') IS NOT NULL
      ON CONFLICT (curated_station_id, field_name, source_kind, source_ref)
      DO UPDATE SET
        field_value = EXCLUDED.field_value,
        metadata = qa_curated_station_field_provenance.metadata || EXCLUDED.metadata
      `,
      {
        field_rows: JSON.stringify(fieldProvenance),
      },
    );
  }

  if (lineage.length > 0) {
    tx.add(
      `
      INSERT INTO qa_curated_station_lineage (
        curated_station_id,
        decision_id,
        cluster_id,
        operation,
        decision_payload,
        created_at
      )
      SELECT
        l.curated_station_id,
        (SELECT decision_id FROM _decision_ctx LIMIT 1),
        :'cluster_id',
        l.operation,
        :'decision_payload'::jsonb,
        now()
      FROM jsonb_to_recordset(:'lineage_rows'::jsonb) AS l(
        curated_station_id text,
        operation text
      )
      WHERE NULLIF(l.curated_station_id, '') IS NOT NULL
      ON CONFLICT (curated_station_id, decision_id)
      DO UPDATE SET
        cluster_id = EXCLUDED.cluster_id,
        operation = EXCLUDED.operation,
        decision_payload = EXCLUDED.decision_payload
      `,
      {
        cluster_id: clusterId,
        decision_payload: JSON.stringify(decisionPayload),
        lineage_rows: JSON.stringify(lineage),
      },
    );
  }
}

module.exports = {
  buildCuratedProjectionRows,
  persistCuratedProjection,
};
