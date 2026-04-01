import { graphqlQuery } from "./graphql";

const CONFIGS_QUERY = `
  query AiEvaluationConfigs {
    aiEvaluationConfigs {
      config_id
      config_key
      version
      name
      description
      provider
      model
      model_params
      system_prompt
      context_sections
      context_preamble
      created_by
      created_at
    }
  }
`;

const CLUSTERS_QUERY = `
  query AiEvaluationClusterPicker($status: String) {
    globalClusters(status: $status) {
      items {
        cluster_id
        display_name
        status
        effective_status
        severity
        candidate_count
      }
    }
  }
`;

const RUNS_QUERY = `
  query AiEvaluationRuns($status: String, $mode: String, $limit: Int) {
    aiEvaluationRuns(status: $status, mode: $mode, limit: $limit) {
      total_count
      limit
      items {
        run_id
        mode
        status
        dataset_source
        gold_set_id
        config_id
        config_snapshot
        filters
        summary_metrics
        progress
        requested_by
        error_message
        temporal_workflow_id
        created_at
        started_at
        ended_at
      }
    }
  }
`;

const RUN_QUERY = `
  query AiEvaluationRun($runId: ID!) {
    aiEvaluationRun(runId: $runId) {
      run_id
      mode
      status
      dataset_source
      gold_set_id
      config_id
      config_snapshot
      filters
      summary_metrics
      progress
      requested_by
      error_message
      temporal_workflow_id
      created_at
      started_at
      ended_at
      items {
        run_item_id
        merge_cluster_id
        item_status
        truth_snapshot
        input_context_snapshot
        prompt_snapshot
        raw_model_response
        normalized_prediction
        comparison
        token_usage
        estimated_cost_usd
        latency_ms
        error_message
        created_at
        updated_at
      }
    }
  }
`;

const GOLD_SETS_QUERY = `
  query AiEvaluationGoldSets {
    aiEvaluationGoldSets {
      gold_set_id
      slug
      name
      description
      is_frozen
      created_by
      created_at
      updated_at
    }
  }
`;

const GOLD_SET_QUERY = `
  query AiEvaluationGoldSet($goldSetId: ID!) {
    aiEvaluationGoldSet(goldSetId: $goldSetId) {
      gold_set_id
      slug
      name
      description
      is_frozen
      created_by
      created_at
      updated_at
      items {
        gold_set_id
        merge_cluster_id
        note
        truth_snapshot
        created_at
      }
    }
  }
`;

const CREATE_CONFIG_MUTATION = `
  mutation CreateAiEvaluationConfigVersion($input: AiEvaluationConfigInput!) {
    createAiEvaluationConfigVersion(input: $input) {
      config_id
      config_key
      version
      name
      description
      provider
      model
      model_params
      system_prompt
      context_sections
      context_preamble
      created_by
      created_at
    }
  }
`;

const PREVIEW_MUTATION = `
  mutation RunAiEvaluationPreview($clusterId: ID!, $input: AiEvaluationPreviewInput!) {
    runAiEvaluationPreview(clusterId: $clusterId, input: $input) {
      run {
        run_id
        status
        summary_metrics
      }
      result {
        run_item_id
        merge_cluster_id
        item_status
        truth_snapshot
        input_context_snapshot
        prompt_snapshot
        raw_model_response
        normalized_prediction
        comparison
        token_usage
        estimated_cost_usd
        latency_ms
        error_message
      }
    }
  }
`;

const START_BENCHMARK_MUTATION = `
  mutation StartAiEvaluationBenchmark($input: AiEvaluationBenchmarkInput!) {
    startAiEvaluationBenchmark(input: $input) {
      run_id
      mode
      status
      dataset_source
      gold_set_id
      config_id
      config_snapshot
      filters
      summary_metrics
      progress
      requested_by
      error_message
      temporal_workflow_id
      created_at
      started_at
      ended_at
    }
  }
`;

const CREATE_GOLD_SET_MUTATION = `
  mutation CreateAiEvaluationGoldSet($input: AiEvaluationGoldSetInput!) {
    createAiEvaluationGoldSet(input: $input) {
      gold_set_id
      slug
      name
      description
      is_frozen
      created_by
      created_at
      updated_at
    }
  }
`;

const REPLACE_GOLD_SET_ITEMS_MUTATION = `
  mutation ReplaceAiEvaluationGoldSetItems($goldSetId: ID!, $clusterIds: [ID!]!) {
    replaceAiEvaluationGoldSetItems(goldSetId: $goldSetId, clusterIds: $clusterIds) {
      gold_set_id
      slug
      name
      description
      is_frozen
      created_by
      created_at
      updated_at
      items {
        merge_cluster_id
        note
        truth_snapshot
        created_at
      }
    }
  }
`;

export async function fetchEvaluationConfigs() {
  const data = await graphqlQuery(CONFIGS_QUERY);
  return Array.isArray(data.aiEvaluationConfigs) ? data.aiEvaluationConfigs : [];
}

export async function fetchClusterPicker(status = "resolved") {
  const data = await graphqlQuery(CLUSTERS_QUERY, { status });
  const items = data.globalClusters?.items;
  return Array.isArray(items) ? items : [];
}

export async function createEvaluationConfigVersion(input) {
  const data = await graphqlQuery(CREATE_CONFIG_MUTATION, { input });
  return data.createAiEvaluationConfigVersion;
}

export async function runEvaluationPreview(clusterId, input) {
  const data = await graphqlQuery(PREVIEW_MUTATION, { clusterId, input });
  return data.runAiEvaluationPreview;
}

export async function fetchEvaluationRuns(filters = {}) {
  const data = await graphqlQuery(RUNS_QUERY, {
    status: filters.status || null,
    mode: filters.mode || null,
    limit: filters.limit || 20,
  });
  return data.aiEvaluationRuns || { items: [], total_count: 0, limit: 20 };
}

export async function fetchEvaluationRun(runId) {
  const data = await graphqlQuery(RUN_QUERY, { runId });
  return data.aiEvaluationRun;
}

export async function startEvaluationBenchmark(input) {
  const data = await graphqlQuery(START_BENCHMARK_MUTATION, { input });
  return data.startAiEvaluationBenchmark;
}

export async function fetchGoldSets() {
  const data = await graphqlQuery(GOLD_SETS_QUERY);
  return Array.isArray(data.aiEvaluationGoldSets) ? data.aiEvaluationGoldSets : [];
}

export async function fetchGoldSet(goldSetId) {
  const data = await graphqlQuery(GOLD_SET_QUERY, { goldSetId });
  return data.aiEvaluationGoldSet;
}

export async function createGoldSet(input) {
  const data = await graphqlQuery(CREATE_GOLD_SET_MUTATION, { input });
  return data.createAiEvaluationGoldSet;
}

export async function replaceGoldSetItems(goldSetId, clusterIds) {
  const data = await graphqlQuery(REPLACE_GOLD_SET_ITEMS_MUTATION, {
    goldSetId,
    clusterIds,
  });
  return data.replaceAiEvaluationGoldSetItems;
}
