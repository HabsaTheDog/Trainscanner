import difflib
import json
import os
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

try:
    from litellm import completion
except Exception:  # pragma: no cover - local fallback when dependency is absent
    completion = None

app = FastAPI(title="Trainscanner AI Scoring Service")


class ClusterCandidate(BaseModel):
    global_station_id: str
    name: str


class ScoreRequest(BaseModel):
    cluster_id: str
    candidates: list[ClusterCandidate]


class EvaluationConfig(BaseModel):
    provider: str = "litellm"
    model: str
    model_params: dict[str, Any] = Field(default_factory=dict)
    system_prompt: str
    context_sections: list[str] = Field(default_factory=list)
    context_preamble: str = ""


class EvaluationPrediction(BaseModel):
    verdict: str
    merges: list[list[str]] = Field(default_factory=list)
    groups: list[dict[str, Any]] = Field(default_factory=list)
    keep_separate_sets: list[list[str]] = Field(default_factory=list)
    renames: list[dict[str, Any]] = Field(default_factory=list)
    rationale: str = ""
    confidence_score: float = 0.0


class EvaluationRequest(BaseModel):
    mode: str = "preview"
    cluster_id: str
    config: EvaluationConfig
    input_context: dict[str, Any] = Field(default_factory=dict)


class ScoreResponse(BaseModel):
    cluster_id: str
    confidence_score: float
    suggested_action: str
    reasoning: str


class EvaluationResponse(BaseModel):
    cluster_id: str
    normalized_prediction: EvaluationPrediction
    token_usage: dict[str, Any] = Field(default_factory=dict)
    estimated_cost_usd: float | None = None
    latency_ms: int
    provider: str
    model: str
    raw_model_response: dict[str, Any] = Field(default_factory=dict)


def calculate_string_similarity(str1: str, str2: str) -> float:
    return difflib.SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


def _extract_candidate_rows(input_context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = input_context.get("candidate_core")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    candidates = []
    for bucket in ("aliases", "provenance", "network_context"):
        rows = input_context.get(bucket)
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    station_id = str(row.get("global_station_id", "")).strip()
                    if station_id and not any(
                        entry.get("global_station_id") == station_id
                        for entry in candidates
                    ):
                        candidates.append(
                            {
                                "global_station_id": station_id,
                                "display_name": station_id,
                            }
                        )
    return candidates


def _heuristic_prediction(input_context: dict[str, Any]) -> EvaluationPrediction:
    candidate_rows = _extract_candidate_rows(input_context)
    names = [
        str(row.get("display_name", "")).strip()
        for row in candidate_rows
        if str(row.get("display_name", "")).strip()
    ]
    candidate_ids = [
        str(row.get("global_station_id", "")).strip()
        for row in candidate_rows
        if str(row.get("global_station_id", "")).strip()
    ]
    if len(candidate_ids) <= 1:
        return EvaluationPrediction(
            verdict="merge_only",
            merges=[candidate_ids] if candidate_ids else [],
            rationale="Single candidate cluster.",
            confidence_score=1.0,
        )

    similarities = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            similarities.append(calculate_string_similarity(names[i], names[j]))
    avg_similarity = sum(similarities) / len(similarities) if similarities else 0

    if avg_similarity > 0.85:
        return EvaluationPrediction(
            verdict="merge_only",
            merges=[candidate_ids],
            rationale=f"High string similarity detected ({avg_similarity:.2f}).",
            confidence_score=round(avg_similarity, 3),
        )
    if avg_similarity > 0.6:
        return EvaluationPrediction(
            verdict="needs_review",
            rationale=f"Moderate similarity detected ({avg_similarity:.2f}).",
            confidence_score=round(avg_similarity, 3),
        )
    return EvaluationPrediction(
        verdict="keep_separate_only",
        keep_separate_sets=[candidate_ids],
        rationale=f"Low string similarity detected ({avg_similarity:.2f}).",
        confidence_score=round(1.0 - avg_similarity, 3),
    )


def _build_llm_messages(request: EvaluationRequest) -> list[dict[str, str]]:
    schema_hint = {
        "verdict": (
            "dismiss | merge_only | group_only | keep_separate_only | "
            "rename_only | mixed_resolution | needs_review"
        ),
        "merges": [["global_station_id", "global_station_id"]],
        "groups": [
            {
                "nodes": [{"label": "string", "station_ids": ["global_station_id"]}],
                "transfer_matrix": [
                    {
                        "from_label": "string",
                        "to_label": "string",
                        "min_walk_seconds": 0,
                        "bidirectional": True,
                    }
                ],
            }
        ],
        "keep_separate_sets": [["global_station_id", "global_station_id"]],
        "renames": [
            {
                "target_ref_type": "raw|merge|group",
                "target_station_ids": ["global_station_id"],
                "display_name": "string",
            }
        ],
        "rationale": "short explanation",
        "confidence_score": 0.0,
    }
    user_payload = {
        "context_preamble": request.config.context_preamble,
        "cluster_id": request.cluster_id,
        "input_context": request.input_context,
        "response_schema": schema_hint,
    }
    return [
        {
            "role": "system",
            "content": request.config.system_prompt.strip()
            + "\nReturn only valid JSON matching the requested response_schema.",
        },
        {
            "role": "user",
            "content": json.dumps(user_payload, ensure_ascii=True),
        },
    ]


def _call_litellm_prediction(
    request: EvaluationRequest,
) -> tuple[EvaluationPrediction, dict[str, Any], dict[str, Any], float | None]:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if completion is None or not api_key:
        raise RuntimeError("LiteLLM or OPENROUTER_API_KEY unavailable")

    params = dict(request.config.model_params)
    params.setdefault("temperature", 0)
    params.setdefault("top_p", 1)
    params.setdefault("api_key", api_key)
    if os.getenv("LITELLM_BASE_URL", "").strip():
        params.setdefault("api_base", os.getenv("LITELLM_BASE_URL", "").strip())

    response = completion(
        model=request.config.model,
        messages=_build_llm_messages(request),
        response_format={"type": "json_object"},
        **params,
    )
    choice = response["choices"][0]["message"]["content"]
    parsed = json.loads(choice)
    prediction = EvaluationPrediction.model_validate(parsed)
    usage = response.get("usage", {}) if isinstance(response, dict) else {}
    if hasattr(response, "model_dump"):
        raw_response = response.model_dump()
    elif isinstance(response, dict):
        raw_response = response
    else:
        raw_response = {"response": str(response)}
    estimated_cost = raw_response.get("_response_cost")
    return prediction, usage, raw_response, estimated_cost


@app.post("/evaluate-cluster", response_model=EvaluationResponse)
async def evaluate_cluster(request: EvaluationRequest):
    started = time.perf_counter()
    raw_response: dict[str, Any] = {}
    token_usage: dict[str, Any] = {}
    estimated_cost_usd: float | None = None
    provider = request.config.provider or "litellm"
    model = request.config.model

    try:
        prediction, token_usage, raw_response, estimated_cost_usd = (
            _call_litellm_prediction(request)
        )
    except Exception as exc:
        prediction = _heuristic_prediction(request.input_context)
        raw_response = {
            "fallback": "heuristic",
            "reason": str(exc),
        }

    latency_ms = int((time.perf_counter() - started) * 1000)
    return EvaluationResponse(
        cluster_id=request.cluster_id,
        normalized_prediction=prediction,
        token_usage=token_usage,
        estimated_cost_usd=estimated_cost_usd,
        latency_ms=latency_ms,
        provider=provider,
        model=model,
        raw_model_response=raw_response,
    )


@app.post(
    "/score-cluster",
    response_model=ScoreResponse,
    responses={400: {"description": "Candidates list cannot be empty"}},
)
async def score_cluster(request: ScoreRequest):
    if not request.candidates:
        raise HTTPException(status_code=400, detail="Candidates list cannot be empty")
    evaluation = await evaluate_cluster(
        EvaluationRequest(
            mode="preview",
            cluster_id=request.cluster_id,
            config=EvaluationConfig(
                model=os.getenv("AI_EVALUATION_DEFAULT_MODEL", "openrouter/auto"),
                system_prompt=(
                    "You are evaluating whether candidate train-station records "
                    "should be merged or kept separate. Return compact JSON."
                ),
                context_sections=["candidate_core"],
            ),
            input_context={
                "candidate_core": [
                    {
                        "global_station_id": candidate.global_station_id,
                        "display_name": candidate.name,
                    }
                    for candidate in request.candidates
                ]
            },
        )
    )
    verdict = evaluation.normalized_prediction.verdict
    if verdict == "merge_only":
        suggested_action = "merge"
    elif verdict == "keep_separate_only":
        suggested_action = "split"
    elif verdict == "dismiss":
        suggested_action = "dismiss"
    else:
        suggested_action = "review"

    return ScoreResponse(
        cluster_id=request.cluster_id,
        confidence_score=evaluation.normalized_prediction.confidence_score,
        suggested_action=suggested_action,
        reasoning=evaluation.normalized_prediction.rationale,
    )


@app.get("/health")
async def health_check():
    return {"status": "ok"}
