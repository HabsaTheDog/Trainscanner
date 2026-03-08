import difflib

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Trainscanner AI Scoring Service")


class ClusterCandidate(BaseModel):
    global_station_id: str
    name: str


class ScoreRequest(BaseModel):
    cluster_id: str
    candidates: list[ClusterCandidate]


class ScoreResponse(BaseModel):
    cluster_id: str
    confidence_score: float
    suggested_action: str
    reasoning: str


def calculate_string_similarity(str1: str, str2: str) -> float:
    return difflib.SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


@app.post(
    "/score-cluster",
    response_model=ScoreResponse,
    responses={400: {"description": "Candidates list cannot be empty"}},
)
async def score_cluster(request: ScoreRequest):
    if not request.candidates:
        raise HTTPException(status_code=400, detail="Candidates list cannot be empty")

    if len(request.candidates) == 1:
        return ScoreResponse(
            cluster_id=request.cluster_id,
            confidence_score=1.0,
            suggested_action="approve",
            reasoning="Only one candidate in cluster.",
        )

    # Mock heuristic AI: compare strings of candidates
    # If they are very similar, suggest merge, otherwise split.
    names = [c.name for c in request.candidates]

    similarities = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            similarities.append(calculate_string_similarity(names[i], names[j]))

    avg_similarity = sum(similarities) / len(similarities) if similarities else 0

    if avg_similarity > 0.85:
        return ScoreResponse(
            cluster_id=request.cluster_id,
            confidence_score=round(avg_similarity, 3),
            suggested_action="merge",
            reasoning=f"High string similarity detected ({avg_similarity:.2f}).",
        )
    if avg_similarity > 0.60:
        return ScoreResponse(
            cluster_id=request.cluster_id,
            confidence_score=round(avg_similarity, 3),
            suggested_action="review",
            reasoning=(
                f"Moderate string similarity detected ({avg_similarity:.2f}). "
                "Requires manual intervention."
            ),
        )

    return ScoreResponse(
        cluster_id=request.cluster_id,
        confidence_score=round(1.0 - avg_similarity, 3),
        suggested_action="split",
        reasoning=(
            f"Low string similarity detected ({avg_similarity:.2f}). "
            "Likely distinct stations."
        ),
    )


@app.get("/health")
async def health_check():
    return {"status": "ok"}
