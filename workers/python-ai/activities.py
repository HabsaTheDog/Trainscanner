from temporalio import activity
import json

class LocalLLMWrapper:
    """
    Dummy wrapper around a local LLM API (e.g. vLLM or Ollama).
    In an actual Phase 4.2 implementation, this will prompt the local model,
    comparing a raw station entry to the OSM canonical database.
    """
    def __init__(self, endpoint: str = "http://localhost:8000/v1"):
        self.endpoint = endpoint
        # Example setup for openai client library pointing to local vLLM API:
        # self.client = AsyncOpenAI(base_url=self.endpoint, api_key="EMPTY")
    
    async def analyze_station_similarity(self, station_id: str) -> dict:
        activity.logger.info(f"Local LLM analyzing station: {station_id}")
        # Mocking an LLM JSON mode response
        return {
            "confidence": 0.95,
            "action": "merge",
            "reasoning": "High string similarity and identical geographic bounding box.",
            "station_id": station_id
        }

@activity.defn
async def calculate_merge_score(station_id: str) -> str:
    activity.logger.info(f"Activity started for calculating merge score: {station_id}")
    
    # 1. Initialize our local model interface
    llm = LocalLLMWrapper()
    
    # 2. Call the LLM to process the 'novel' entity matching
    result = await llm.analyze_station_similarity(station_id)
    
    # 3. Return JSON string representing the output decision back to Temporal
    return json.dumps(result)
