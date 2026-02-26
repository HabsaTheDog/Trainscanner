import json
import os
import asyncpg
from temporalio import activity

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://trainscanner:trainscanner@localhost:5432/trainscanner"
)

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
    
    async def analyze_station_similarity(self, staging_name: str, candidates: list) -> dict:
        activity.logger.info(f"Local LLM analyzing: '{staging_name}' against {len(candidates)} candidates")
        # Mocking an LLM JSON mode response
        if candidates:
            best = candidates[0]
            return {
                "confidence": 0.85,
                "action": "merge",
                "reasoning": f"LLM determined that {staging_name} matches {best['canonical_name']} based on geographical proximity.",
                "canonical_station_id": best["canonical_station_id"]
            }
            
        return {
            "confidence": 0.99,
            "action": "insert_new",
            "reasoning": "No viable canonical candidates exist in spatial vicinity. Safe to insert.",
            "canonical_station_id": None
        }

@activity.defn
async def calculate_merge_score(station_id: str) -> str:
    activity.logger.info(f"Activity started for calculating merge score: {station_id}")
    
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # 1. Fetch novel station properties
        staging_row = await conn.fetchrow("""
            SELECT staging_id, stop_name, geom
            FROM netex_stops_staging
            WHERE source_stop_id = $1 OR staging_id::text = $1
            LIMIT 1
        """, station_id)
        
        if not staging_row:
            activity.logger.warning(f"Station {station_id} not found in staging.")
            return json.dumps({
                "confidence": 0.0,
                "action": "insert_new",
                "reasoning": "Station not found in staging.",
                "canonical_station_id": None
            })
            
        staging_name = staging_row['stop_name']
        geom = staging_row['geom']
        
        if not geom:
            return json.dumps({
                "confidence": 0.6,
                "action": "insert_new",
                "reasoning": "Staging station lacks geometry; cannot perform spatial check.",
                "canonical_station_id": None
            })
            
        # 2. Query nearby canonical stations (within ~2000m) and their OSM payloads
        nearby_candidates = await conn.fetch("""
            SELECT 
                cs.canonical_station_id,
                cs.canonical_name,
                ST_Distance(cs.geom::geography, $1::geography) as distance,
                COALESCE(
                    jsonb_agg(nss.raw_payload) FILTER (WHERE nss.raw_payload IS NOT NULL), 
                    '[]'::jsonb
                ) as payloads
            FROM canonical_stations cs
            LEFT JOIN canonical_station_sources css 
                ON cs.canonical_station_id = css.canonical_station_id
            LEFT JOIN netex_stops_staging nss
                ON css.source_id = nss.source_id AND css.source_stop_id = nss.source_stop_id
            WHERE ST_DWithin(cs.geom::geography, $1::geography, 2000)
              AND cs.is_deleted = false
            GROUP BY cs.canonical_station_id, cs.canonical_name, cs.geom
            ORDER BY distance ASC
            LIMIT 10
        """, geom)

        llm_candidates = []
        
        # 3. OSM Multi-Language Check
        for cand in nearby_candidates:
            cand_id = cand['canonical_station_id']
            cand_name = cand['canonical_name']
            matched_alias = False
            
            if cand_name.lower() == staging_name.lower():
                matched_alias = True
                
            if not matched_alias:
                payloads = cand['payloads']
                if isinstance(payloads, str):
                    payloads = json.loads(payloads)
                
                for payload in payloads:
                    if matched_alias:
                        break
                    tags = payload.get('tags', payload)
                    if isinstance(tags, dict):
                        for k, v in tags.items():
                            if k.startswith('name:') and isinstance(v, str) and v.lower() == staging_name.lower():
                                matched_alias = True
                                break
                                
            if matched_alias:
                return json.dumps({
                    "confidence": 1.0,
                    "action": "merge",
                    "reasoning": f"Exact alias or canonical name match found locally ({cand['distance']:.1f}m).",
                    "canonical_station_id": cand_id
                })
                
            llm_candidates.append({
                "canonical_station_id": cand_id,
                "canonical_name": cand_name,
                "distance": cand['distance']
            })

        # 4. LLM Fallback (No exact alias match)
        llm = LocalLLMWrapper()
        result = await llm.analyze_station_similarity(staging_name, llm_candidates)
        return json.dumps(result)
        
    except Exception as e:
        activity.logger.error(f"Error calculating merge score: {str(e)}")
        return json.dumps({
            "confidence": 0.0,
            "action": "insert_new",
            "reasoning": f"Internal error during evaluation: {str(e)}",
            "canonical_station_id": None
        })
    finally:
        await conn.close()
