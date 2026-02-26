# Agent Prompt: Task 4.1 - Local vLLM Worker Setup

**Context & Objective:**
You are an AI agent tasked with executing Phase 4, Task 4.1 of the Trainscanner V2 migration.
Your goal is to build a Python worker node designed to host quantized local Large Language Models (LLMs) using `vLLM` or `Ollama`.
Because Trainscanner needs to process millions of stations, we absolutely _cannot_ afford the latency or cost of live web-based API calls (like OpenAI/Anthropic) during daily ingestion streams. This local AI will be responsible for computing "Merge Confidence Scores" and deduping novel stations against our PostGIS database.

**Current State Reference:**

- Phase 3 (Temporal Orchestration) is complete. The TS `control-plane` is firing off jobs.
- The `netex_stops_staging` table is filling up with novel stations.
- You are strictly responsible for setting up the Python API service and the model environment—_not_ the deep spatial matching logic itself (that is Task 4.2).

**Your Instructions:**

1. **Bootstrap the Python Worker Service:**
   - Create a new directory for this service (e.g., `workers/python-ai/`).
   - Create a `requirements.txt` or `pyproject.toml` including necessary dependencies (e.g., `fastapi`, `uvicorn`, `vllm` or `ollama`, `temporalio` Python SDK, `psycopg2-binary` or `asyncpg`).
   - Initialize a basic FastAPI web server.

2. **Temporal Integration:**
   - Instead of (or alongside) the FastAPI server, setup a Python Temporal Worker that listens to the `entity-update` task queue.
   - Define a simple Temporal Activity stub (e.g., `calculate_merge_score`) that the TypeScript orchestrator can call. For now, just have it return a dummy high-confidence score (e.g., `{"confidence": 0.95, "action": "merge"}`).

3. **Local LLM Harness (vLLM / Ollama):**
   - Write instructions or a `Dockerfile` for downloading and running a high-performance, quantized local model (e.g., `meta-llama/Llama-3-8B-Instruct` AWQ/GGUF or a small specific fine-tune).
   - Create a utility class within your Python app that wraps the API calls to this local model.
   - The class should parse a prompt (e.g., comparing two station names and coordinates) and return a structured JSON response (the Confidence Score).

4. **Integration & Validation:**
   - Ensure the new Python service has a clean entry point (`python main.py`).
   - Write a `README.md` instructing how a developer can boot the Python Temporal worker and ensure it connects to the local Temporal cluster without crashing.

**Deliverables:**

- The new `workers/python-ai/` directory containing the FastAPI/Temporal app.
- Python Dependency files (`requirements.txt`).
- A `Dockerfile` or `docker-compose.yml` snippet illustrating how the GPU/CPU local LLM environment is hosted.

Please begin your analysis and implement the Python Worker.
