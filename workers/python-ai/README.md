# Python Local AI Worker (Trainscanner V2)

This Python service runs a FastAPI environment housing a Temporal Worker. It handles AI-driven Spatial Inference using local quantized LLMs.
It sits on the `entity-update` Temporal Task Queue and calculates `Merge Confidence Scores` to deduplicate novel European train stations without expensive live API calls (e.g. to OpenAI).

## Local Configuration

Currently, the service acts as a stub to establish the core orchestration connection with the TypeScript Control Plane.

### Prerequisites

- Python 3.11+
- A running Temporal cluster (`temporal server start-dev` or Docker)

### Installation

```bash
cd workers/python-ai
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running the Worker

```bash
# Connects to Temporal on localhost:7233 by default
export PORT=8080
export TEMPORAL_ADDRESS=localhost:7233
export DATABASE_URL="postgresql://trainscanner:trainscanner@localhost:5432/trainscanner"

python main.py
```

## Integrating vLLM (Phase 4.2 Guidance)

To spin up a local Llama-3 (or similar) model, you can either run Ollama locally or mount a `vllm` docker image.
For example:

```bash
docker run --gpus all -v ~/.cache/huggingface:/root/.cache/huggingface --env "HUGGING_FACE_HUB_TOKEN=<secret>" -p 8000:8000 ipcrm/vllm:latest --model meta-llama/Meta-Llama-3-8B-Instruct
```

Once running, update the `LocalLLMWrapper` in `activities.py` to prompt the model endpoint on `localhost:8000/v1` using standard OpenAI SDK bindings!
