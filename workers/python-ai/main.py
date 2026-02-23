import asyncio
import os
import uvicorn
import logging
from fastapi import FastAPI
from temporalio.client import Client
from temporalio.worker import Worker

from activities import calculate_merge_score

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Trainscanner V2 - Python AI Worker")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "worker": "python-ai"}

async def start_temporal_worker():
    temporal_address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    logging.info(f"Connecting to Temporal server at {temporal_address}...")
    
    client = await Client.connect(temporal_address)
    
    worker = Worker(
        client,
        task_queue="entity-update",
        activities=[calculate_merge_score],
    )
    logging.info("Starting Python Temporal Worker on task_queue: entity-update...")
    await worker.run()

@app.on_event("startup")
async def startup_event():
    # Run the Temporal worker as a background task alongside the FastAPI HTTP server
    asyncio.create_task(start_temporal_worker())

def main():
    port = int(os.getenv("PORT", "8080"))
    logging.info(f"Starting FastAPI server on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

if __name__ == "__main__":
    main()
