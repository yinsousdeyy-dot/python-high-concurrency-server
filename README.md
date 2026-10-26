# Python High-Concurrency HTTP Server (FastAPI + Uvicorn + uvloop)

## Overview
A production-grade Python asynchronous HTTP server engineered to handle **10,000+ concurrent connections** using **FastAPI**, **Uvicorn (with uvloop)**, explicit **asyncio Semaphore backpressure**, bounded **asyncio.Queue** micro-batching, and **Lifespan Graceful Shutdown**.

## Quick Start
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the server (with uvloop acceleration):
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1 --loop uvloop
   ```
3. Check Stats:
   ```bash
   curl http://localhost:8080/api/v1/stats
   ```
4. Test Ingestion:
   ```bash
   curl -X POST http://localhost:8080/api/v1/ingest \
     -H "Content-Type: application/json" \
     -d '{"client_id":"sensor-01","event_type":"METRIC","timestamp":1761475200}'
   ```
