import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.config import settings
from app.models import PayloadRequest
from app.state import server_state
from app.service import background_queue_worker

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_state.semaphore = asyncio.Semaphore(settings.max_concurrent_permits)
    server_state.task_queue = asyncio.Queue(maxsize=settings.queue_capacity)
    
    worker_task = asyncio.create_task(background_queue_worker())
    print("🚀 Python High-Concurrency Server started. Ready to accept traffic.")
    
    yield
    
    print("⚠️ SIGTERM / Shutdown initiated! Rejecting new requests...")
    server_state.shutting_down = True
    
    deadline = time.time() + 25.0
    while server_state.in_flight_requests > 0 and time.time() < deadline:
        print(f"Waiting for {server_state.in_flight_requests} in-flight requests to finish...")
        await asyncio.sleep(0.2)
        
    print(f"Flushing remaining {server_state.task_queue.qsize()} items in async queue...")
    try:
        await asyncio.wait_for(server_state.task_queue.join(), timeout=5.0)
    except asyncio.TimeoutError:
        print("⚠️ Queue drain timeout reached.")
        
    worker_task.cancel()
    print("✅ Graceful shutdown complete. Server exited cleanly.")

app = FastAPI(title="Python High-Concurrency HTTP Server", version="1.0.0", lifespan=lifespan)

@app.middleware("http")
async def backpressure_and_lifecycle_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/metrics") or path == "/api/v1/stats":
        return await call_next(request)

    if server_state.shutting_down:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Connection": "close"},
            content={"error": "Server is shutting down gracefully. Please retry on another node."}
        )

    try:
        acquired = await asyncio.wait_for(
            server_state.semaphore.acquire(),
            timeout=settings.acquire_timeout_sec
        )
    except asyncio.TimeoutError:
        server_state.rejected_counter += 1
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": "2", "X-Backpressure-Rejected-Total": str(server_state.rejected_counter)},
            content={"error": "Server at maximum capacity (Backpressure triggered). Try again shortly."}
        )

    server_state.in_flight_requests += 1
    try:
        response = await call_next(request)
        return response
    finally:
        server_state.in_flight_requests -= 1
        server_state.semaphore.release()

@app.post("/api/v1/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_payload(payload: PayloadRequest):
    try:
        server_state.task_queue.put_nowait(payload)
    except asyncio.QueueFull:
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"status": "REJECTED_BUFFER_FULL", "error": "Internal queue saturated."}
        )

    await asyncio.sleep(0.05)

    return {
        "status": "ACCEPTED",
        "client_id": payload.client_id,
        "async_loop": "uvicorn-uvloop"
    }

@app.get("/api/v1/stats")
async def get_stats():
    return {
        "server": "Python FastAPI High-Concurrency Server",
        "in_flight_requests": server_state.in_flight_requests,
        "available_semaphore_permits": server_state.semaphore._value,
        "internal_queue_size": server_state.task_queue.qsize(),
        "total_processed": server_state.processed_counter,
        "total_rejected_by_backpressure": server_state.rejected_counter,
        "shutting_down": server_state.shutting_down,
        "uptime_seconds": round(time.time() - server_state.start_time, 2)
    }
