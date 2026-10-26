import asyncio
import logging
from app.state import server_state

logger = logging.getLogger("uvicorn.error")

async def background_queue_worker():
    batch = []
    while True:
        try:
            item = await asyncio.wait_for(server_state.task_queue.get(), timeout=0.1)
            batch.append(item)
            server_state.task_queue.task_done()

            while len(batch) < 100:
                try:
                    extra = server_state.task_queue.get_nowait()
                    batch.append(extra)
                    server_state.task_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            await asyncio.sleep(0.01)
            server_state.processed_counter += len(batch)
            batch.clear()

        except asyncio.TimeoutError:
            if server_state.shutting_down and server_state.task_queue.empty():
                break
            continue
        except Exception as e:
            logger.error(f"Background worker error: {e}")
