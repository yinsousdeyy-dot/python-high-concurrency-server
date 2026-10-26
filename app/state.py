import asyncio
import time

class ServerState:
    def __init__(self, max_permits: int, queue_capacity: int):
        self.semaphore = asyncio.Semaphore(max_permits)
        self.task_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_capacity)
        self.in_flight_requests = 0
        self.shutting_down = False
        self.rejected_counter = 0
        self.processed_counter = 0
        self.start_time = time.time()

server_state = ServerState(max_permits=3000, queue_capacity=5000)
