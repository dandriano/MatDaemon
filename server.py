#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

import matlab.engine
from aiohttp import WSMsgType, web


def convert_to_matlab_types(data: dict[str, any]) -> dict[str, any]:
    """
    Recursively convert native (numerics) types to MATLAB-compatible types.
    
    Args:
        data: Dictionary potentially containing numeric lists/values to convert
        
    Returns:
        Dictionary with MATLAB-compatible types
    """
    result = deepcopy(data)
    
    for key, value in result.items():
        if isinstance(value, dict):
            result[key] = convert_to_matlab_types(value)
        elif isinstance(value, list):
            if not value:
                continue

            if all(isinstance(item, list) and 
                    all(isinstance(sub, (int, float)) for sub in item) 
                    for item in value):
                result[key] = matlab.double(value)
            elif all(isinstance(item, (int, float)) for item in value):
                result[key] = matlab.double(value)
        elif isinstance(value, (int, float)):
            result[key] = matlab.double([value])
    
    return result


def convert_from_matlab_types(data: dict[str, any]) -> dict[str, any]:
    """
    Recursively convert MATLAB (numerics) types back to native types.
    
    Args:
        data: Dictionary potentially containing MATLAB types
        
    Returns:
        Dictionary with native types
    """
    result = deepcopy(data)
    
    for key, value in result.items():
        if isinstance(value, dict):
            result[key] = convert_from_matlab_types(value)
        elif isinstance(value, matlab.double):
            result[key] = value.tolist()
            # It's not clear, when I want a true scalar
            # if len(result[key]) == 1:
            #     result[key] = result[key][0]
    
    return result


class TaskPriority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


@dataclass(order=True)
class QueuedTask:
    priority: int
    request_id: str = field(compare=False)
    data: dict[str, any] = field(compare=False)
    future: asyncio.Future[any] = field(compare=False)
    timestamp: float = field(default_factory=time.monotonic)


class MatlabDaemon:
    """
    Daemon for processing tasks to matlab parfor process
    """
    def __init__(self, batch_size: int, drain_timeout: float, max_queue_size: int) -> None:
        self.batch_size = batch_size
        self.drain_timeout = drain_timeout
        self.max_queue_size = max_queue_size

        # Matlab control
        self._task_queue: asyncio.PriorityQueue[QueuedTask] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._matlab_engine: matlab.engine.MatlabEngine | None = None

        # State management
        self._is_running: bool = False
        self._processing_task: asyncio.Task[None] | None = None

        # Stats
        self._start_time = time.monotonic()
        self._stats: dict[str, any] = {
            "total_processed": 0,
            "batches_processed": 0,
            "avg_batch_size": 0.0,
            "worktime_seconds": 0
        }

        self._log = logging.getLogger("MatlabDaemon")

    async def submit_request(
        self,
        request_id: str,
        request_data: dict[str, any],
        priority: TaskPriority = TaskPriority.NORMAL
    ) -> asyncio.Future[dict[str, any]]:
        """
        Create and queue task from request

        Args:
            request_id: nuff said
            request_data: function name and parameters
            priority: HIGH, NORMAL, LOW

        Returns:
            asyncio.Future to await results

        Raises:
            RuntimeError: Queue is full or daemon failed to launch
        """
        if not self._is_running:
            raise RuntimeError("MATLAB daemon not running")

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        task = QueuedTask(
            priority=priority.value,
            request_id=request_id,
            data=request_data,
            future=future
        )

        try:
            await asyncio.wait_for(self._task_queue.put(task), timeout=1.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Queue is full, please try again later")

        return future

    def start(self) -> None:
        if self._is_running:
            self._log.warning("Daemon already running")
            return

        self._log.info("Starting MATLAB...")
        start_time = time.monotonic()

        try:
            self._matlab_engine = matlab.engine.start_matlab()

            # TODO: add matlab util / script paths (programmatically)
            # but right now assume constant path for our matlab processor
            self._matlab_engine.addpath(self._matlab_engine.genpath("matlab"))
            self._log.info(f"Starting MATLAB... Finished in {time.monotonic() - start_time:.2f}")

            self._is_running = True
            self._processing_task = asyncio.create_task(self._run())

            # TODO: use cpu count / batch size to determine size of parfor loop
            self._log.info(f"Batch size: {self.batch_size}\t"
                           f"Drain timeout: {self.drain_timeout}\t"
                           f"CPU count: {self._matlab_engine.feature("numcores")}")

        except Exception as e:
            self._log.error(f"Failed to start MATLAB: {e}")
            self._is_running = False
            raise

    async def stop(self) -> None:
        self._log.info("Shutting down MATLAB...")
        start_time = time.monotonic()
        self._is_running = False

        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        if self._matlab_engine:
            self._matlab_engine.quit()

        while not self._task_queue.empty():
            task = self._task_queue.get_nowait()
            task.future.cancel()

        self._log.info(f"Shutting down MATLAB... Finished in {time.monotonic() - start_time:.2f}")

    async def _run(self) -> None:
        while self._is_running:
            batch: list[QueuedTask] = []
            self._log.info("Waiting for tasks...")
            try:
                batch = await self._collect_batch()
                await self._process_batch(batch)
            except asyncio.CancelledError:
                if batch:
                    for task in batch:
                        if not task.future.done():
                            task.future.cancel()
                break
            except Exception as e:
                self._log.error(f"Error: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _collect_batch(self) -> list[QueuedTask]:
        result_batch: list[QueuedTask] = []
        while len(result_batch) < self.batch_size:
            # 1) First attempt to retrieve from a potentially empty queue is blocking,
            #    to avoid wasting resources when there are no tasks.
            # 2) Upon receiving a task from the queue, the timeout is updated (sliding timeout)
            #    to wait for additional tasks within that timeframe.
            timeout = self.drain_timeout if len(result_batch) > 0 else None
            try:
                if timeout is None:
                    result_batch.append(await self._task_queue.get())
                else:
                    result_batch.append(await asyncio.wait_for(self._task_queue.get(), timeout=timeout))
            except asyncio.TimeoutError:
                break

        return result_batch

    async def _process_batch(self, batch: list[QueuedTask]) -> None:
        if not batch or not self._matlab_engine:
            return

        fnames = [t.data["fname"] for t in batch]
        params = [convert_to_matlab_types(t.data["params"]) for t in batch]
        self._log.info("Executing batch...")
        
        # Process tasks
        start_time = time.monotonic()
        try:
            raw_results = await asyncio.get_running_loop().run_in_executor(
                None, self._matlab_engine.MatlabProcessorFunc, fnames, params
            )
        except Exception as err:
            for task in batch:
                if not task.future.done():
                    task.future.set_exception(err)
            return

        # Distribute results
        for task, result in zip(batch, raw_results):
            if not task.future.done():
                if isinstance(result, dict):
                    result = convert_from_matlab_types(result)
                task.future.set_result(result)

        # Update statistics
        elapsed = time.monotonic() - start_time
        self._stats["total_processed"] += len(batch)
        self._stats["batches_processed"] += 1
        self._stats["avg_batch_size"] = (
            self._stats["total_processed"] / self._stats["batches_processed"]
            if self._stats["batches_processed"] > 0 else 0)
        self._stats["worktime_seconds"] += elapsed
        self._log.info(f"Executing batch... Finished in {elapsed:.2f} sec.\t"
                       f"Remaining tasks: {self._task_queue.qsize()}.")

    def get_stats(self) -> dict[str, any]:
        uptime = time.monotonic() - self._start_time
        return {
            **self._stats,
            "uptime_seconds": uptime,
            "uptime_readable": f"{int(uptime//86400)}d "
                               f"{int(uptime%86400//3600)}h "
                               f"{int(uptime%3600//60)}m "
                               f"{int(uptime%60)}s",
            "in_queue": self._task_queue.qsize(),
            "active": self._is_running,
        }


async def init_matlab_daemon(app: web.Application) -> None:
    batch_size = int(os.getenv("MATLAB_DAEMON_CONCURRENCY_LIMIT", "10"))
    drain_timeout = float(os.getenv("MATLAB_DRAIN_TIMEOUT", "0.4"))

    daemon = MatlabDaemon(
        batch_size=batch_size,
        drain_timeout=drain_timeout,
        max_queue_size=1000
    )

    app["mat_daemon"] = daemon
    daemon.start()


async def cleanup_matlab_daemon(app: web.Application) -> None:
    if "mat_daemon" in app:
        await app["mat_daemon"].stop()


async def process_job(
    ws: web.WebSocketResponse,
    daemon: MatlabDaemon,
    request_id: str,
    content: dict[str, any]
) -> None:
    """Background task: runs MATLAB job and sends result when done."""
    try:
        future = await daemon.submit_request(request_id, content)
        result = await asyncio.wait_for(asyncio.shield(future), timeout=300)
        await ws.send_json({
            "type": "result",
            "request_id": request_id,
            "data": result
        })
    except asyncio.TimeoutError as e:
        await ws.send_json({
            "type": "error",
            "request_id": request_id,
            "error": str(e),
            "code": "TIMEOUT"
        })
    except Exception as e:
        await ws.send_json({
            "type": "error",
            "request_id": request_id,
            "error": str(e),
            "code": "INTERNAL_ERROR"
        })


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    daemon: MatlabDaemon = request.app["mat_daemon"]
    pending_tasks: set[asyncio.Task] = set()
    
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
                
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                await ws.send_json({"type": "error", "error": "Invalid JSON"})
                continue
            
            msg_type = data.get("type")
            request_id = data.get("request_id") or str(uuid.uuid4())
            
            if msg_type == "submit":
                payload = data.get("payload")
                if payload is None:
                    await ws.send_json({
                        "type": "error",
                        "request_id": request_id,
                        "error": "Missing payload"
                    })
                    continue
                
                # Fire off background task so we can keep receiving messages
                task = asyncio.create_task(
                    process_job(ws, daemon, request_id, payload),
                    name=f"job_{request_id}"
                )
                pending_tasks.add(task)
                task.add_done_callback(pending_tasks.discard)
                
                # Immediate acknowledgment
                await ws.send_json({
                    "type": "ack",
                    "request_id": request_id
                })
            
            elif msg_type == "stats":
                stats = daemon.get_stats()
                await ws.send_json({
                    "type": "stats",
                    "request_id": request_id,
                    "data": stats
                })
            
            else:
                await ws.send_json({
                    "type": "error",
                    "request_id": request_id,
                    "error": f"Unknown type: {msg_type}"
                })
                
    except Exception as e:
        logging.error(f"WebSocket handler error: {e}")
    finally:
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        logging.info("WebSocket closed, cleaned up %d pending tasks", len(pending_tasks))
    
    return ws

async def handle_get(request: web.Request) -> web.Response:
    daemon = request.app["mat_daemon"]
    stats = daemon.get_stats()
    return web.json_response(stats)


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", handle_get)
    app.router.add_get("/ws", handle_websocket)

    app.on_startup.append(init_matlab_daemon)
    app.on_cleanup.append(cleanup_matlab_daemon)

    return app


if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL", logging.INFO)
    log_filepath = os.getenv("LOG_FILE_PATH", "./matdaemon.log")

    file_handler = logging.FileHandler(log_filepath)
    console_handler = logging.StreamHandler()

    logging.basicConfig(level=log_level, handlers=[file_handler, console_handler])

    web.run_app(create_app(), port=int(os.getenv("MATLAB_LISTENING_PORT", 80)))
