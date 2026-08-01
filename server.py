import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field

from aiohttp import web
from daemon import MatlabDaemon, TaskPriority

RESULT_TTL = 300


@dataclass
class TaskHandle:
    request_id: str
    future: asyncio.Future
    fname: str
    created_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None


class TaskRegistry:
    """
    Bridges HTTP request/response boundaries with the daemon's per-task
    Future.
    """

    def __init__(self) -> None:
        self._handles: dict[str, TaskHandle] = {}

    def register(self, request_id: str, future: asyncio.Future, fname: str) -> TaskHandle:
        handle = TaskHandle(request_id=request_id, future=future, fname=fname)
        self._handles[request_id] = handle
        future.add_done_callback(lambda _: self._schedule_eviction(request_id))
        return handle

    def get(self, request_id: str) -> TaskHandle | None:
        return self._handles.get(request_id)

    def _schedule_eviction(self, request_id: str) -> None:
        handle = self._handles.get(request_id)
        if handle is None:
            return
        handle.finished_at = time.monotonic()
        asyncio.get_event_loop().call_later(RESULT_TTL, self._handles.pop, request_id, None)


def serialize_status(daemon: MatlabDaemon, handle: TaskHandle) -> dict:
    if not handle.future.done():
        return {
            "request_id": handle.request_id,
            "fname": handle.fname,
            "status": daemon.get_task_state(handle.request_id),
        }

    if handle.future.cancelled():
        return {"request_id": handle.request_id, "fname": handle.fname, "status": "cancelled"}

    exc = handle.future.exception()
    if exc is not None:
        return {
            "request_id": handle.request_id,
            "fname": handle.fname,
            "status": "failed",
            "error": str(exc),
        }

    return {
        "request_id": handle.request_id,
        "fname": handle.fname,
        "status": "completed",
        "result": handle.future.result(),
    }


async def handle_task_submit(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        daemon: MatlabDaemon = request.app["mat_daemon"]
        registry: TaskRegistry = request.app["task_registry"]

        fname = data.get("fname")
        if not fname:
            return web.json_response({"error": "'fname' is required"}, status=400)
        params = data.get("params", {})
        priority = TaskPriority[data.get("priority", "NORMAL").upper()]

        request_id = str(uuid.uuid4())
        future = await daemon.submit_request(
            request_id=request_id,
            request_data={"fname": fname, "params": params},
            priority=priority
        )
        registry.register(request_id, future, fname)

        return web.json_response(
            {"request_id": request_id, "status_url": f"/tasks/{request_id}"},
            status=202
        )

    except KeyError as e:
        return web.json_response({"error": f"invalid priority: {e}"}, status=400)
    except RuntimeError as e:
        return web.json_response({"error": str(e)}, status=503)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=400)


async def handle_task_status(request: web.Request) -> web.Response:
    request_id = request.match_info["request_id"]
    registry: TaskRegistry = request.app["task_registry"]
    daemon: MatlabDaemon = request.app["mat_daemon"]

    handle = registry.get(request_id)
    if handle is None:
        return web.json_response({"error": "unknown request_id"}, status=404)

    return web.json_response(serialize_status(daemon, handle))


async def handle_task_events(request: web.Request) -> web.StreamResponse:
    """
    Optional SSE stream, which pushes status transitions and closes on completion.
    """
    request_id = request.match_info["request_id"]
    registry: TaskRegistry = request.app["task_registry"]
    daemon: MatlabDaemon = request.app["mat_daemon"]

    handle = registry.get(request_id)
    if handle is None:
        return web.json_response({"error": "unknown request_id"}, status=404)

    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )
    await response.prepare(request)

    async def send(event: dict) -> None:
        await response.write(f"data: {json.dumps(event)}\n\n".encode())

    last_state = None
    try:
        while not handle.future.done():
            state = daemon.get_task_state(request_id)
            if state != last_state:
                await send({"status": state})
                last_state = state
            try:
                await asyncio.wait_for(asyncio.shield(handle.future), timeout=2.0)
            except asyncio.TimeoutError:
                await response.write(b": heartbeat\n\n")

        await send(serialize_status(daemon, handle))
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        await response.write_eof()

    return response


async def handle_statistics(request: web.Request) -> web.Response:
    daemon: MatlabDaemon = request.app["mat_daemon"]
    return web.json_response(daemon.get_stats())
