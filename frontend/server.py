import uuid

from aiohttp import web
from backend.daemon import MatlabDaemon, MatlabTaskPriority


async def handle_task_submit(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        daemon: MatlabDaemon = request.app["mat_daemon"]

        fname = data.get("fname")
        if not fname:
            return web.json_response({"error": "'fname' is required"}, status=400)
        params = data.get("params", {})
        priority = MatlabTaskPriority[data.get("priority", "NORMAL").upper()]

        task_id = str(uuid.uuid4())
        await daemon.submit_task(
            task_id=task_id,
            request_data={"fname": fname, "params": params},
            priority=priority
        )

        return web.json_response(
            {"task_id": task_id, "status_url": f"/tasks/{task_id}"},
            status=202
        )

    except KeyError as e:
        return web.json_response({"error": f"invalid priority: {e}"}, status=400)
    except RuntimeError as e:
        return web.json_response({"error": str(e)}, status=503)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=400)


async def handle_task_status(request: web.Request) -> web.Response:
    task_id = request.match_info["task_id"]
    daemon: MatlabDaemon = request.app["mat_daemon"]

    return web.json_response(daemon.get_task_stats(task_id))


async def handle_statistics(request: web.Request) -> web.Response:
    daemon: MatlabDaemon = request.app["mat_daemon"]
    return web.json_response(daemon.get_stats())
