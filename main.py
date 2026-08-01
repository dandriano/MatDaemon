#!/usr/bin/env python3
import logging

from aiohttp import web
from config import CONFIG
from daemon import MatlabDaemon
from server import TaskRegistry, handle_statistics, handle_task_submit, handle_task_status, handle_task_events


async def init_matlab_daemon(app: web.Application) -> None:
    daemon = MatlabDaemon(
        batch_size=CONFIG["CONCURRENCY_LIMIT"],
        drain_timeout=CONFIG["DRAIN_TIMEOUT"],
        max_queue_size=1000,
        script_paths=CONFIG["SCRIPT_PATHS"]
    )

    app["mat_daemon"] = daemon
    await daemon.start()


async def cleanup_matlab_daemon(app: web.Application) -> None:
    if "mat_daemon" in app:
        await app["mat_daemon"].stop()


def create_app() -> web.Application:
    app = web.Application()
    app["task_registry"] = TaskRegistry()

    app.router.add_post("/tasks", handle_task_submit)
    app.router.add_get("/tasks/{request_id}", handle_task_status)
    app.router.add_get("/tasks/{request_id}/events", handle_task_events)
    app.router.add_get("/", handle_statistics)

    app.on_startup.append(init_matlab_daemon)
    app.on_cleanup.append(cleanup_matlab_daemon)

    return app


if __name__ == "__main__":
    file_handler = logging.FileHandler(CONFIG["LOG_FILE_PATH"])
    console_handler = logging.StreamHandler()

    logging.basicConfig(level=CONFIG["LOG_LEVEL"], handlers=[file_handler, console_handler])

    web.run_app(create_app(), port=CONFIG["PORT"])
