#!/usr/bin/env python3
import logging

from aiohttp import web
from config import CONFIG
from daemon import MatlabDaemon
from server import handle_get, handle_websocket


async def init_matlab_daemon(app: web.Application) -> None:
    daemon = MatlabDaemon(
        batch_size=CONFIG["CONCURRENCY_LIMIT"],
        drain_timeout=CONFIG["DRAIN_TIMEOUT"],
        max_queue_size=1000,
        script_paths=CONFIG["SCRIPT_PATHS"]
    )

    app["mat_daemon"] = daemon
    daemon.start()


async def cleanup_matlab_daemon(app: web.Application) -> None:
    if "mat_daemon" in app:
        await app["mat_daemon"].stop()


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", handle_get)
    app.router.add_get("/ws", handle_websocket)

    app.on_startup.append(init_matlab_daemon)
    app.on_cleanup.append(cleanup_matlab_daemon)

    return app


if __name__ == "__main__":
    file_handler = logging.FileHandler(CONFIG["LOG_FILE_PATH"])
    console_handler = logging.StreamHandler()

    logging.basicConfig(level=CONFIG["LOG_LEVEL"], handlers=[file_handler, console_handler])

    web.run_app(create_app(), port=CONFIG["PORT"])
