#!/usr/bin/env python3
import asyncio
import json
import logging
import uuid

from aiohttp import WSMsgType, web
from config import CONFIG
from daemon import MatlabDaemon


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
