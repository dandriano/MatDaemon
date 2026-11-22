import uuid
from pathlib import Path
from aiohttp import web
from backend.daemon import MatlabDaemon, MatlabTaskPriority
from backend.validator import MatlabFunctionValidator
 
 
async def handle_script_upload(request: web.Request) -> web.Response:
    """Handle uploading a new MATLAB script"""
    daemon: MatlabDaemon = request.app["mat_daemon"]
    

    raw_path = daemon.scripts[0]
    target_dir = Path(raw_path)
    if not target_dir.is_absolute():
        # Resolve relative to the current working directory (/home/matlab)
        target_dir = Path.cwd() / raw_path
    
    reader = await request.multipart()
    field = await reader.next()
    
    if not field or field.name != "file":
        return web.json_response({"error": "Expected 'file' field"}, status=400)
    
    filename = field.filename
    if not filename.endswith(".m"):
        return web.json_response({"error": "Only .m files are allowed"}, status=400)
    
    filepath = target_dir / filename

    # TODO: refactor routine below, temp is redundant

    # Write to temp file first for validation
    temp_path = target_dir / f"temp_{filename}"
    with open(temp_path, "wb") as f:
        while True:
            chunk = await field.read_chunk()
            if not chunk:
                break
            f.write(chunk)
            
    # Validate
    is_valid, message = MatlabFunctionValidator.validate(temp_path)
    if not is_valid:
        temp_path.unlink()
        return web.json_response({"error": f"Validation failed: {message}"}, status=400)
    
    # Move to final location
    if filepath.exists():
        filepath.unlink()
    temp_path.rename(filepath)
    
    # Refresh daemon scripts
    await daemon.refresh_scripts()
    
    return web.json_response({
        "message": "Script uploaded and registered",
        "fname": filepath.stem
    })


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
