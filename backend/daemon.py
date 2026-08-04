import asyncio
import logging
import time

import matlab.engine
from backend.converters import convert_from_matlab_types, convert_to_matlab_types
from backend.registry import MatlabDaemonRegistry
from backend.task import MatlabDaemonTask, MatlabTaskPriority


class MatlabDaemon:
    """
    Daemon for processing tasks to matlab parfor process
    """
    def __init__(self, batch_size: int, drain_timeout: float, max_queue_size: int, script_paths: list[str]) -> None:
        self.batch_size = batch_size
        self.drain_timeout = drain_timeout
        self.max_queue_size = max_queue_size
        self.scripts = script_paths

        # Matlab control
        self._task_queue: asyncio.PriorityQueue[MatlabDaemonTask] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._registry: MatlabDaemonRegistry = MatlabDaemonRegistry()
        self._matlab_engine: matlab.engine.MatlabEngine | None = None

        # State management
        self._is_running: bool = False
        self._processing_task: asyncio.Task[None] | None = None

        self._start_time = time.monotonic()
        self._stats: dict[str, any] = {
            "total_processed": 0,
            "batches_processed": 0,
            "avg_batch_size": 0.0,
            "worktime_seconds": 0
        }

        self._log = logging.getLogger("MatlabDaemon")

    async def submit_task(
        self,
        task_id: str,
        request_data: dict[str, any],
        priority: MatlabTaskPriority = MatlabTaskPriority.NORMAL
    ) -> MatlabDaemonTask:
        if not self._is_running:
            raise RuntimeError("MATLAB daemon not running")

        try:
            task = self._registry.register(task_id, request_data, priority.value)
            await asyncio.wait_for(self._task_queue.put(task), timeout=1.0)
        except KeyError:
            raise RuntimeError("Duplicate task")
        except asyncio.TimeoutError:
            self._registry.discard(task)
            raise RuntimeError("Queue is full, please try again later")

        return task

    async def start(self) -> None:
        if self._is_running:
            self._log.warning("Daemon already running")
            return

        self._log.info("Starting MATLAB...")
        start_time = time.monotonic()
        loop = asyncio.get_running_loop()

        try:
            self._matlab_engine = await loop.run_in_executor(
                None, 
                matlab.engine.start_matlab
            )

            def configure_engine():
                for path in self.scripts:
                    self._matlab_engine.addpath(self._matlab_engine.genpath(path), nargout=0)
                return self._matlab_engine.feature("numcores")

            num_cores = await loop.run_in_executor(None, configure_engine)
            
            self._log.info(f"Starting MATLAB... Finished in {time.monotonic() - start_time:.2f}")

            self._is_running = True
            self._processing_task = asyncio.create_task(self._run())
            self._log.info(f"Batch size: {self.batch_size}\t"
                           f"Drain timeout: {self.drain_timeout}\t"
                           f"CPU count: {num_cores}")

        except Exception as e:
            self._log.error(f"Failed to start MATLAB: {e}")
            self._is_running = False
            if self._matlab_engine:
                await loop.run_in_executor(None, self._matlab_engine.quit)
            raise

    async def stop(self) -> None:
        self._log.info("Shutting down MATLAB...")
        start_time = time.monotonic()
        loop = asyncio.get_running_loop()
        self._is_running = False

        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        if self._matlab_engine:
            try:
                await loop.run_in_executor(None, self._matlab_engine.quit)
            except Exception as e:
                self._log.error(f"Error during engine quit: {e}")
            finally:
                self._matlab_engine = None

        while not self._task_queue.empty():
            task = self._task_queue.get_nowait()
            if not task.future.done():
                task.future.cancel()

        self._log.info(f"Shutting down MATLAB... Finished in {time.monotonic() - start_time:.2f}")

    async def _run(self) -> None:
        while self._is_running:
            batch: list[MatlabDaemonTask] = []
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

    async def _collect_batch(self) -> list[MatlabDaemonTask]:
        result_batch: list[MatlabDaemonTask] = []
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

                result_batch[-1].is_processing = True
            except asyncio.TimeoutError:
                break

        return result_batch

    async def _process_batch(self, batch: list[MatlabDaemonTask]) -> None:
        if not batch or not self._matlab_engine:
            return

        fnames = [t.data["fname"] for t in batch]
        params = [convert_to_matlab_types(t.data["params"]) for t in batch]

        self._log.info("Executing batch...")
        
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

        for task, result in zip(batch, raw_results):
            if not task.future.done():
                if isinstance(result, dict):
                    result = convert_from_matlab_types(result)
                task.future.set_result(result)

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

    def get_task_stats(self, task_id: str) -> dict:
        task = self._registry.get(task_id)
        if not task:
            return {
                "task_id": task_id,
                "fname": "unknown",
                "status": "unknown"
            }

        if task.future.cancelled():
            return {"task_id": task.task_id, "fname": task.fname, "status": "cancelled"}

        exc = task.future.exception()
        if exc is not None:
            return {
                "task_id": task.task_id,
                "fname": task.fname,
                "status": "failed",
                "error": str(exc),
            }

        if not task.future.done():
            return {
                "task_id": task.task_id,
                "fname": task.fname,
                "status": "processing" if task.is_processing else "queued",
            }

        return {
            "task_id": task.task_id,
            "fname": task.fname,
            "status": "completed",
            "result": task.future.result(),
        }

    async def refresh_scripts(self) -> None:
        if not self._matlab_engine:
            return
         
        loop = asyncio.get_running_loop()
        def configure_engine():
            for path in self.scripts:
                self._matlab_engine.addpath(self._matlab_engine.genpath(path), nargout=0)
         
        await loop.run_in_executor(None, configure_engine)
        self._log.info("Scripts path refreshed...")
