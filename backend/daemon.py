from __future__ import annotations

import asyncio
import logging
import time

import matlab.engine
from backend.converter import convert_from_matlab_types, convert_to_matlab_types
from backend.task import MatlabDaemonTask, MatlabTaskPriority

RESULT_TTL = 300
QUEUE_PUT_TIMEOUT = 1.0


def _convert_batch_results(raw_results: object, expected_count: int) -> list[tuple[bool, object]]:
    """Convert one engine result per task, keeping conversion failures isolated."""
    if not isinstance(raw_results, list):
        raise TypeError(
            "MATLAB returned malformed batch: expected a Python list, "
            f"got {type(raw_results).__name__}"
        )

    if len(raw_results) > expected_count:
        logging.getLogger("MatlabDaemon").warning(
            "MATLAB returned %d results for %d tasks; ignoring extra results",
            len(raw_results),
            expected_count,
        )

    converted: list[tuple[bool, object]] = []
    for index in range(expected_count):
        if index >= len(raw_results):
            converted.append((
                False,
                RuntimeError(
                    f"MATLAB returned no result for task at index {index} "
                    f"({len(raw_results)} results for {expected_count} tasks)"
                ),
            ))
            continue

        result = raw_results[index]
        try:
            if isinstance(result, dict):
                result = convert_from_matlab_types(result)
            converted.append((True, result))
        except Exception as err:
            converted.append((False, err))
    return converted


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
        self._work_queue: asyncio.PriorityQueue[MatlabDaemonTask] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._registry: dict[str, MatlabDaemonTask] = {}
        self._matlab_engine: matlab.engine.MatlabEngine | None = None

        # State management
        self._is_running: bool = False
        self._processing_task: asyncio.Task[None] | None = None
        self._inflight_operation: asyncio.Future | None = None
        self._stop_event = asyncio.Event()

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

        task: MatlabDaemonTask | None = None
        try:
            task = self._register_task(task_id, request_data, priority)
            await asyncio.wait_for(self._work_queue.put(task), timeout=QUEUE_PUT_TIMEOUT)
        except KeyError:
            raise RuntimeError("Duplicate task")
        except asyncio.TimeoutError:
            if task is not None:
                self._discard_task(task)
                task.future.cancel()
            raise RuntimeError("Queue is full, please try again later")

        return task

    def _register_task(
        self,
        task_id: str,
        request_data: dict[str, any],
        priority: MatlabTaskPriority,
    ) -> MatlabDaemonTask:
        if task_id in self._registry:
            raise KeyError(task_id)

        future = asyncio.get_running_loop().create_future()
        task = MatlabDaemonTask(
            priority=priority.value,
            task_id=task_id,
            data=request_data,
            future=future,
        )
        self._registry[task_id] = task
        future.add_done_callback(lambda _: self._schedule_task_eviction(task))
        return task

    def _get_task(self, task_id: str) -> MatlabDaemonTask | None:
        return self._registry.get(task_id)

    def _discard_task(self, task: MatlabDaemonTask) -> None:
        if self._registry.get(task.task_id) is task:
            self._registry.pop(task.task_id)

    def _schedule_task_eviction(self, task: MatlabDaemonTask) -> None:
        if self._registry.get(task.task_id) is not task:
            return
        task.finished = time.monotonic()
        asyncio.get_running_loop().call_later(
            RESULT_TTL, self._evict_task, task.task_id, task
        )

    def _evict_task(self, task_id: str, task: MatlabDaemonTask) -> None:
        if self._registry.get(task_id) is task:
            self._registry.pop(task_id)

    def _cancel_queued_tasks(self) -> None:
        while not self._work_queue.empty():
            task = self._work_queue.get_nowait()
            if not task.future.done():
                task.future.cancel()

    async def start(self) -> None:
        if self._is_running:
            self._log.warning("Daemon already running")
            return

        self._log.info("Starting MATLAB...")
        start_time = time.monotonic()
        loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

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
        self._stop_event.set()

        # Tasks still waiting for a batch are cancelled. The worker is allowed to
        # finish its selected batch before the engine is shut down.
        self._cancel_queued_tasks()

        if self._processing_task:
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
            finally:
                self._processing_task = None

        # Keep engine teardown ordered after the executor call even if the worker
        # was cancelled outside the normal stop path.
        if self._inflight_operation is not None:
            operation = self._inflight_operation
            try:
                await asyncio.shield(operation)
            except Exception as e:
                self._log.error(f"Error while waiting for MATLAB call: {e}")
            finally:
                if operation.done() and self._inflight_operation is operation:
                    self._inflight_operation = None

        if self._matlab_engine:
            try:
                await loop.run_in_executor(None, self._matlab_engine.quit)
            except Exception as e:
                self._log.error(f"Error during engine quit: {e}")
            finally:
                self._matlab_engine = None

        self._log.info(f"Shutting down MATLAB... Finished in {time.monotonic() - start_time:.2f}")

    async def _run(self) -> None:
        while True:
            batch: list[MatlabDaemonTask] = []
            if self._stop_event.is_set():
                break
            self._log.info("Waiting for tasks...")
            try:
                batch = await self._collect_batch()
                if batch:
                    await self._process_batch(batch)
                if self._stop_event.is_set():
                    break
            except asyncio.CancelledError:
                self._is_running = False
                self._stop_event.set()
                self._cancel_queued_tasks()
                if batch:
                    for task in batch:
                        if not task.future.done():
                            task.future.cancel()
                break
            except Exception as e:
                for task in batch:
                    if not task.future.done():
                        task.future.set_exception(e)
                self._log.error(f"Error: {e}", exc_info=True)
                if not self._stop_event.is_set():
                    await asyncio.sleep(1)

    async def _next_task_or_stop(self, timeout: float | None) -> MatlabDaemonTask | None:
        queue_get = asyncio.create_task(self._work_queue.get())
        stop_wait = asyncio.create_task(self._stop_event.wait())
        waiters = (queue_get, stop_wait)

        try:
            await asyncio.wait(
                waiters,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            if queue_get.done() and not queue_get.cancelled():
                queued_task = queue_get.result()
                if not queued_task.future.done():
                    queued_task.future.cancel()
            raise

        stopped = self._stop_event.is_set()
        for waiter in waiters:
            if not waiter.done():
                waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)

        if stopped:
            if queue_get.done() and not queue_get.cancelled():
                queued_task = queue_get.result()
                if not queued_task.future.done():
                    queued_task.future.cancel()
            return None
        if queue_get.done() and not queue_get.cancelled():
            return queue_get.result()
        return None

    async def _collect_batch(self) -> list[MatlabDaemonTask]:
        result_batch: list[MatlabDaemonTask] = []
        while len(result_batch) < self.batch_size:
            # 1) First attempt to retrieve from a potentially empty queue is blocking,
            #    to avoid wasting resources when there are no tasks.
            # 2) Upon receiving a task from the queue, the timeout is updated (sliding timeout)
            #    to wait for additional tasks within that timeframe.
            timeout = self.drain_timeout if len(result_batch) > 0 else None
            task = await self._next_task_or_stop(timeout)
            if task is None:
                break

            task.is_processing = True
            result_batch.append(task)

        return result_batch

    async def _process_batch(self, batch: list[MatlabDaemonTask]) -> None:
        if not batch:
            return

        engine = self._matlab_engine
        if not engine:
            err = RuntimeError("MATLAB engine is not available")
            for task in batch:
                if not task.future.done():
                    task.future.set_exception(err)
            return

        processable: list[MatlabDaemonTask] = []
        fnames: list[str] = []
        params: list[dict[str, any]] = []
        for task in batch:
            try:
                converted_params = convert_to_matlab_types(task.data["params"])
            except Exception as err:
                if not task.future.done():
                    task.future.set_exception(err)
                continue

            processable.append(task)
            fnames.append(task.data["fname"])
            params.append(converted_params)

        if not processable:
            return

        self._log.info("Executing batch...")
        start_time = time.monotonic()
        operation = asyncio.get_running_loop().run_in_executor(
            None, engine.MatlabProcessorFunc, fnames, params
        )
        self._inflight_operation = operation
        try:
            raw_results = await asyncio.shield(operation)
        except asyncio.CancelledError:
            # Cancelling the coroutine cannot stop the executor thread. Wait for
            # that call to leave MATLAB before allowing the worker to finish.
            try:
                await asyncio.shield(operation)
            except Exception:
                pass
            raise
        except Exception as err:
            for task in processable:
                if not task.future.done():
                    task.future.set_exception(err)
            return
        finally:
            if operation.done() and self._inflight_operation is operation:
                self._inflight_operation = None

        try:
            converted_results = await asyncio.get_running_loop().run_in_executor(
                None, _convert_batch_results, raw_results, len(processable)
            )
        except Exception as err:
            for task in processable:
                if not task.future.done():
                    task.future.set_exception(err)
            return

        for task, (success, result) in zip(processable, converted_results):
            if not task.future.done():
                if success:
                    task.future.set_result(result)
                else:
                    task.future.set_exception(result)

        elapsed = time.monotonic() - start_time
        self._stats["total_processed"] += len(processable)
        self._stats["batches_processed"] += 1
        self._stats["avg_batch_size"] = (
            self._stats["total_processed"] / self._stats["batches_processed"]
            if self._stats["batches_processed"] > 0 else 0)
        self._stats["worktime_seconds"] += elapsed

        self._log.info(f"Executing batch... Finished in {elapsed:.2f} sec.\t"
                       f"Remaining tasks: {self._work_queue.qsize()}.")

    def get_stats(self) -> dict[str, any]:
        uptime = time.monotonic() - self._start_time
        return {
            **self._stats,
            "uptime_seconds": uptime,
            "uptime_readable": f"{int(uptime//86400)}d "
                               f"{int(uptime%86400//3600)}h "
                               f"{int(uptime%3600//60)}m "
                               f"{int(uptime%60)}s",
            "in_queue": self._work_queue.qsize(),
            "active": self._is_running,
        }

    def get_task_stats(self, task_id: str) -> dict:
        task = self._get_task(task_id)
        if not task:
            return {
                "task_id": task_id,
                "fname": "unknown",
                "status": "unknown"
            }

        if task.future.cancelled():
            return {"task_id": task.task_id, "fname": task.fname, "status": "cancelled"}

        if not task.future.done():
            return {
                "task_id": task.task_id,
                "fname": task.fname,
                "status": "processing" if task.is_processing else "queued",
            }

        exc = task.future.exception()
        if exc is not None:
            return {
                "task_id": task.task_id,
                "fname": task.fname,
                "status": "failed",
                "error": str(exc),
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
