import asyncio
import importlib
import sys
import threading
import types
import unittest
from unittest.mock import patch


matlab_module = types.ModuleType("matlab")
matlab_module.__path__ = []
engine_module = types.ModuleType("matlab.engine")

class MatlabDouble(list):
    def tolist(self):
        return list(self)

engine_module.MatlabEngine = object
engine_module.start_matlab = lambda: None
matlab_module.engine = engine_module
matlab_module.double = MatlabDouble
with patch.dict(sys.modules, {"matlab": matlab_module, "matlab.engine": engine_module}):
    daemon_module = importlib.import_module("backend.daemon")
    MatlabDaemon = daemon_module.MatlabDaemon
    MatlabTaskPriority = importlib.import_module("backend.task").MatlabTaskPriority


class FakeEngine:
    def __init__(self, results=None):
        self.results = results if results is not None else []
        self.quit_called = False

    def MatlabProcessorFunc(self, fnames, params):
        return self.results

    def quit(self):
        self.quit_called = True


class MatlabDaemonTests(unittest.IsolatedAsyncioTestCase):
    def make_daemon(self, *, batch_size=4, max_queue_size=10):
        daemon = MatlabDaemon(
            batch_size=batch_size,
            drain_timeout=0.01,
            max_queue_size=max_queue_size,
            script_paths=[],
        )
        daemon._is_running = True
        return daemon

    def make_task(self, daemon, task_id, *, priority=MatlabTaskPriority.NORMAL, params=None):
        return daemon._register_task(
            task_id,
            {"fname": "Example", "params": params or {}},
            priority,
        )

    async def test_pending_and_terminal_task_statuses(self):
        daemon = self.make_daemon()
        queued = self.make_task(daemon, "queued")
        processing = self.make_task(daemon, "processing")
        completed = self.make_task(daemon, "completed")
        failed = self.make_task(daemon, "failed")
        cancelled = self.make_task(daemon, "cancelled")

        processing.is_processing = True
        completed.future.set_result({"value": 3})
        failed.future.set_exception(ValueError("bad conversion"))
        cancelled.future.cancel()

        self.assertEqual(daemon.get_task_stats("queued")["status"], "queued")
        self.assertEqual(daemon.get_task_stats("processing")["status"], "processing")
        self.assertEqual(daemon.get_task_stats("completed")["status"], "completed")
        self.assertEqual(daemon.get_task_stats("failed")["status"], "failed")
        self.assertEqual(daemon.get_task_stats("cancelled")["status"], "cancelled")
        self.assertEqual(daemon.get_task_stats("missing")["status"], "unknown")

    async def test_duplicate_ids_and_queue_full_cleanup(self):
        daemon = self.make_daemon(max_queue_size=1)
        await daemon.submit_task("first", {"fname": "Example", "params": {}})

        with self.assertRaisesRegex(RuntimeError, "Duplicate task"):
            await daemon.submit_task("first", {"fname": "Example", "params": {}})

        with patch.object(daemon_module, "QUEUE_PUT_TIMEOUT", 0.01):
            with self.assertRaisesRegex(RuntimeError, "Queue is full"):
                await daemon.submit_task("rejected", {"fname": "Example", "params": {}})

        self.assertIsNone(daemon._get_task("rejected"))
        self.assertEqual(daemon._task_queue.qsize(), 1)

    async def test_completed_tasks_are_evicted_after_ttl(self):
        daemon = self.make_daemon()
        task = self.make_task(daemon, "short-lived")

        with patch.object(daemon_module, "RESULT_TTL", 0.01):
            task.future.set_result("done")
            await asyncio.sleep(0.03)

        self.assertIsNone(daemon._get_task("short-lived"))

    async def test_parameter_conversion_failure_isolated_to_task(self):
        daemon = self.make_daemon()
        bad = self.make_task(daemon, "bad", params={"invalid": True})
        good = self.make_task(daemon, "good", params={"invalid": False})
        engine = FakeEngine(results=["good result"])
        daemon._matlab_engine = engine

        def convert(params):
            if params["invalid"]:
                raise ValueError("ragged numeric list")
            return params

        with patch.object(daemon_module, "convert_to_matlab_types", side_effect=convert):
            await daemon._process_batch([bad, good])

        self.assertEqual(str(bad.future.exception()), "ragged numeric list")
        self.assertEqual(good.future.result(), "good result")

    async def test_result_conversion_failure_isolated_to_task(self):
        daemon = self.make_daemon()
        bad = self.make_task(daemon, "bad-result")
        good = self.make_task(daemon, "good-result")
        daemon._matlab_engine = FakeEngine(results=[{"bad": True}, {"good": True}])

        def convert(result):
            if result.get("bad"):
                raise ValueError("cannot convert result")
            return {"converted": True}

        with patch.object(daemon_module, "convert_from_matlab_types", side_effect=convert):
            await daemon._process_batch([bad, good])

        self.assertEqual(str(bad.future.exception()), "cannot convert result")
        self.assertEqual(good.future.result(), {"converted": True})

    async def test_one_item_list_is_accepted_for_one_task(self):
        daemon = self.make_daemon()
        task = self.make_task(daemon, "single-result")
        daemon._matlab_engine = FakeEngine(results=["single result"])

        await daemon._process_batch([task])

        self.assertEqual(task.future.result(), "single result")

    async def test_multi_item_list_maps_results_to_tasks_by_position(self):
        daemon = self.make_daemon()
        tasks = [self.make_task(daemon, f"task-{i}") for i in range(3)]
        daemon._matlab_engine = FakeEngine(results=["first", "second", "third"])

        await daemon._process_batch(tasks)

        self.assertEqual([task.future.result() for task in tasks], ["first", "second", "third"])

    async def test_scalar_response_for_one_task_fails_task(self):
        daemon = self.make_daemon()
        task = self.make_task(daemon, "scalar-result")
        daemon._matlab_engine = FakeEngine(results="single result")

        await daemon._process_batch([task])

        self.assertIsInstance(task.future.exception(), TypeError)
        self.assertIn("expected a Python list", str(task.future.exception()))

    async def test_too_few_results_fail_only_tasks_without_a_result(self):
        daemon = self.make_daemon()
        tasks = [self.make_task(daemon, f"short-{i}") for i in range(3)]
        daemon._matlab_engine = FakeEngine(results=["first", "second"])

        await daemon._process_batch(tasks)

        self.assertEqual(tasks[0].future.result(), "first")
        self.assertEqual(tasks[1].future.result(), "second")
        self.assertIsInstance(tasks[2].future.exception(), RuntimeError)
        self.assertIn("no result for task at index 2", str(tasks[2].future.exception()))

    async def test_extra_results_are_ignored_with_warning(self):
        daemon = self.make_daemon()
        tasks = [self.make_task(daemon, f"extra-{i}") for i in range(2)]
        daemon._matlab_engine = FakeEngine(results=["first", "second", "extra"])

        with self.assertLogs("MatlabDaemon", level="WARNING") as logs:
            await daemon._process_batch(tasks)

        self.assertEqual([task.future.result() for task in tasks], ["first", "second"])
        self.assertIn("ignoring extra results", logs.output[0])

    async def test_malformed_response_types_fail_all_tasks(self):
        for response in (None, {"result": "value"}, ("value",)):
            with self.subTest(response_type=type(response).__name__):
                daemon = self.make_daemon()
                task = self.make_task(daemon, f"malformed-{type(response).__name__}")
                engine = FakeEngine()
                engine.results = response
                daemon._matlab_engine = engine

                await daemon._process_batch([task])

                self.assertIsInstance(task.future.exception(), TypeError)

    async def test_result_conversion_runs_outside_event_loop_thread(self):
        daemon = self.make_daemon()
        task = self.make_task(daemon, "large-result")
        daemon._matlab_engine = FakeEngine(results=[{"value": 1}])
        event_loop_thread = threading.get_ident()
        conversion_threads = []

        def convert(result):
            conversion_threads.append(threading.get_ident())
            return result

        with patch.object(daemon_module, "convert_from_matlab_types", side_effect=convert):
            await daemon._process_batch([task])

        self.assertTrue(conversion_threads)
        self.assertNotEqual(conversion_threads[0], event_loop_thread)

    async def test_shutdown_waits_for_active_call_and_cancels_queued_task(self):
        call_started = threading.Event()
        release_call = threading.Event()

        class BlockingEngine(FakeEngine):
            def __init__(self):
                super().__init__()
                self.call_finished = False
                self.quit_after_call = False

            def MatlabProcessorFunc(self, fnames, params):
                call_started.set()
                release_call.wait(timeout=2)
                self.call_finished = True
                return ["active result"]

            def quit(self):
                self.quit_after_call = self.call_finished
                super().quit()

        daemon = self.make_daemon(batch_size=1)
        engine = BlockingEngine()
        daemon._matlab_engine = engine
        active = self.make_task(daemon, "active", priority=MatlabTaskPriority.HIGH)
        queued = self.make_task(daemon, "queued", priority=MatlabTaskPriority.LOW)
        await daemon._task_queue.put(active)
        await daemon._task_queue.put(queued)
        daemon._processing_task = asyncio.create_task(daemon._run())

        self.assertTrue(await asyncio.to_thread(call_started.wait, 1))
        stopping = asyncio.create_task(daemon.stop())
        await asyncio.sleep(0)

        self.assertFalse(stopping.done())
        self.assertFalse(engine.quit_called)
        self.assertTrue(queued.future.cancelled())

        release_call.set()
        await stopping

        self.assertEqual(active.future.result(), "active result")
        self.assertTrue(engine.quit_called)
        self.assertTrue(engine.quit_after_call)


if __name__ == "__main__":
    unittest.main()
