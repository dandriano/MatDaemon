import asyncio
import time

from backend.task import MatlabDaemonTask, MatlabTaskPriority

RESULT_TTL = 300

class MatlabDaemonRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, MatlabDaemonTask] = {}

    def register(self, task_id: str, data: any, priority: MatlabTaskPriority) -> MatlabDaemonTask:
        if (task_id in self._tasks):
            raise KeyError

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self._tasks[task_id] = MatlabDaemonTask(task_id=task_id, data=data, future=future, priority=priority)
        future.add_done_callback(lambda _: self._schedule_eviction(task_id))
        return self._tasks[task_id]

    def get(self, task_id: str) -> MatlabDaemonTask | None:
        return self._tasks.get(task_id)

    def discard(self, task: MatlabDaemonTask):
        self._tasks.pop(task.task_id)

    def _schedule_eviction(self, task_id: str) -> None:
        task = self.get(task_id)
        if task is None:
            return
        task.finished = time.monotonic()
        
        def evict():
            self._tasks.pop(task_id, None)

        asyncio.get_running_loop().call_later(RESULT_TTL, evict)
