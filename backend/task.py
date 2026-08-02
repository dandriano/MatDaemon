import asyncio
import hashlib
import json
import time

from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property


class MatlabTaskPriority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2
    

@dataclass(order=True)
class MatlabDaemonTask:
    priority: int
    task_id: str = field(compare=False)
    data: dict[str, any] = field(compare=False)
    future: asyncio.Future[any] = field(compare=False)
    timestamp: float = field(default_factory=time.monotonic)
    finished: float = field(default=0.0, compare=False)
    is_processing: bool = field(default=False, compare=False)

    @property
    def fname(self) -> str:
        return self.data.get("fname", "unknown")

    @cached_property
    def content_hash(self) -> str:
        compact_data = [
            self.fname,
            self.data.get("params")
        ]
        dump = json.dumps(compact_data, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(dump.encode()).hexdigest()
