from typing_extensions import TypeVar, Generic, get_args
from dataclasses import dataclass
from pipeteer.queues import WriteQueue, ListQueue

A = TypeVar('A')

@dataclass
class singleton(WriteQueue[A], Generic[A]):
  queue: ListQueue[A]

  async def push(self, key: str, val: A):
    await self.queue.push(key, [val])

@dataclass
class appender(WriteQueue[A], Generic[A]):
  queue: ListQueue[A]

  async def push(self, key: str, val: A):
    await self.queue.append(key, val)