from typing_extensions import TypeVar, Generic, Callable, Awaitable
import asyncio
from dataclasses import dataclass
from pipeteer import WriteQueue

A = TypeVar('A')
B = TypeVar('B')

class tee(WriteQueue[A], Generic[A]):
  """A queue that pushes to the multiple queues at once"""

  def __init__(self, q1: WriteQueue[A], q2: WriteQueue[A], /, *qs: WriteQueue[A], ordered: bool = False):
    self.queues: list[WriteQueue[A]] = [q1, q2, *qs]
    self.ordered = ordered

  def __repr__(self) -> str:
    reprs = ', '.join(repr(q) for q in self.queues)
    return f'tee({reprs})'
  
  async def push(self, key: str, value: A):
    if self.ordered:
      for q in self.queues:
        await q.push(key, value)
    else:
      await asyncio.gather(*[q.push(key, value) for q in self.queues])


@dataclass
class prefilter(WriteQueue[A], Generic[A]):
  queue: WriteQueue[A]
  pred: Callable[[tuple[str, A]], Awaitable[bool]]

  def __repr__(self):
    return f'prefilter({self.queue!r})'

  async def push(self, key: str, value: A):
    if await self.pred((key, value)):
      return await self.queue.push(key, value)

@dataclass
class premap(WriteQueue[B], Generic[A, B]):

  queue: WriteQueue[A]
  mapper: Callable[[str, B], Awaitable[tuple[str, A]]]

  def __repr__(self):
    return f'premap({self.queue!r})'

  async def push(self, key: str, value: B):
    k, v = await self.mapper(key, value)
    return await self.queue.push(k, v)