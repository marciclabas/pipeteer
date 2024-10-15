from typing_extensions import TypeVar, Generic, AsyncIterable
from abc import abstractmethod
import asyncio
from datetime import timedelta
from pipeteer.queues import InexistentItem, Transactional

A = TypeVar('A')

class ReadQueue(Transactional, Generic[A]):
  """A read/pop-only view of a `Queue`"""

  @abstractmethod
  async def pop(self, key: str, /):
    """Delete a specific item from the queue
    Throws `ReadError`"""

  async def wait_any(self, *, reserve: timedelta | None = None, poll_interval: timedelta = timedelta(seconds=1)) -> tuple[str, A]:
    """Read any item from the queue, waiting if necessary
    - `reserve`: reservation timeout. If not acknowledged within this time, the item is visible again
    - Throws `InfraError`
    """
    while not (res := await self.read_any(reserve=reserve)):
      await asyncio.sleep(poll_interval.total_seconds())
    return res

  async def read_any(self, *, reserve: timedelta | None = None) -> tuple[str, A] | None:
    """Read any item from the queue if not empty
    - `reserve`: reservation timeout. If not acknowledged within this time, the item is visible again
    - Throws `InfraError`
    """
    async for key, val in self.items(reserve=reserve, max=1):
      return key, val

  @abstractmethod
  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> A:
    """Read a specific item from the queue
    - `reserve`: reservation timeout. If not acknowledged within this time, the item is visible again
    - Throws `ReadError`
    """
  
  async def safe_read(self, key: str, /, *, reserve: timedelta | None = None) -> A | None:
    """Read a specific item from the queue
    - `reserve`: reservation timeout. If not acknowledged within this time, the item is visible again
    - Throws `InfraError`
    """
    try:
      return await self.read(key, reserve=reserve)
    except InexistentItem:
      ...
  
  @abstractmethod
  def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, A]]:
    """Iterate over the queue's items
    - `reserve`: reservation reserve for each iterated item. If not acknowledged within this time, items will become visible again
    - `max`: maximum number of items to iterate over (and reserve)
    - Throws `InfraError`
    """
  
  async def has(self, key: str, /, *, reserve: timedelta | None = None) -> bool:
    """Check if a specific item is in the queue
    - `reserve`: reservation timeout, after which the item is visible again
    - Throws `InfraError`
    """
    return await self.safe_read(key, reserve=reserve) is not None
    
  
  async def keys(self) -> AsyncIterable[str]:
    async for key, _ in self.items(reserve=None, max=None):
      yield key
  
  async def values(self) -> AsyncIterable[A]:
    async for _, val in self.items(reserve=None, max=None):
      yield val

class WriteQueue(Transactional, Generic[A]):
  """A write-only view of a `Queue`"""
  @abstractmethod
  async def push(self, key: str, value: A):
    """Push an item into the queue
    Throws `InfraError`"""


class Queue(ReadQueue[A], WriteQueue[A], Generic[A]):
  """A key-value, point-readable queue"""

class ListQueue(Queue[list[A]]):
  @abstractmethod
  async def append(self, key: str, value: A):
    ...