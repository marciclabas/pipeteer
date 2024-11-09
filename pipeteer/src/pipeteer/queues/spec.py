from typing_extensions import TypeVar, Generic, AsyncIterable, Callable
from abc import abstractmethod
import asyncio
from datetime import timedelta
from pipeteer.queues import InexistentItem, Transactional, ops

A = TypeVar('A', covariant=True)
B = TypeVar('B', contravariant=True)
C = TypeVar('C')

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
  async def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, A]]:
    """Iterate over the queue's items
    - `reserve`: reservation reserve for each iterated item. If not acknowledged within this time, items will become visible again
    - `max`: maximum number of items to iterate over (and reserve)
    - Throws `InfraError`
    """
    async for key in self.keys():
      yield key, await self.read(key, reserve=reserve)
  
  async def has(self, key: str, /, *, reserve: timedelta | None = None) -> bool:
    """Check if a specific item is in the queue
    - `reserve`: reservation timeout, after which the item is visible again
    - Throws `InfraError`
    """
    return await self.safe_read(key, reserve=reserve) is not None
  
  @abstractmethod
  async def clear(self):
    """Delete all items
    - Throws `InfraError`"""
    
  
  async def keys(self) -> AsyncIterable[str]:
    async for key, _ in self.items(reserve=None, max=None):
      yield key
  
  async def values(self) -> AsyncIterable[A]:
    async for _, val in self.items(reserve=None, max=None):
      yield val

class WriteQueue(Transactional, Generic[B]):
  """A write-only view of a `Queue`"""

  @abstractmethod
  async def push(self, key: str, value: B, /):
    """Push an item into the queue
    Throws `InfraError`"""

  def premap(self, mapper: Callable[[C], B]) -> 'WriteQueue[C]':
    """Map the key-value pair before pushing"""
    async def f(k: str, v: C):
      return k, mapper(v)
    return ops.premap(self, f)
  
  def premap_k(self, mapper: Callable[[str], str]) -> 'WriteQueue[B]':
    """Map the key-value pair before pushing"""
    async def f(k: str, v: B):
      return mapper(k), v
    return ops.premap(self, f)

  def premap_kv(self, mapper: Callable[[str, C], tuple[str, B]]) -> 'WriteQueue[C]':
    """Map the key-value pair before pushing"""
    async def f(k: str, v: C):
      return mapper(k, v)
    return ops.premap(self, f)
  
class Queue(ReadQueue[C], WriteQueue[C], Generic[C]):
  """A key-value, point-readable queue"""

  @staticmethod
  def of(url: str, type: type[A]) -> 'Queue[A]':
    from pipeteer.queues import parse
    return parse(url, type)

  @classmethod
  def sink(cls):
    return Sink()

class ListQueue(Queue[list[C]], Generic[C]):
  @abstractmethod
  async def append(self, key: str, value: C, /):
    ...


class Sink(Queue[C], Generic[C]):
  async def push(self, key: str, value: C):
    ...

  async def pop(self, key: str):
    ...

  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> C:
    raise InexistentItem(key)
  
  async def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, C]]:
    if False:
      yield

  async def clear(self):
    ...