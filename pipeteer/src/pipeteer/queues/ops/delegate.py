from datetime import timedelta
from typing import Any, Coroutine
from typing_extensions import AsyncIterable, Callable, TypeVar, Generic
from dataclasses import dataclass
from pipeteer.queues import ReadQueue, WriteQueue, Queue

T = TypeVar('T')

@dataclass
class ReadDelegate(ReadQueue[T], Generic[T]):
  """A queue that delegates operations to its `queue` attribute"""
  queue: ReadQueue[T]

  def read(self, key: str, /, *, reserve: timedelta | None = None):
    return self.queue.read(key, reserve=reserve)
  
  def read_any(self, *, reserve: timedelta | None = None):
    return self.queue.read_any(reserve=reserve)
  
  def pop(self, key: str):
    return self.queue.pop(key)

  def wait_any(self, *, reserve: timedelta | None = None, poll_interval: timedelta = timedelta(seconds=1)):
    return self.queue.wait_any(reserve=reserve, poll_interval=poll_interval)
  
  def safe_read(self, key: str, /, *, reserve: timedelta | None = None):
    return self.queue.safe_read(key, reserve=reserve)
  
  def items(self, *, reserve: timedelta | None = None, max: int | None = None):
    return self.queue.items(reserve=reserve, max=max)
  
  def keys(self):
    return self.queue.keys()
  
  def values(self):
    return self.queue.values()
  
  def has(self, key: str, /, *, reserve: timedelta | None = None):
    return self.queue.has(key, reserve=reserve)
  
  def clear(self):
    return self.queue.clear()



@dataclass
class WriteDelegate(WriteQueue[T], Generic[T]):
  queue: WriteQueue[T]

  def push(self, key: str, value: T):
    return self.queue.push(key, value)
  
  def premap(self, mapper):
    return self.queue.premap(mapper)
  
  def premap_k(self, mapper):
    return self.queue.premap_k(mapper)
  
  def premap_kv(self, mapper):
    return self.queue.premap_kv(mapper)
  

@dataclass
class Delegate(ReadDelegate[T], WriteDelegate[T], Generic[T]):
  queue: Queue[T] # type: ignore