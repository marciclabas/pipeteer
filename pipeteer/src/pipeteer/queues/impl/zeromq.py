from typing_extensions import TypeVar, Generic, Iterable, Coroutine, Any
import asyncio
from functools import cached_property
from dataclasses import dataclass, field
from datetime import timedelta
import zmq
from zmq.asyncio import Context
from pipeteer import Queue, WriteQueue, ReadQueue

T = TypeVar('T')

async def race(coros: Iterable[Coroutine[Any, Any, T]]) -> T:
  tasks = [asyncio.create_task(c) for c in coros]
  done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
  for task in pending:
      task.cancel()
  return done.pop().result()

@dataclass
class WriteZeroMQueue(WriteQueue[T], Generic[T]):
  queue: WriteQueue[T]
  topic: str
  url: str = 'tcp://*:5555'
  ctx: Context = field(default_factory=Context)

  @cached_property
  def pub(self):
    sock = self.ctx.socket(zmq.PUB)
    sock.connect(self.url)
    return sock

  async def push(self, key: str, value: T):
    res = await self.queue.push(key, value)
    await self.pub.send_string(self.topic)
    return res
  
@dataclass
class ReadZeroMQueue(ReadQueue[T], Generic[T]):
  queue: ReadQueue[T]
  topic: str
  url: str = 'tcp://localhost:5555'
  ctx: Context = field(default_factory=Context)

  @cached_property
  def sub(self):
    sock = self.ctx.socket(zmq.SUB)
    sock.connect(self.url)
    sock.setsockopt_string(zmq.SUBSCRIBE, self.topic)
    return sock

  async def wait_zmq(self):
    await self.sub.recv_string()

  async def exp_backoff(self, *, reserve: timedelta | None = None, poll_interval: timedelta = timedelta(seconds=1)) -> tuple[str, T]:
    t0 = poll_interval.total_seconds()
    exp = 1
    while True:
      result = await self.queue.wait_any(reserve=reserve)
      if result:
        return result
      
      await asyncio.sleep(t0 * exp)
      exp *= 2


  async def wait_any(self, *, reserve: timedelta | None = None, poll_interval: timedelta = timedelta(seconds=1)) -> tuple[str, T]:
    while True:
      first = await race([self.exp_backoff(reserve=reserve, poll_interval=poll_interval), self.wait_zmq()])
      if first:
        return first
      await self.wait_zmq()
      
      result = await self.queue.wait_any(reserve=reserve)
      if result:
        return result
      
  def pop(self, key: str):
    return self.queue.pop(key)
  
  def read(self, key: str, /, *, reserve: timedelta | None = None):
    return self.queue.read(key, reserve=reserve)
  
  def read_any(self, *, reserve: timedelta | None = None):
    return self.queue.read_any(reserve=reserve)
  
  def items(self, *, reserve: timedelta | None = None, max: int | None = None):
    return self.queue.items(reserve=reserve, max=max)
  
  def keys(self):
    return self.queue.keys()
  
  def has(self, key: str, /, *, reserve: timedelta | None = None):
    return self.queue.has(key, reserve=reserve)
  
  def values(self):
    return self.queue.values()
        
@dataclass
class ZeroMQueue(Queue[T], ReadZeroMQueue[T], WriteZeroMQueue[T], Generic[T]):
  queue: Queue[T] # type: ignore