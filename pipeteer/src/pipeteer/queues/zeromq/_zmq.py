from datetime import timedelta
import os
from typing_extensions import TypeVar, Generic
from dataclasses import dataclass
from pydantic import BaseModel
import zmq
from zmq.asyncio import Context
from pipeteer.queues import ReadQueue, WriteQueue, Queue, ops
from pipeteer.util import race, exp_backoff

T = TypeVar('T')

class Message(BaseModel, Generic[T]):
  key: str
  value: T

@dataclass
class SubZMQ(Generic[T]):
  topic: str
  type: type[T]
  url: str = 'tcp://localhost:5556'

  def __post_init__(self):
    self.pid = os.getpid()
    ctx = Context.instance()
    self.sub = ctx.socket(zmq.SUB)
    self.sub.connect(self.url)
    self.sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)
  
  async def wait(self) -> tuple[str, T]:
    if os.getpid() != self.pid:
      raise RuntimeError('ReadZQueue is not fork-safe')
    while True:
      msg = await self.sub.recv_string()
      topic, ser_msg = msg.split(' ', 1)
      if topic == self.topic: # could be a prefix but not equal
        msg = Message[self.type].model_validate_json(ser_msg)
        return msg.key, msg.value
  

@dataclass
class ReadZQueue(ops.ReadDelegate[T], Generic[T]):
  """ReadQueue with ZeroMQ realtime notifications"""
  queue: ReadQueue[T]
  sub: SubZMQ[T]

  @classmethod
  def of(cls, queue: ReadQueue[T], topic: str, type: type[T], *, url: str = 'tcp://localhost:5556'):
    return cls(queue, SubZMQ(topic, type, url))

  async def wait_any(self, *, reserve: timedelta | None = None, poll_interval: timedelta = timedelta(seconds=1)):
    if (pair := await self.queue.read_any(reserve=reserve)):
      return pair
    while True:
      _, (key, val) = await race([
        self.sub.wait(),
        exp_backoff(self.queue.read_any, t0=poll_interval.total_seconds(), base=2)
      ])
      if await self.queue.has(key, reserve=reserve):
        return key, val
      


@dataclass
class PubZMQ(Generic[T]):
  topic: str
  type: type[T]
  url: str = 'tcp://localhost:5555'

  @classmethod
  def of(cls, topic: str, type: 'type[T]', *, url: str = 'tcp://localhost:5555'):
    return cls(topic, type, url)
  
  def __post_init__(self):
    self.pid = os.getpid()
    ctx = Context.instance()
    self.pub = ctx.socket(zmq.PUB)
    self.pub.connect(self.url)
  
  async def send(self, key: str, value: T):
    if os.getpid() != self.pid:
      raise RuntimeError('PubZMQ is not fork-safe')
    msg = Message[self.type](key=key, value=value).model_dump_json()
    # print(f'Sending {self.topic}: {msg} to {self.url}')
    self.pub.send_string(f'{self.topic} {msg}')


@dataclass
class WriteMQueue(ops.WriteDelegate[T], Generic[T]):
  """WriteQueue with ZeroMQ realtime notifications"""
  queue: WriteQueue[T]
  pub: PubZMQ[T]

  @classmethod
  def of(cls, queue: WriteQueue[T], topic: str, type: type[T], *, url: str = 'tcp://localhost:5555'):
    return cls(queue, PubZMQ(topic, type, url))

  async def push(self, key: str, value: T):
    await self.queue.push(key, value)
    await self.pub.send(key, value)


@dataclass
class ZeroMQueue(Queue[T], ReadZQueue[T], WriteMQueue[T], Generic[T]):
  """Queue with ZeroMQ realtime notifications"""
  queue: Queue[T] # type: ignore

  @classmethod
  def of( # type: ignore
    cls, queue: Queue[T], topic: str, type: type[T], *,
    pub_url: str = 'tcp://localhost:5555', sub_url: str = 'tcp://localhost:5556'
  ):
    return cls(queue, PubZMQ(topic, type, pub_url), SubZMQ(topic, type, sub_url))