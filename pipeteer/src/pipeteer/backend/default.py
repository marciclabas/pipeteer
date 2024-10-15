from typing import Sequence, TypeVar
from dataclasses import dataclass, field
from functools import cached_property
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
import zmq
from zmq.asyncio import Context
from pipeteer.backend import Backend
from pipeteer.queues import ListQueue, Queue, SqlQueue, ListSqlQueue, ZeroMQueue

A = TypeVar('A')

@dataclass
class DefaultBackend(Backend):
  engine: AsyncEngine
  ctx: Context = field(default_factory=Context)
  zmq_port: int = 5555

  @cached_property
  def pub_sock(self):
    sock = self.ctx.socket(zmq.PUB)
    sock.bind(f'tcp://*:{self.zmq_port}')
    return sock
  
  @cached_property
  def sub_sock(self):
    sock = self.ctx.socket(zmq.SUB)
    sock.connect(f'tcp://localhost:{self.zmq_port}')
    sock.setsockopt_string(zmq.SUBSCRIBE, '')
    return sock

  @classmethod
  def sqlite(cls, path: str):
    return cls(create_async_engine(f'sqlite+aiosqlite:///{path}'))
  
  @classmethod
  def sql(cls, url: str):
    return cls(create_async_engine(url))

  @staticmethod
  def key(path: Sequence[str]) -> str:
    return '-'.join(path) or 'root'

  def queue(self, path: Sequence[str], type: type[A]) -> Queue[A]:
    key = self.key(path)
    queue = SqlQueue(type, self.engine, table=key)
    return ZeroMQueue(queue, topic=key)
    
  def list_queue(self, path: Sequence[str], type: type[A]) -> ListQueue[A]:
    return ListSqlQueue(list[type], self.engine, table=self.key(path))
  
  def output(self, type: type[A]) -> Queue[A]:
    return self.queue(('output',), type)