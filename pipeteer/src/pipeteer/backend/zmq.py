from typing_extensions import TypeVar
from dataclasses import dataclass
from multiprocessing import Process
from pipeteer.queues import Queue, ListQueue, ZeroMQueue, SqlQueue, ListSqlQueue
from pipeteer.backend.sql import SqlBackend
import asyncio

A = TypeVar('A')

@dataclass
class ZmqBackend(SqlBackend):

  def queue(self, id: str, type: type[A]) -> Queue[A]:
    queue = SqlQueue(type, self.engine, table=id)
    return ZeroMQueue.of(queue, topic=id, type=type)
  
  def list_queue(self, id: str, type: type[A]) -> ListQueue[A]:
    return ListSqlQueue(list[type], self.engine, table=id)
  
  def run(self):
    from pipeteer.queues.zeromq import proxy
    def _run():
      asyncio.run(proxy())
    return Process(target=_run)