from typing_extensions import TypeVar
from dataclasses import dataclass
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from pipeteer.queues import Queue, ListQueue, SqlQueue, ListSqlQueue
from pipeteer.backend import Backend

A = TypeVar('A')

@dataclass
class SqlBackend(Backend):
  engine: AsyncEngine

  def queue(self, id: str, type: type[A]) -> Queue[A]:
    queue = SqlQueue(type, self.engine, table=id)
    asyncio.create_task(queue.initialize())
    return queue
  
  def list_queue(self, id: str, type: type[A]) -> ListQueue[A]:
    return ListSqlQueue(list[type], self.engine, table=id)