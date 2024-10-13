from typing import Sequence, TypeVar, Self
from dataclasses import dataclass
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from pipeteer.backend import Backend
from pipeteer.queues import ListQueue, Queue, SqlQueue, ListSqlQueue

A = TypeVar('A')

@dataclass
class SqlBackend(Backend):
  engine: AsyncEngine

  @classmethod
  def at(cls, path: str) -> 'Self':
    return cls(create_async_engine(f'sqlite+aiosqlite:///{path}'))

  @staticmethod
  def table(path: Sequence[str]) -> str:
    return '-'.join(path) or 'root'

  def queue(self, path: Sequence[str], type: type[A]) -> Queue[A]:
    return SqlQueue(type, self.engine, table=self.table(path))
  
  def list_queue(self, path: Sequence[str], type: type[A]) -> ListQueue[A]:
    return ListSqlQueue(list[type], self.engine, table=self.table(path))
  
  def output(self, type: type[A]) -> Queue[A]:
    return self.queue(('output',), type)