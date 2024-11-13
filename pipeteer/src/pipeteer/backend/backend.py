from typing_extensions import TypeVar
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pipeteer.queues import Queue, ListQueue

A = TypeVar('A')
B = TypeVar('B')


class Backend(ABC):
  """Backend to create queues"""

  @abstractmethod
  def queue(self, id: str, type: type[A], /) -> Queue[A]:
    ...

  @abstractmethod
  def public_queue(self, id: str, type: type[A], /) -> tuple[str, Queue[A]]:
    ...

  @abstractmethod
  def list_queue(self, id: str, type: type[A], /) -> ListQueue[A]:
    ...

  @abstractmethod
  def queue_at(self, url: str, type: type[A], /) -> Queue[A]:
    ...

  @staticmethod
  def local_sql(url: str):
    from sqlalchemy.ext.asyncio import create_async_engine
    from pipeteer.backend import ZmqBackend
    @dataclass
    class DefaultSqlBackend(LocalBackend, ZmqBackend):
      ...
    return DefaultSqlBackend(id=url, engine=create_async_engine(url))
  
  @staticmethod
  def local_sqlite(path: str):
    return Backend.local_sql(f'sqlite+aiosqlite:///{path}')
  
  @staticmethod
  def sql(*, url: str, sql_url: str, secret: str | None = None):
    from sqlalchemy.ext.asyncio import create_async_engine
    from pipeteer.backend import ZmqBackend, HttpBackend
    @dataclass
    class DefaultSqlBackend(HttpBackend, ZmqBackend):
      ...
    return DefaultSqlBackend(engine=create_async_engine(sql_url), base_url=url)
  
  @staticmethod
  def sqlite(*, url: str, path: str):
    return Backend.sql(url=url, sql_url=f'sqlite+aiosqlite:///{path}')
  
  @staticmethod
  def client():
    return ClientBackend()

@dataclass
class LocalBackend(Backend):
  id: str

  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    url = f'{self.id}/{id}'
    return url, self.queue(id, type)
  
  def queue_at(self, url: str, type: type[A]) -> Queue[A]:
    if url.startswith(self.id):
      id = url.removeprefix(f'{self.id}/')
      return self.queue(id, type)
    else:
      return Queue.of(url, type)
    
class ClientBackend(Backend):
  def queue(self, id: str, type: type[A]) -> Queue[A]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def list_queue(self, id: str, type: type[A]) -> ListQueue[A]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def queue_at(self, url: str, type: type[A]) -> Queue[A]:
    return Queue.of(url, type)