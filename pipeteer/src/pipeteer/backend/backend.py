from typing_extensions import TypeVar, overload, Literal
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pipeteer.queues import Queue, ListQueue

A = TypeVar('A')
B = TypeVar('B')

@overload
def getenv(name: str) -> str: ...
@overload
def getenv(name: str, required: Literal[False]) -> str | None: ...
def getenv(name: str, required: bool = True) -> str | None:
  import os
  value = os.getenv(name)
  if value is None and required:
    raise ValueError(f'{name} is not set')
  return value or ''

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
  def sql(
    *, public_url: str | None = None, db_url: str | None = None, callback_url: str | None = None,
    secret: str | None = None
  ):
    db_url = db_url or getenv('DB_URL')
    public_url = public_url or getenv('PUBLIC_URL')
    callback_url = callback_url or getenv('CALLBACK_URL', required=False) or public_url
    secret = secret or getenv('SECRET')
    from sqlalchemy.ext.asyncio import create_async_engine
    from pipeteer.backend import ZmqBackend, HttpBackend
    @dataclass
    class DefaultSqlBackend(HttpBackend, ZmqBackend):
      ...
    return DefaultSqlBackend(
      engine=create_async_engine(db_url), public_url=public_url,
      callback_url=callback_url, secret=secret,
    )
  
  @staticmethod
  def client():
    return ClientBackend()
     
class ClientBackend(Backend):
  def queue(self, id: str, type: type[A]) -> Queue[A]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def list_queue(self, id: str, type: type[A]) -> ListQueue[A]:
    raise NotImplementedError('ClientBackend can only create remote queues')
  
  def queue_at(self, url: str, type: type[A]) -> Queue[A]:
    return Queue.of(url, type)