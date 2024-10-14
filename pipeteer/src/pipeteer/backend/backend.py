from typing import TypeVar, Sequence
from abc import ABC, abstractmethod
from pipeteer.queues import Queue, ListQueue

A = TypeVar('A')
B = TypeVar('B')

class Backend(ABC):

  @abstractmethod
  def queue(self, path: Sequence[str], type: type[A]) -> Queue[A]:
    ...

  @abstractmethod
  def list_queue(self, path: Sequence[str], type: type[A]) -> ListQueue[A]:
    ...

  @abstractmethod
  def output(self, type: type[A]) -> Queue[A]:
    ...

  @classmethod
  def sqlite(cls, path: str):
    from .default import DefaultBackend
    return DefaultBackend.sqlite(path)
  
  @classmethod
  def sql(cls, url: str):
    from .default import DefaultBackend
    return DefaultBackend.sql(url)