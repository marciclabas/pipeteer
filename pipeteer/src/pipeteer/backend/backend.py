from typing import TypeVar, Sequence, Self
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

  @staticmethod
  def sqlite(path: str):
    from .sql import SqlBackend
    return SqlBackend.at(path)