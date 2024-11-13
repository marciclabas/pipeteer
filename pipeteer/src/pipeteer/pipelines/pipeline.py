from typing_extensions import TypeVar, Generic, Mapping
from abc import ABC, abstractmethod
from pipeteer.backend import Backend
from pipeteer.queues import Queue, WriteQueue, Routed

A = TypeVar('A')
B = TypeVar('B')
Artifact = TypeVar('Artifact', covariant=True)

class Base(ABC, Generic[A, B]):
  @property
  @abstractmethod
  def id(self) -> str:
    ... 

  @property
  @abstractmethod
  def Tin(self) -> type[A]:
    ...

  @property
  @abstractmethod
  def Tout(self) -> type[B]:
    ...

  async def __call__(self, x: A) -> B:
    ...

class Inputtable(Base[A, B], Generic[A, B]):
  def input(self, backend: Backend) -> WriteQueue[Routed[A]]:
    return backend.queue(self.id, Routed[self.Tin])
  

class Runnable(Base[A, B], Generic[A, B, Artifact]):
  @abstractmethod
  def run(self, backend: Backend, /) -> Artifact:
    ...

class Observable(ABC):
  @abstractmethod
  def observe(self, backend: Backend) -> Mapping[str, Queue]:
    ...

class Pipeline(Runnable[A, B, Artifact], Inputtable[A, B], Observable):
  ...