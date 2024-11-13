from pipeteer.queues import Routed, WriteQueue
from typing_extensions import TypeVar, Generic
from dataclasses import dataclass
from pipeteer import Inputtable, Backend

A = TypeVar('A')
B = TypeVar('B')

@dataclass
class Client(Inputtable[A, B], Generic[A, B]):
  url: str
  Tin_: type[A]
  Tout_: type[B]
  id_: str

  @property
  def id(self) -> str:
    return self.id_
  
  @property
  def Tin(self) -> type[A]:
    return self.Tin_
  
  @property
  def Tout(self) -> type[B]:
    return self.Tout_

  def input(self, backend: Backend) -> WriteQueue[Routed[A]]:
    return backend.queue_at(self.url, Routed[self.Tin])