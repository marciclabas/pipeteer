from typing_extensions import TypeVar, Generic, Self, TypedDict
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace, KW_ONLY
from dslog import Logger
from pipeteer.backend import Backend
from pipeteer.queues import WriteQueue, Routed

A = TypeVar('A')
B = TypeVar('B')
Artifact = TypeVar('Artifact', covariant=True)

@dataclass
class Context:
  backend: Backend
  _: KW_ONLY
  log: Logger = field(default_factory=lambda: Logger.click().limit('INFO'))

  def prefix(self, prefix: str) -> Self:
    return replace(self, log=self.log.prefix(prefix))

Ctx = TypeVar('Ctx', bound=Context)

@dataclass
class Inputtable(Generic[A, B, Ctx]):
  Tin: type[A]
  Tout: type[B]
  id: str

  def input(self, ctx: Ctx) -> WriteQueue[Routed[A]]:
    return ctx.backend.queue(self.id, Routed[self.Tin])
  
@dataclass
class Runnable(ABC, Generic[A, B, Ctx, Artifact]):
  Tin: type[A]
  Tout: type[B]
  id: str
  
  @abstractmethod
  def run(self, ctx: Ctx, /) -> Artifact:
    ...

class Pipeline(Runnable[A, B, Ctx, Artifact], Inputtable[A, B, Ctx]):
  ...