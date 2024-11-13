from typing_extensions import TypeVar, Generic, Self, Mapping
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace, KW_ONLY
from dslog import Logger
from pipeteer.backend import Backend
from pipeteer.queues import Queue, WriteQueue, Routed


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

class Observable(ABC, Generic[Ctx]):
  @abstractmethod
  def observe(self, ctx: Ctx) -> Mapping[str, Queue]:
    ...

class Pipeline(Runnable[A, B, Ctx, Artifact], Inputtable[A, B, Ctx], Observable[Ctx], Generic[A, B, Ctx, Artifact]):
  def as_client(self, url: str) -> 'Pipeline[A, B, Context, Artifact]':
    """Modify (in-place) the pipeline to act as `Client(url)`"""
    self.url = url
    self.input = lambda ctx: ctx.backend.queue_at(self.url, Routed[self.Tin])
    return self # type: ignore
