from typing_extensions import TypeVar, Generic, Callable, Awaitable, Protocol, get_type_hints, Any, Self
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, trees, promise as P
from dslog import Logger
from pipeteer import Queue, ReadQueue, WriteQueue, Backend, ListQueue, Transaction

A = TypeVar('A', default=Any)
B = TypeVar('B', default=Any)
C = TypeVar('C', default=Any)

@dataclass
class Context:
  backend: Backend
  log: Logger = field(default_factory=Logger.click)

  def prefix(self, path: tuple[str, ...]) -> Self:
    key = '/'.join(path) or 'root'
    return replace(self, log=self.log.prefix(f'[{key}]'))

  @classmethod
  def sqlite(cls, path: str):
    return cls(Backend.sqlite(path))
  
  @classmethod
  def sql(cls, url: str):
    return cls(Backend.sql(url))

Ctx = TypeVar('Ctx', bound=Context, default=Any)

Artifact = Callable[[], Process]

@dataclass
class Pipeline(ABC, Generic[A, B, Ctx]):
  type: type[A]
  name: str

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Queue[A]:
    return ctx.backend.queue(prefix + (self.name,), self.type)

  @abstractmethod
  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    ...

  def run_all(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()):
    procs = self.run(Qout, ctx, prefix=prefix)
    procs = trees.map(procs, lambda proc: proc())
    
    for path, proc in trees.flatten(procs):
      key = '/'.join((k for k in path if k != '_root'))
      ctx.log(f'[{key}] Starting...')
      proc.start()
    
    for path, proc in trees.flatten(procs):
      key = '/'.join((k for k in path if k != '_root'))
      proc.join()
      ctx.log(f'[{key}] Stopping...')