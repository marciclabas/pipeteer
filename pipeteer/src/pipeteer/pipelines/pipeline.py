from typing_extensions import TypeVar, Generic, Callable, Self, Protocol, Sequence
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace, KW_ONLY
from multiprocessing import Process
from haskellian import Tree, trees
from dslog import Logger
from pipeteer import Queue, WriteQueue, Backend

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D', contravariant=True)
Artifact = TypeVar('Artifact', covariant=True, bound=Tree)

@dataclass
class Context:
  backend: Backend
  _: KW_ONLY
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

Ctx = TypeVar('Ctx', bound=Context)

class Executor(Protocol, Generic[D]):
  def __call__(self, path: Sequence[str], artifact: D, /) -> Process | None:
    ...

def default_executor(_, artifact: Callable[[], Process]) -> Process:
  return artifact()

@dataclass
class Inputtable(Generic[A, B, Ctx]):
  Tin: type[A]
  Tout: type[B]
  name: str

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> WriteQueue[A]:
    return ctx.backend.queue(prefix + (self.name,), self.Tin)
  
@dataclass
class Runnable(ABC, Generic[A, B, Ctx, Artifact]):
  Tin: type[A]
  Tout: type[B]
  name: str
  
  @abstractmethod
  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    ...

  def run_all(
    self, Qout: WriteQueue[B], ctx: Ctx, *,
    prefix: tuple[str, ...] = (), executor: Executor[Artifact] = default_executor
  ):
    procs = self.run(Qout, ctx, prefix=prefix)
    procs = trees.path_map(procs, executor)
    
    for path, proc in trees.flatten(procs):
      if proc:
        key = '/'.join((k for k in path if k != '_root'))
        ctx.log(f'[{key}] Starting...')
        proc.start()
    
    for path, proc in trees.flatten(procs):
      if proc:
        key = '/'.join((k for k in path if k != '_root'))
        proc.join()
        ctx.log(f'[{key}] Stopping...')

class Pipeline(Runnable[A, B, Ctx, Artifact], Inputtable[A, B, Ctx]):
  ...