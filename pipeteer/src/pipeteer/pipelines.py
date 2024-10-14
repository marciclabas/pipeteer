from typing_extensions import TypeVar, Generic, Callable, Awaitable, Protocol, get_type_hints, Any, Self
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, trees, promise as P
from dslog import Logger
from pipeteer import ReadQueue, WriteQueue, Backend, ListQueue, Transaction

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

Ctx = TypeVar('Ctx', bound=Context, default=Any)

Artifact = Callable[[], Process]

@dataclass
class Pipeline(ABC, Generic[A, B, Ctx]):
  type: type[A]
  name: str

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> WriteQueue[A]:
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

@dataclass
class Task(Pipeline[A, B, Ctx], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    Qin = self.input(ctx, prefix=prefix)
    ctx = ctx.prefix(prefix + (self.name,))

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B]):
      while True:
        try:
          k, x = await Qin.wait_any(reserve=self.reserve)
          y = await self.call(x, ctx)
          async with Transaction(Qin, Qout, autocommit=True):
            await Qout.push(k, y)
            await Qin.pop(k)

        except Exception as e:
          ctx.log(f'Error: {e}', level='ERROR')

    return { self.name: lambda: Process(target=runner, args=(Qin, Qout)) }
  

def param_type(fn, idx=0):
  return list(get_type_hints(fn).values())[idx]

def num_params(fn) -> int:
  from inspect import signature
  return len(signature(fn).parameters)

Func1or2 = Callable[[A, B], C] | Callable[[A], C]

def task(name: str | None = None, *, reserve: timedelta | None = timedelta(minutes=1)):
  def decorator(fn: Func1or2[A, Ctx, Awaitable[B]]) -> Task[A, B, Ctx]:
    return Task(
      type=param_type(fn), reserve=reserve, name=name or fn.__name__,
      call=fn if num_params(fn) == 2 else (lambda x, _: fn(x)) # type: ignore
    )
      
  return decorator
  
class Stop(Exception):
  ...

class WorkflowContext(Protocol):
  async def call(self, pipe: Pipeline[A, B], x: A, /) -> B:
    ...

@dataclass
class WkfContext(WorkflowContext, Generic[Ctx]):
  ctx: Ctx
  prefix: tuple[str, ...]
  states: list
  key: str
  step: int = 0

  async def call(self, pipe: Pipeline[A, B, Ctx], x: A, /) -> B:
    self.step += 1
    if self.step < len(self.states):
      return self.states[self.step]
    else:
      self.ctx.log(f'Running step {self.step}: {pipe.name}({self.states[self.step-1]}) [{self.key}]', level='DEBUG')
      Qin = pipe.input(self.ctx, prefix=self.prefix)
      await Qin.push(self.key, x)
      raise Stop()

@dataclass
class Workflow(Pipeline[A, B, Ctx], Generic[A, B, Ctx]):
  pipelines: list[Pipeline]
  call: Callable[[A, WorkflowContext], Awaitable[B]]

  def states(self, ctx: Ctx, prefix: tuple[str, ...]) -> ListQueue:
    return ctx.backend.list_queue(prefix + (self.name, '_states'), self.type)

  async def rerun(self, key: str, ctx: Ctx, *, prefix: tuple[str, ...]):
    states = await self.states(ctx, prefix=prefix).read(key)
    wkf_ctx = WkfContext(ctx, prefix + (self.name,), states, key)
    return await self.call(states[0], wkf_ctx)

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    
    Qin = self.input(ctx, prefix=prefix)
    Qstates = self.states(ctx, prefix=prefix)

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B], Qstates: ListQueue, ctx: Ctx, prefix: tuple[str, ...]):
      ctx = ctx.prefix(prefix + (self.name,))
      while True:
        try:
          key, val = await Qin.wait_any()
          await Qstates.append(key, val)
          try:
            out = await self.rerun(key, ctx, prefix=prefix)
            ctx.log(f'Outputting {out} [{key}]', level='DEBUG')
            await Qout.push(key, out)
            await Qstates.pop(key)
          except Stop:
            ...
          await Qin.pop(key)
        
        except Exception as e:
          ctx.log(f'Error: {e}', level='ERROR')

    procs = dict(_root=lambda: Process(target=runner, args=(Qin, Qout, Qstates, ctx, prefix)))
    for pipe in self.pipelines:
      procs |= pipe.run(Qin, ctx, prefix=prefix + (self.name,)) # type: ignore

    return { self.name: procs }
  

def workflow(pipelines: list[Pipeline[Any, Any, Ctx]], name: str | None = None):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> Workflow[A, B, Ctx]:
    return Workflow(
      type=param_type(fn),
      name=name or fn.__name__,
      pipelines=pipelines,
      call=fn,
    )
  return decorator