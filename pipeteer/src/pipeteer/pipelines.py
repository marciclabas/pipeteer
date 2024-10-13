from typing_extensions import TypeVar, Generic, Callable, Awaitable, Protocol, get_type_hints, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, promise as P
from pipeteer import ReadQueue, WriteQueue, Backend, ListQueue

A = TypeVar('A', default=Any)
B = TypeVar('B', default=Any)
C = TypeVar('C', default=Any)

@dataclass
class Context:
  backend: Backend

Ctx = TypeVar('Ctx', bound=Context, default=Any)

Artifact = Callable[[], Process]

@dataclass
class Pipeline(ABC, Generic[A, B, Ctx]):
  type: type[A]
  name: str

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> WriteQueue[A]:
    return ctx.backend.queue(prefix, self.type)

  @abstractmethod
  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    ...

@dataclass
class Task(Pipeline[A, B, Ctx], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    Qin = ctx.backend.queue(prefix, self.type)

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B]):
      while True:
        k, x = await Qin.read_any(reserve=self.reserve)
        y = await self.call(x, ctx)
        await Qout.push(k, y)
        await Qin.pop(k)

    return lambda: Process(target=runner, args=(Qin, Qout))
  
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
      Qin = pipe.input(self.ctx, prefix=self.prefix + (pipe.name,))
      await Qin.push(self.key, x)
      raise Stop()

@dataclass
class Workflow(Pipeline[A, B, Ctx], Generic[A, B, Ctx]):
  pipelines: list[Pipeline]
  call: Callable[[A, WorkflowContext], Awaitable[B]]

  async def rerun(self, key: str, ctx: Ctx, *, prefix: tuple[str, ...]):
    states = await ctx.backend.list_queue(prefix + ('_states',), self.type).read(key)
    wkf_ctx = WkfContext(ctx, prefix, states, key)
    return await self.call(states[0], wkf_ctx)

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    Qin = ctx.backend.queue(prefix, self.type)
    Qstates = ctx.backend.list_queue(prefix + ('_states',), self.type)

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B], Qstates: ListQueue, ctx: Ctx, prefix: tuple[str, ...]):
      while True:
        key, val = await Qin.read_any()
        await Qstates.append(key, val)
        try:
          out = await self.rerun(key, ctx, prefix=prefix)
          await Qout.push(key, out)
          await Qstates.pop(key)
        except Stop:
          ...
        await Qin.pop(key)

    procs = dict(wkf=lambda: Process(target=runner, args=(Qin, Qout, Qstates, ctx, prefix)))
    for pipe in self.pipelines:
      procs[pipe.name] = pipe.run(Qin, ctx, prefix=prefix + (pipe.name,)) # type: ignore

    return procs
  

def workflow(pipelines: list[Pipeline[Any, Any, Ctx]], name: str | None = None):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> Workflow[A, B, Ctx]:
    return Workflow(
      type=param_type(fn),
      name=name or fn.__name__,
      pipelines=pipelines,
      call=fn,
    )
  return decorator