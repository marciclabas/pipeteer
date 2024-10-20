from typing_extensions import TypeVar, Generic, Callable, Awaitable, Any, Protocol, overload, Union
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, promise as P
from pipeteer.pipelines import Pipeline, Inputtable, Context, Runnable
from pipeteer.queues import Queue, ReadQueue, WriteQueue, ListQueue, ops
from pipeteer.util import param_type, return_type

Aw = Awaitable
A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

class Stop(Exception):
  ...

class WorkflowContext(Protocol):
  async def call(self, pipe: Inputtable[A, B, Any], x: A, /) -> B:
    ...

  @overload
  async def all(self, a: Aw[A], b: Aw[B], /) -> tuple[A, B]: ...
  @overload
  async def all(self, a: Aw[A], b: Aw[B], c: Aw[C], /) -> tuple[A, B, C]: ...
  @overload
  async def all(self, a: Aw[A], b: Aw[B], c: Aw[C], d: Aw[D], /) -> tuple[A, B, C, D]: ...
  @overload
  async def all(self, *coros: Aw[A]) -> tuple[A, ...]: ...

@dataclass
class WkfContext(WorkflowContext, Generic[Ctx]):
  ctx: Ctx
  prefix: tuple[str, ...]
  states: list
  key: str
  step: int = 0

  async def call(self, pipe: Inputtable[A, B, Ctx], x: A, /) -> B:
    self.step += 1
    if self.step < len(self.states):
      return self.states[self.step]
    else:
      Qin = pipe.input(self.ctx, prefix=self.prefix)
      await Qin.push(f'{self.step}_{self.key}', x)
      raise Stop()
    
  async def all(self, *coros: Awaitable):
    n = len(coros)
    if self.step + n < len(self.states):
      prev = self.step+1
      self.step += n
      return tuple(self.states[prev:prev+n])
    
    elif self.step+1 == len(self.states):
      for coro in coros:
        try:
          await coro
        except Stop:
          ...
    
    raise Stop()

@dataclass
class Workflow(Pipeline[A, B, Ctx, Artifact | Callable[[], Process]], Generic[A, B, Ctx, Artifact]):
  pipelines: list[Runnable]
  call: Callable[[A, WorkflowContext], Awaitable[B]]

  def states(self, ctx: Ctx, prefix: tuple[str, ...]) -> ListQueue[tuple]:
    """Actual data store"""
    type = Union[self.Tin, *(pipe.Tout for pipe in self.pipelines)] # type: ignore
    return ctx.backend.list_queue(prefix + (self.name, '_states'), tuple[int, type])
  
  def tasks(self, ctx: Ctx, prefix: tuple[str, ...]) -> ListQueue[str]:
    """Queue to keep track of tasks"""
    return ctx.backend.list_queue(prefix + (self.name, '_tasks'), str)
  
  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> WriteQueue[A]:
    """Input view, creating a singleton state. For inputting new tasks."""
    return ops.tee(
      ops.singleton(self.states(ctx, prefix)).premap(lambda x: (0, x)),
      ops.appender(self.tasks(ctx, prefix)).premap(lambda _: ''),
      ordered=True
    )
  
  def collector(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> WriteQueue:
    """Collector view, appending a state. For subpipelines."""
    def indexed(k: str, v: C) -> tuple[str, tuple[int, C]]:
      i, key = k.split('_', 1)
      return key, (int(i), v)
    return ops.tee(
      ops.appender(self.states(ctx, prefix)).premap_kv(indexed),
      ops.appender(self.tasks(ctx, prefix)).premap(lambda _: '').premap_k(lambda k: k.split('_', 1)[1]),
      ordered=True
    )


  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()):
    
    Qstates = self.states(ctx, prefix=prefix)
    Qin = self.tasks(ctx, prefix=prefix)
    Qresults = self.collector(ctx, prefix=prefix)

    @P.run
    async def runner(Qin: ReadQueue[list[A]], Qout: WriteQueue[B], Qstates: ListQueue[tuple[int, Any]], ctx: Ctx, prefix: tuple[str, ...]):
      ctx = ctx.prefix(prefix + (self.name,))
      while True:
        try:
          key, _ = await Qin.wait_any()
          ctx.log('Processing:', key, level='DEBUG')
          states = await Qstates.read(key)
          states = [v for _, v in sorted(states)]
          wkf_ctx = WkfContext(ctx, prefix + (self.name,), states, key)
          try:
            out = await self.call(states[0], wkf_ctx)
            await Qstates.pop(key)
            await Qout.push(key, out)

          except Stop:
            ...

          await Qin.pop(key)

        except Exception as e:
          ctx.log('Error', e, level='ERROR')

    procs = dict(_root=lambda: Process(target=runner, args=(Qin, Qout, Qstates, ctx, prefix)))
    for pipe in self.pipelines:
      children = pipe.run(Qresults, ctx, prefix=prefix + (self.name,))
      if not isinstance(children, dict):
        children = { pipe.name: children }
      procs |= children # type: ignore

    return { self.name: procs }
  

def workflow(
  pipelines: list[Runnable[Any, Any, Ctx, Artifact]],
  *, name: str | None = None,
  Tin: type[A] | None = None, Tout: type[B] | None = None,
):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> Workflow[A, B, Ctx, Artifact]:
    Tin_ = Tin or param_type(fn)
    if Tin_ is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its input parameter')

    Tout_ = Tout or return_type(fn)
    if Tout_ is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its return value')
    
    return Workflow(
      Tin=Tin_, Tout=Tout_,
      name=name or fn.__name__,
      pipelines=pipelines,
      call=fn, # type: ignore
    ) # type: ignore	
  return decorator