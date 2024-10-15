from typing_extensions import TypeVar, Generic, Callable, Awaitable, Any, Protocol
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, promise as P
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import ReadQueue, WriteQueue, ListQueue
from pipeteer.util import param_type

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

class Stop(Exception):
  ...

class WorkflowContext(Protocol):
  async def call(self, pipe: Pipeline[A, B, Any, Any], x: A, /) -> B:
    ...

@dataclass
class WkfContext(WorkflowContext, Generic[Ctx]):
  ctx: Ctx
  prefix: tuple[str, ...]
  states: list
  key: str
  step: int = 0

  async def call(self, pipe: Pipeline[A, B, Ctx, Any], x: A, /) -> B:
    self.step += 1
    if self.step < len(self.states):
      return self.states[self.step]
    else:
      Qin = pipe.input(self.ctx, prefix=self.prefix)
      await Qin.push(self.key, x)
      raise Stop()

@dataclass
class Workflow(Pipeline[A, B, Ctx, Artifact | Callable[[], Process]], Generic[A, B, Ctx, Artifact]):
  pipelines: list[Pipeline]
  call: Callable[[A, WorkflowContext], Awaitable[B]]

  def states(self, ctx: Ctx, prefix: tuple[str, ...]) -> ListQueue:
    return ctx.backend.list_queue(prefix + (self.name, '_states'), self.type)

  async def rerun(self, key: str, ctx: Ctx, *, prefix: tuple[str, ...]):
    states = await self.states(ctx, prefix=prefix).read(key)
    wkf_ctx = WkfContext(ctx, prefix + (self.name,), states, key)
    return await self.call(states[0], wkf_ctx)

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()):
    
    Qin = self.input(ctx, prefix=prefix)
    Qstates = self.states(ctx, prefix=prefix)

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B], Qstates: ListQueue, ctx: Ctx, prefix: tuple[str, ...]):
      ctx = ctx.prefix(prefix + (self.name,))
      while True:
        try:
          key, val = await Qin.wait_any()
          
          states = (await Qstates.safe_read(key) or []) + [val]
          wkf_ctx = WkfContext(ctx, prefix + (self.name,), states, key)
          try:
            out = await self.call(states[0], wkf_ctx)
            await Qstates.pop(key)
            await Qout.push(key, out)

          except Stop:
            await Qstates.append(key, val)

          await Qin.pop(key)

        except Exception as e:
          ctx.log('Error', e, level='ERROR')

    procs = dict(_root=lambda: Process(target=runner, args=(Qin, Qout, Qstates, ctx, prefix)))
    for pipe in self.pipelines:
      procs |= pipe.run(Qin, ctx, prefix=prefix + (self.name,)) # type: ignore

    return { self.name: procs }
  

def workflow(pipelines: list[Pipeline[Any, Any, Ctx, Artifact]], name: str | None = None):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> Workflow[A, B, Ctx, Artifact]:
    return Workflow(
      type=param_type(fn),
      name=name or fn.__name__,
      pipelines=pipelines,
      call=fn,
    )
  return decorator