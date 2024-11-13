from typing_extensions import TypeVar, Generic, Callable, Awaitable, Any, Protocol, overload
from dataclasses import dataclass, field
import asyncio
from multiprocessing import Process
import traceback
from pydantic import TypeAdapter
from dslog import Logger
from pipeteer import Pipeline, Inputtable, Backend
from pipeteer.queues import Queue, Transaction, Routed
from pipeteer.util import param_type, return_type, race

Aw = Awaitable
A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
AnyT: type = Any # type: ignore

class Stop(Exception):
  ...

class WorkflowContext(Protocol):
  async def call(self, pipe: Inputtable[A, B], x: A, /) -> B:
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
class WkfContext(WorkflowContext):
  backend: Backend
  log: Logger
  states: list
  key: str
  callback_url: str
  step: int = 0

  async def call(self, pipe: Inputtable[A, B], x: A, /) -> B:
    self.step += 1
    if self.step < len(self.states):
      val = self.states[self.step]
      return TypeAdapter(pipe.Tout).validate_python(val)
    else:
      Qin = pipe.input(self.backend)
      self.log(f'Calling {pipe.id}({x}), step={self.step}, key="{self.key}"', level='DEBUG')
      await Qin.push(f'{self.step}_{self.key}', { 'url': self.callback_url, 'value': x })
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
  
@dataclass(kw_only=True)
class FnWorkflow(Pipeline[A, B, Process], Generic[A, B]):
  call: Callable[[A, WorkflowContext], Awaitable[B]]
  id_: str | None = None
  log: Logger = field(default_factory=Logger.click)

  @property
  def id(self) -> str:
    return self.id_ or self.__class__.__name__.lower()

  @property
  def Tin(self) -> type[A]:
    Tin = param_type(self.call)
    if Tin is None:
      raise TypeError(f'Activity {self.call.__name__} must have a type hint for its input parameter')
    return Tin
  
  @property
  def Tout(self) -> type[B]:
    Tout = return_type(self.call)
    if Tout is None:
      raise TypeError(f'Activity {self.call.__name__} must have a type hint for its return value')
    return Tout

  def states(self, backend: Backend):
    return backend.list_queue(self.id + '-states', tuple[int, Any])

  def urls(self, backend: Backend):
    return backend.queue(self.id + '-urls', str)
  
  def input(self, backend: Backend) -> Queue[Routed[A]]:
    return backend.queue(self.id, Routed[self.Tin])
  
  def results(self, backend: Backend) -> tuple[str, Queue]:
    return backend.public_queue(self.id + '-results', AnyT)
  
  def observe(self, backend: Backend):
    return {
      'input': self.input(backend),
      'states': self.states(backend),
      'urls': self.urls(backend),
      'results': self.results(backend),
    }

  def run(self, backend: Backend):
      
    self.results(backend) # trigger creation
    
    async def loop():
      callback_url, Qresults = self.results(backend)
      Qin = self.input(backend)
      Qstates = self.states(backend)
      Qurls = self.urls(backend)

      async def run(key: str, states: list):
        wkf_ctx = WkfContext(backend, log=self.log, states=states, key=key, callback_url=callback_url)
        self.log(f'Rerunning: key="{key}", states={states}', level='DEBUG')
        out = await self.call(states[0], wkf_ctx)
        self.log(f'Outputting: key="{key}", value={out}', level='DEBUG')
        out_url = await Qurls.read(key)
        Qout = backend.queue_at(out_url, self.Tout)
        
        async with Transaction(Qout, Qurls, Qstates, autocommit=True):
          await Qout.push(key, out)
          await Qurls.pop(key)
          await Qstates.pop(key)

      async def input_step(key, x):
        value, url = x['value'], x['url']
        self.log(f'Input loop: key="{key}", value={value}', level='DEBUG')
        try:
          await run(key, [value])
        except Stop:
          async with Transaction(Qin, Qstates, Qurls, autocommit=True):
            await Qin.pop(key)
            await Qstates.push(key, [(0, value)])
            await Qurls.push(key, url)

      async def results_step(idx_key: str, val):
        i, key = idx_key.split('_', 1)
        i = int(i)
        self.log(f'Results loop: key="{key}", value={val}, step={i}', level='DEBUG')
        states = await Qstates.read(key) + [(i, val)]
        states = [v for _, v in sorted(states)]

        try:
          await run(key, states)
          await Qresults.pop(idx_key)
          
        except Stop:
          async with Transaction(Qresults, Qstates, autocommit=True):
            await Qresults.pop(idx_key)
            await Qstates.append(key, (i, val))

        # has = await Qresults.has(idx_key)
        # if has:
        #   backend.log(f'Results loop: key="{key}", value={val}, step={i}, has more', level='CRITICAL')

      while True:
        try:
          idx, (k, v) = await race([Qin.wait_any(), Qresults.wait_any()])
          try:
            fn = input_step if idx == 0 else results_step
            await fn(k, v)

          except Exception:
            loop = 'Input' if idx == 0 else 'Results'
            self.log(f'{loop} loop error:', traceback.format_exc(), level='ERROR')

        except Exception:
          self.log('Error waiting for items:', traceback.format_exc(), level='ERROR')

    coro = loop()
    return Process(target=asyncio.run, args=(coro,))
  

def workflow(
  *, id: str | None = None,
):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> FnWorkflow[A, B]:
    return FnWorkflow(id_=id or fn.__name__, call=fn)
  return decorator