from typing_extensions import TypeVar, Generic, Callable, Awaitable
from abc import abstractmethod
from dataclasses import dataclass, field
import asyncio
from datetime import timedelta
from multiprocessing import Process
from dslog import Logger
from pipeteer import Pipeline, Backend
from pipeteer.queues import Queue, Transaction, Routed
from pipeteer.util import param_type, return_type

A = TypeVar('A')
B = TypeVar('B')

@dataclass(kw_only=True)
class Activity(Pipeline[A, B, Process], Generic[A, B]):
  id_: str | None = None
  reserve: timedelta | None = None
  log: Logger = None # type: ignore

  def __post_init__(self):
    self.log = self.log or Logger.click().prefix(f'[{self.id}]')

  @property
  def id(self) -> str:
    return self.id_ or self.__class__.__name__.lower()

  @property
  def Tin(self) -> type[A]:
    Tin = param_type(self.__call__)
    if Tin is None:
      raise TypeError(f'Activity {self.__call__.__name__} must have a type hint for its input parameter')
    return Tin
  
  @property
  def Tout(self) -> type[B]:
    Tout = return_type(self.__call__)
    if Tout is None:
      raise TypeError(f'Activity {self.__call__.__name__} must have a type hint for its return value')
    return Tout

  def input(self, backend: Backend) -> Queue[Routed[A]]:
    return backend.queue(self.id, Routed[self.Tin])

  def observe(self, backend: Backend) -> dict[str, Queue[Routed[A]]]:
    return { 'input': self.input(backend) }
  
  @abstractmethod
  async def __call__(self, x: A) -> B:
    ...

  def run(self, backend: Backend) -> Process:
    async def loop():
      Qin = self.input(backend)
      while True:
        try:
          k, x = await Qin.wait_any(reserve=self.reserve)
          self.log(f'Processing "{k}" with input {x["value"]}', level='DEBUG')
          try:
            y = await self(x['value'])
            Qout = backend.queue_at(x['url'], self.Tout)
            async with Transaction(Qin, Qout, autocommit=True):
              await Qout.push(k, y)
              await Qin.pop(k)

          except Exception as e:
            self.log(f'Error processing "{k}": {e}. Value: {x["value"]}', level='ERROR')

        except Exception as e:
          self.log(f'Error reading from input queue: {e}', level='ERROR')
      
    def runner():
      asyncio.run(loop())

    return Process(target=runner)

@dataclass(kw_only=True)
class FnActivity(Activity[A, B]):
  _call: Callable[[A], Awaitable[B]]
  async def __call__(self, x: A) -> B:
    return await self._call(x)


def activity(
  id: str | None = None, *,
  reserve: timedelta | None = timedelta(minutes=2),
):
  def decorator(fn: Callable[[A], Awaitable[B]]) -> Activity[A, B]:
    return FnActivity(reserve=reserve, id_=id or fn.__name__, _call=fn)
      
  return decorator