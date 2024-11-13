from typing_extensions import TypeVar, Generic, Callable, Any
from dataclasses import dataclass
from abc import abstractmethod
from pipeteer import Pipeline, Backend
from pipeteer.queues import Queue, ReadQueue, WriteQueue, Routed, RoutedQueue, ops
from pipeteer.util import param_type, type_arg, num_params, Func2or3

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Artifact = TypeVar('Artifact')

@dataclass(kw_only=True)
class Task(Pipeline[A, B, Artifact], Generic[A, B, Artifact]):
  id_: str | None = None

  @property
  def id(self) -> str:
    return self.id_ or self.__class__.__name__.lower()

  @property
  def Tin(self) -> type[A]:
    Tin = type_arg(param_type(self.call, 0) or Any) # type: ignore
    if Tin is None:
      raise TypeError(f'Task {self.call.__name__} must have a type hint for its input type')
    return Tin
    
  @property
  def Tout(self) -> type[B]:
    Tout = type_arg(param_type(self.call, 1) or Any) # type: ignore
    if Tout is None:
      raise TypeError(f'Task {self.call.__name__} must have a type hint for its output type')
    return Tout
  
  @abstractmethod
  def call(self, Qin: ReadQueue[A], Qout: WriteQueue[B], /) -> Artifact:
    ...

  def Qurls(self, backend: Backend) -> Queue[str]:
    return backend.queue(self.id+'-urls', str)
  
  def Qin(self, backend: Backend) -> Queue[A]:
    return backend.queue(self.id, self.Tin)

  def input(self, backend: Backend) -> WriteQueue[Routed[A]]:
    return ops.tee(
      self.Qurls(backend).premap(lambda x: x['url']),
      self.Qin(backend).premap(lambda x: x['value'])
    )
  
  def observe(self, backend: Backend):
    return { 'input': self.Qin(backend), 'urls': self.Qurls(backend) }
  
  def run(self, backend: Backend, /):
    Qin = self.Qin(backend)
    Qout = RoutedQueue(self.Qurls(backend), lambda url: backend.queue_at(url, self.Tout))
    return self.call(Qin, Qout)
  
@dataclass
class FnTask(Task[A, B, Artifact], Generic[A, B, Artifact]):
  _call: Callable[[ReadQueue[A], WriteQueue[B]], Artifact]
  def call(self, Qin: ReadQueue[A], Qout: WriteQueue[B]) -> Artifact:
    return self._call(Qin, Qout)

def task(id: str | None = None):
  def decorator(fn: Callable[[ReadQueue[A], WriteQueue[B]], Artifact]) -> Task[A, B, Artifact]:
    return FnTask(id_=id or fn.__name__.lower(), _call=fn)
      
  return decorator