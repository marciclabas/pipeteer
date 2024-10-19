from typing_extensions import TypeVar, Generic, Callable, overload, Any, Sequence
from dataclasses import dataclass
from pipeteer.pipelines import Runnable, Context
from pipeteer.queues import WriteQueue

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
T = TypeVar('T')
Ctx = TypeVar('Ctx', bound=Context)
Artif1 = TypeVar('Artif1')
Artif2 = TypeVar('Artif2')

@dataclass
class MultiTask(Runnable[A, B, Ctx, Artif2], Generic[A, B, Ctx, Artif1, Artif2]):
  pipelines: Sequence[Runnable[A, B, Ctx, Artif1]]
  merge: Callable[[tuple[Artif1, ...]], Artif2]

  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Artif2:
    artifs = tuple(pipe.run(Qout, ctx, prefix=prefix) for pipe in self.pipelines)
    return self.merge(*artifs) # type: ignore
  
@overload
def multitask(
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B], /, *,
  name: str | None = None,
) -> Callable[[Callable[[A, B], T]], Runnable[Any, Any, Any, T]]:
  ...
@overload
def multitask(
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B],
  p3: Runnable[Any, Any, Any, C], /, *,
  name: str | None = None,
) -> Callable[[Callable[[A, B, C], T]], Runnable[Any, Any, Any, T]]:
  ...
@overload
def multitask(
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B],
  p3: Runnable[Any, Any, Any, C],
  p4: Runnable[Any, Any, Any, D], /, *,
  name: str | None = None,
) -> Callable[[Callable[[A, B, C, D], T]], Runnable[Any, Any, Any, T]]:
  ...
@overload
def multitask(*pipelines: Runnable[Any, Any, Any, T], name: str | None = None) -> Callable[[Callable[..., T]], Runnable[Any, Any, Any, T]]:
  ...

def multitask(*pipelines: Runnable[Any, Any, Any, T], name: str | None = None): # type: ignore
  def decorator(merge: Callable[..., T]):
    return MultiTask(name or merge.__name__, pipelines, merge)
  return decorator
