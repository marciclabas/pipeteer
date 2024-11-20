from typing_extensions import TypeVar, Generic, Callable, overload, Any, Sequence, Protocol, Union
from dataclasses import dataclass
from pipeteer.pipelines import Runnable, Context

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
T = TypeVar('T')
Ctx = TypeVar('Ctx', bound=Context)
Artif1 = TypeVar('Artif1')
Artif2 = TypeVar('Artif2')

class MultiFn(Protocol, Generic[A, Ctx, T]): # type: ignore
  def __call__(self, *artifs: A, ctx: Ctx) -> T:
    ...

@dataclass
class MultiTask(Runnable[A, B, Ctx, Artif2], Generic[A, B, Ctx, Artif1, Artif2]):
  def __init__(
    self, id: str,
    pipelines: Sequence[Runnable[A, B, Ctx, Artif1]],
    merge: MultiFn[Artif1, Ctx, Artif2]
  ):
    self.id = id
    self.pipelines = pipelines
    self.merge = merge
    self.Tin = Union[*(pipe.Tin for pipe in pipelines)] # type: ignore
    self.Tout = Union[*(pipe.Tout for pipe in pipelines)] # type: ignore

  def run(self, ctx: Ctx, /) -> Artif2:
    artifs = tuple(pipe.run(ctx) for pipe in self.pipelines)
    return self.merge(*artifs, ctx=ctx) # type: ignore

class MultiFn2(Protocol, Generic[A, B, Ctx, T]): # type: ignore
  def __call__(self, a: A, b: B, /, ctx: Ctx) -> T: ...

class MultiFn3(Protocol, Generic[A, B, C, Ctx, T]): # type: ignore
  def __call__(self, a: A, b: B, c: C, /, ctx: Ctx) -> T: ...

class MultiFn4(Protocol, Generic[A, B, C, D, Ctx, T]): # type: ignore
  def __call__(self, a: A, b: B, c: C, d: D, /, ctx: Ctx) -> T: ...

@overload
def multitask( # type: ignore
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B], /, *,
  id: str | None = None,
) -> Callable[[MultiFn2[A, B, Ctx, T]], MultiTask[Any, Any, Ctx, Any, T]]:
  ...
@overload
def multitask(
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B],
  p3: Runnable[Any, Any, Any, C], /, *,
  id: str | None = None,
) -> Callable[[MultiFn3[A, B, C, Ctx, T]], MultiTask[Any, Any, Ctx, Any, T]]:
  ...
@overload
def multitask(
  p1: Runnable[Any, Any, Any, A],
  p2: Runnable[Any, Any, Any, B],
  p3: Runnable[Any, Any, Any, C],
  p4: Runnable[Any, Any, Any, D], /, *,
  id: str | None = None,
) -> Callable[[MultiFn4[A, B, C, D, Ctx, T]], MultiTask[Any, Any, Ctx, Any, T]]:
  ...
@overload
def multitask(*pipelines: Runnable[Any, Any, Any, T], id: str | None = None) -> Callable[[MultiFn[Any, Ctx, T]], MultiTask[Any, Any, Ctx, Any, T]]:
  ...

def multitask(*pipelines: Runnable[Any, Any, Any, T], id: str | None = None): # type: ignore
  def decorator(merge: MultiFn[Any, Ctx, T]):
    return MultiTask(id or merge.__name__, pipelines, merge) # type: ignore	
  return decorator
