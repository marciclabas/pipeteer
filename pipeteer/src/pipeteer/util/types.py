from typing_extensions import TypeVar, Callable, Generic, get_type_hints, get_args
from pipeteer.queues import ReadQueue

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')

def param_type(fn: Callable, idx=0):
  return list(get_type_hints(fn).values())[idx]

def type_arg(generic: type, idx=0) -> type:
  return get_args(generic)[idx]

def num_params(fn) -> int:
  from inspect import signature
  return len(signature(fn).parameters)

Func1or2 = Callable[[A, B], C] | Callable[[A], C]
Func2or3 = Callable[[A, B, C], D] | Callable[[A, B], D]