from typing_extensions import TypeVar, Callable, get_args
import inspect

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')

def param_type(fn: Callable, idx=0) -> type | None:
  sig = inspect.signature(fn)
  ann = sig.parameters[list(sig.parameters.keys())[idx]].annotation
  if ann != inspect.Signature.empty:
    return ann

def return_type(fn: Callable) -> type | None:
  ann = inspect.signature(fn).return_annotation
  if ann != inspect.Signature.empty:
    return ann


def type_arg(generic: type, idx=0) -> type | None:
  try:
    return get_args(generic)[idx]
  except IndexError:
    ...
  try:
    return generic.__args__[idx]
  except:
    ...

def num_params(fn) -> int:
  return len(inspect.signature(fn).parameters)

Func1or2 = Callable[[A, B], C] | Callable[[A], C]
Func2or3 = Callable[[A, B, C], D] | Callable[[A, B], D]