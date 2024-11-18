from typing_extensions import Iterable, Coroutine, Any, TypeVar, Callable, Awaitable
import asyncio

A = TypeVar('A')

async def race(coros: Iterable[Coroutine[Any, Any, A]]) -> tuple[int, A]:
  """Race `coros` and return `(idx, result)` of the first completed coroutine"""
  async def enum_task(idx, coro):
    return idx, await coro
  
  tasks = [asyncio.create_task(enum_task(i, c)) for i, c in enumerate(coros)]
  done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
  for task in pending:
    task.cancel()
  return done.pop().result()


async def exp_backoff(
  fn: Callable[[], Awaitable[A|None]], *,
  t0: float = 1, base: float = 2, tmax: float | None = None
) -> A:
  """Exponential backoff polling, waiting `t(i) = t0 * base^i` seconds before the `i`-th call to `fn`
  - `tmax`: optional upper bound. If set, `t(i) = min(t0 * base^i, tmax)`
  """
  i = 0
  while True:
    if (x := await fn()):
      return x
    t = t0 * base ** i
    if tmax is not None:
      t = min(t, tmax)
    await asyncio.sleep(t)