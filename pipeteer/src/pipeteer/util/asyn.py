from typing_extensions import Iterable, Coroutine, Any, TypeVar
import asyncio

A = TypeVar('A')

async def race(coros: Iterable[Coroutine[Any, Any, A]]) -> tuple[int, A]:
  async def enum_task(idx, coro):
    return idx, await coro
  
  tasks = [asyncio.create_task(enum_task(i, c)) for i, c in enumerate(coros)]
  done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
  for task in pending:
    task.cancel()
  return done.pop().result()