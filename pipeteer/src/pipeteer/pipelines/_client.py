from pipeteer.queues import Routed, WriteQueue
from typing_extensions import TypeVar, Generic, Any
from dataclasses import dataclass
from pipeteer import Inputtable, Context

A = TypeVar('A')
B = TypeVar('B')

@dataclass
class Client(Inputtable[A, B, Context], Generic[A, B]):
  url: str | None = None

  def input(self, ctx: Context) -> WriteQueue[Routed[A]]:
    if self.url is None:
      raise RuntimeError('Client URL is not set')
    return ctx.backend.queue_at(self.url, Routed[self.Tin])