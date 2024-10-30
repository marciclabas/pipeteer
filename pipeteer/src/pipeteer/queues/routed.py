from typing_extensions import TypeVar, Generic, TypedDict, Protocol
from dataclasses import dataclass
from pipeteer.queues import WriteQueue, ReadQueue, Transaction

T = TypeVar('T')
U = TypeVar('U', contravariant=True)

class QueueAt(Protocol, Generic[U]):
  def __call__(self, url: str, /) -> WriteQueue[U]:
    ...

class Routed(TypedDict, Generic[T]):
  url: str
  value: T

@dataclass
class RoutedQueue(WriteQueue[T], Generic[T]):
  Qurls: ReadQueue[str]
  queue_at: QueueAt

  async def push(self, key: str, value: T):
    url = await self.Qurls.read(key)
    Qout = self.queue_at(url)
    async with Transaction(self.Qurls, Qout, autocommit=True):
      await Qout.push(key, value)
      await self.Qurls.pop(key)