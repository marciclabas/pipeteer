from typing_extensions import TypeVar, Generic, Self, Any, AsyncIterable
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace, KW_ONLY
from datetime import datetime
from functools import cached_property
from sqlmodel import SQLModel, Field, select
from pydantic import TypeAdapter
from sqltypes import ValidatedJSON
from dslog import Logger
from pipeteer.backend import DB, ZMQ

AnyT: type = Any # type: ignore
A = TypeVar('A')
B = TypeVar('B')
Artifact = TypeVar('Artifact', covariant=True)

@dataclass
class Context:
  db: DB
  zmq: ZMQ
  _: KW_ONLY
  log: Logger = field(default_factory=Logger.click)

  async def wait(self, topic: str, /):
    await self.zmq.sub(topic).wait()

  @classmethod
  def of(
    cls, db: DB, *, log: Logger = Logger.click(),
    pub_url: str = 'tcp://localhost:5555',
    sub_url: str = 'tcp://localhost:5556',
  ):
    return cls(db, ZMQ(pub_url=pub_url, sub_url=sub_url), log=log)

  def prefix(self, prefix: str) -> Self:
    return replace(self, log=self.log.prefix(prefix))
  
Ctx = TypeVar('Ctx', bound=Context)


class EntryT(SQLModel, Generic[A]):
  key: str = Field(primary_key=True)
  value: A
  
  def __class_getitem__(cls, type) -> type: # apparently SQLModel messes generics up
    cls = super().__class_getitem__(type)
    cls.__args__ = (type,) # type: ignore
    return cls # type: ignore

def Entry(type: type[A]) -> type[EntryT[A]]:
  class Entry(EntryT[type]):
    value: type = Field(sa_type=ValidatedJSON(type, name='JSON'))
  return Entry

class InputT(EntryT[A], Generic[A]):
  ttl: datetime | None = None
  output: str = 'output'


def Input(type: type[A]) -> type[InputT[A]]:
  class Input(InputT[type]):
    value: type = Field(sa_type=ValidatedJSON(type, name='JSON'))
  return Input

@dataclass
class TypesMixin(Generic[A, B]):
  Tin: type[A]
  Tout: type[B]

  @cached_property
  def input_adapter(self):
    return TypeAdapter(self.Tin)
  
  @cached_property
  def output_adapter(self):
    return TypeAdapter(self.Tout)

@dataclass
class Inputtable(TypesMixin[A, B], Generic[A, B]):
  id: str

  def input(self, ctx: Context) -> type[InputT[A]]:
    return ctx.db.table(self.id, Input(self.Tin)) # type: ignore
  
  def output(self, ctx: Context, table: str = 'output') -> type[EntryT[B]]:
    return ctx.db.table(table, Entry(self.Tout))
  
  async def notify(self, ctx: Context):
    await ctx.zmq.pub.send(self.id)

  async def items(self, ctx: Context) -> AsyncIterable[InputT[A]]:
    Inp = self.input(ctx)
    with ctx.db.session as s:
      for it in s.exec(select(Inp)):
        yield it
  

@dataclass
class Runnable(ABC, TypesMixin[A, B], Generic[A, B, Ctx, Artifact]):
  id: str
  
  @abstractmethod
  def run(self, ctx: Ctx, /) -> Artifact:
    ...

class Pipeline(Runnable[A, B, Ctx, Artifact], Inputtable[A, B], Generic[A, B, Ctx, Artifact]):
  ...