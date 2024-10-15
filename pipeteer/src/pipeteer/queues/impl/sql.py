from typing_extensions import AsyncIterable, TypeVar, Generic, ParamSpec, \
  Protocol, Awaitable, Callable, Coroutine, Any
from functools import wraps
from datetime import timedelta, datetime
from pydantic import RootModel, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlmodel import select, text, or_
from sqlmodel.ext.asyncio.session import AsyncSession
from pipeteer.queues import Queue, ListQueue, QueueError, InfraError, InexistentItem

T = TypeVar('T')
U = TypeVar('U')
Ps = ParamSpec('Ps')

class SessionFn(Protocol, Generic[Ps, U]): # type: ignore
  def __call__(self, s: AsyncSession, /, *args: Ps.args, **kwargs: Ps.kwargs) -> U:
    ...

def wrap_exceptions(fn: Callable[Ps, Coroutine[Any, Any, T]]) -> Callable[Ps, Coroutine[Any, Any, T]]:
  @wraps(fn)
  async def wrapped(*args: Ps.args, **kwargs: Ps.kwargs) -> T:
    try:
      return await fn(*args, **kwargs)
    except DatabaseError as e:
      raise InfraError(e) from e
  
  return wrapped

exec_options = {}
# exec_options = {'isolation_level': 'SERIALIZABLE', 'no_cache': True}

class SqlQueue(Queue[T], Generic[T]):

  @classmethod
  def new(cls, type: type[T], url: str, *, table: str, echo: bool = False) -> 'SqlQueue[T]':
    engine = create_async_engine(url, echo=echo)
    return cls(type, engine, table=table)

  def __init__(self, type: type[T], engine: AsyncEngine, *, table: str):
    self.engine = engine
    self.table = table
    self.session: AsyncSession | None = None

    class Base(DeclarativeBase):
      ...

    json_type = JSONB if self.engine.dialect.name == 'postgresql' else JSON
    class Table(Base):
      __tablename__ = table
      key: Mapped[str] = mapped_column(primary_key=True)
      value: Mapped[RootModel[type]] = mapped_column(type_=json_type) # type: ignore
      ttl: Mapped[datetime|None] = mapped_column(default=None)

    self.Table = Table
    self.metadata = Base.metadata
    self.initialized = False

  async def initialize(self):
    if not self.initialized:
      async with self.engine.begin() as conn:
        await conn.run_sync(self.metadata.create_all)
        if self.engine.dialect.name == 'sqlite':
          await conn.execute(text("PRAGMA journal_mode=WAL"))
          await conn.execute(text("PRAGMA busy_timeout=5000"))
      self.initialized = True

  def __repr__(self):
    return f'SqlQueue(engine={self.engine!r}, table={self.Table.__tablename__!r})'
  
  async def with_session(self, f: SessionFn[Ps, Awaitable[U]], *args: Ps.args, **kwargs: Ps.kwargs) -> U:
    """Generates a session on-the-fly if executing without a transaction"""
    try:
      await self.initialize()
      if self.session is None:
        async with AsyncSession(self.engine) as s:
          return await f(s, *args, **kwargs)
      else:
        return await f(self.session, *args, **kwargs)
    except DatabaseError as e:
      raise InfraError(e) from e
  
  async def with_autocommit(self, f: SessionFn[Ps, Awaitable[U]], *args: Ps.args, **kwargs: Ps.kwargs) -> U:
    """Generates a session on-the-fly if executing without a transaction. Autocommits at the end"""
    try:
      await self.initialize()
      if self.session is None:
        async with AsyncSession(self.engine) as s:
          out = await f(s, *args, **kwargs)
          await s.commit()
          return out
      else:
        return await f(self.session, *args, **kwargs)
    except DatabaseError as e:
      raise InfraError(e) from e
  
  async def _push(self, s: AsyncSession, key: str, value: T):
    stmt = select(self.Table).where(self.Table.key == key)
    row = (await s.exec(stmt)).first()
    if row is not None:
      await s.delete(row)
    s.add(self.Table(key=key, value=value, ttl=datetime.now()))

  async def push(self, key: str, value: T):
    return await self.with_autocommit(self._push, key, value)
    
  async def _pop(self, s: AsyncSession, key: str):
    stmt = select(self.Table).where(self.Table.key == key)
    row = (await s.exec(stmt)).first()
    if row is None:
      raise InexistentItem(key)
    
    await s.delete(row)

  async def pop(self, key: str):
    return await self.with_autocommit(self._pop, key)

  async def _read(self, s: AsyncSession, key: str, /, *, reserve: timedelta | None = None) -> T:
    stmt = select(self.Table).where(
      self.Table.key == key, 
      or_(self.Table.ttl < datetime.now(), self.Table.ttl == None)
    )
    if reserve is not None:
      stmt = stmt.with_for_update(nowait=True, skip_locked=True)

    row = (await s.exec(stmt, execution_options=exec_options if reserve else {})).first()
    if row:
      if reserve is not None:
        row.ttl = datetime.now() + reserve
        s.add(row)
        await s.commit()
      return row.value # type: ignore
    
    raise InexistentItem(key)
    
  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> T:
    return await self.with_session(self._read, key, reserve=reserve)
  
  async def _read_any(self, s: AsyncSession, *, reserve: timedelta | None = None) -> tuple[str, T] | None:
    stmt = select(self.Table).where(
      or_(self.Table.ttl < datetime.now(), self.Table.ttl == None)
    ).limit(1)
    if reserve is not None:
      stmt = stmt.with_for_update(skip_locked=True)
    
    row = (await s.exec(stmt, execution_options=exec_options if reserve else {})).first()
    if row:
      k, v = row.key, row.value
      if reserve is not None:
        row.ttl = datetime.now() + reserve
        s.add(row)
        await s.commit()
      
      return k, v # type: ignore
    
  async def read_any(self, *, reserve: timedelta | None = None) -> tuple[str, T] | None:
    return await self.with_session(self._read_any, reserve=reserve)
    
  async def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, T]]: 
    await self.initialize()
    try:
      async with AsyncSession(self.engine) as s:
        stmt = select(self.Table).where(
          or_(self.Table.ttl < datetime.now(), self.Table.ttl == None)
        ).limit(max)
        if reserve is not None:
          stmt = stmt.with_for_update(skip_locked=True)

        result = await s.exec(stmt, execution_options=exec_options if reserve else {})
        for row in result:
          k, v = row.key, row.value
          if reserve is not None:
            row.ttl = datetime.now() + reserve
            s.add(row)
            
          yield k, v # type: ignore
        
        if reserve is not None:
          await s.commit()
          
    except DatabaseError as e:
      raise InfraError(e) from e
  
  @wrap_exceptions
  async def enter(self, other=None):
    if isinstance(other, SqlQueue) and other.engine.url == self.engine.url:
      self.session = other.session
    else:
      self.session = await AsyncSession(self.engine).__aenter__()

  @wrap_exceptions
  async def commit(self, other=None):
    if not self.session:
      raise QueueError('No transaction to commit')
    
    if not isinstance(other, SqlQueue) or other.engine.url != self.engine.url:
      await self.session.commit()

  @wrap_exceptions
  async def close(self, other=None):
    if self.session and (not isinstance(other, SqlQueue) or other.engine.url != self.engine.url):
      await self.session.close()
    
  @wrap_exceptions
  async def rollback(self, other=None):
    if not self.session:
      raise QueueError('No transaction to rollback')
    
    if not isinstance(other, SqlQueue) or other.engine.url != self.engine.url:
      await self.session.rollback()
    
adapter = TypeAdapter(Any)

class ListSqlQueue(ListQueue[T], SqlQueue[list[T]], Generic[T]):
  async def _append(self, s: AsyncSession, key: str, value: T):
    single = adapter.dump_json([value]).decode()
    obj = adapter.dump_json(value).decode()
    if s.bind.dialect.name == 'postgresql':
      stmt = f'''
        INSERT INTO "{self.table}" (key, value)
          VALUES (:key, jsonb(:single))
          ON CONFLICT (key)
          DO UPDATE SET 
            value = "{self.table}".value || jsonb(:obj)
      '''
    else:
      stmt = f'''
        INSERT INTO "{self.table}" (key, value)
          VALUES (:key, json(:single))
          ON CONFLICT(key)
          DO UPDATE SET 
            value = json_insert(value, '$[#]', json(:obj))
      '''
    stmt = text(stmt).bindparams(key=key, single=single, obj=obj)
    await s.execute(stmt)

  async def append(self, key: str, value: T):
    return await self.with_autocommit(self._append, key, value)
    