from typing_extensions import AsyncIterable, TypeVar, Generic, ParamSpec, \
  Awaitable, Callable, Coroutine, Any
from functools import wraps, cached_property
from datetime import timedelta, datetime
from pydantic import RootModel, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.expression import func
from sqltypes import ValidatedJSON
from sqlmodel import select, text, or_
from sqlmodel.ext.asyncio.session import AsyncSession
from pipeteer.queues import Queue, ListQueue, QueueError, InfraError, InexistentItem
from pipeteer.util import type_arg

T = TypeVar('T')
U = TypeVar('U')
Ps = ParamSpec('Ps')

SessionFn = Callable[[AsyncSession], U]

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

  @staticmethod
  def new(type: type[U], url: str, *, table: str, echo: bool = False) -> 'SqlQueue[U]':
    engine = create_async_engine(url, echo=echo)
    return SqlQueue(type, engine, table=table)

  def __init__(self, type: type[T], engine: AsyncEngine, *, table: str):
    self.engine = engine
    self.table = table
    self.session: AsyncSession | None = None
    self.type = type

    class Base(DeclarativeBase):
      ...

    class Table(Base):
      __tablename__ = table
      key: Mapped[str] = mapped_column(primary_key=True)
      value: Mapped[RootModel[Any]] = mapped_column(type_=ValidatedJSON(type, name='JSON')) # type: ignore
      ttl: Mapped[datetime|None] = mapped_column(default=None)

    self.Table = Table
    self.metadata = Base.metadata
    self.initialized = False

  async def initialize(self):
    if not self.initialized:
      try:
        async with self.engine.begin() as conn:
          await conn.run_sync(self.metadata.create_all)
          if self.engine.dialect.name == 'sqlite':
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA busy_timeout=200"))
        self.initialized = True
      except DatabaseError:
        ...

  def __repr__(self):
    return f'SqlQueue(engine={self.engine!r}, table={self.Table.__tablename__!r})'
  
  async def with_session(self, f: SessionFn[Awaitable[U]]) -> U:
    """Generates a session on-the-fly if executing without a transaction"""
    try:
      await self.initialize()
      if self.session is None:
        async with AsyncSession(self.engine) as s:
          return await f(s)
      else:
        return await f(self.session)
    except DatabaseError as e:
      raise InfraError(e) from e
  
  async def with_autocommit(self, f: SessionFn[Awaitable[U]]) -> U:
    """Generates a session on-the-fly if executing without a transaction. Autocommits at the end"""
    try:
      await self.initialize()
      if self.session is None:
        async with AsyncSession(self.engine) as s:
          out = await f(s)
          await s.commit()
          return out
      else:
        return await f(self.session)
    except DatabaseError as e:
      raise InfraError(e) from e
  

  async def push(self, key: str, value: T):
    async def _push(s: AsyncSession):
      stmt = select(self.Table).where(self.Table.key == key)
      row = (await s.exec(stmt)).first()
      if row is not None:
        await s.delete(row)
      s.add(self.Table(key=key, value=value, ttl=datetime.now()))
    
    return await self.with_autocommit(_push)
    

  async def pop(self, key: str):
    async def _pop(s: AsyncSession):
      stmt = select(self.Table).where(self.Table.key == key)
      row = (await s.exec(stmt)).first()
      if row is None:
        raise InexistentItem(key)
      
      await s.delete(row)

    return await self.with_autocommit(_pop)


  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> T:
    async def _read(s: AsyncSession) -> T:
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
  
    return await self.with_session(_read)
  
  
  async def read_any(self, *, reserve: timedelta | None = None) -> tuple[str, T] | None:
    async def _read_any(s: AsyncSession) -> tuple[str, T] | None:
      stmt = select(self.Table).where(
        or_(self.Table.ttl < datetime.now(), self.Table.ttl == None)
      ).limit(1).order_by(func.random())
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
      
    return await self.with_session(_read_any)
    
  async def has(self, key: str, /, *, reserve: timedelta | None = None) -> bool:
    async def _has(s: AsyncSession) -> bool:
      stmt = select(self.Table).where(
        self.Table.key == key, 
        or_(self.Table.ttl < datetime.now(), self.Table.ttl == None)
      )
      if reserve is not None:
        stmt = stmt.with_for_update(nowait=True, skip_locked=True)

      row = (await s.exec(stmt, execution_options=exec_options if reserve else {})).first()
      if row and reserve is not None:
        row.ttl = datetime.now() + reserve
        s.add(row)
        await s.commit()
      
      return row is not None
    
    return await self.with_session(_has)

    
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
    
  async def _clear(self, s: AsyncSession):
    await s.execute(text(f'DELETE FROM "{self.table}"'))
    
  async def clear(self):
    return await self.with_autocommit(self._clear)
  
  @wrap_exceptions
  async def enter(self, other=None):
    if isinstance(other, SqlQueue) and other.engine.url == self.engine.url:
      self.session = other.session
    elif self.session is None:
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
    
class ListSqlQueue(ListQueue[T], SqlQueue[list[T]], Generic[T]):

  @cached_property
  def adapter(self):
    return TypeAdapter(self.type)
  
  @cached_property
  def single_adapter(self):
    return TypeAdapter(type_arg(self.type))

  async def append(self, key: str, value: T):
    async def _append(s: AsyncSession):
      single = self.adapter.dump_json([value]).decode()
      obj = self.single_adapter.dump_json(value).decode()
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

    return await self.with_autocommit(_append)
    