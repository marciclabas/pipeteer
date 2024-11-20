from typing_extensions import TypeVar
from dataclasses import dataclass, field, replace
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.schema import MetaData
from sqlmodel import SQLModel, create_engine, Session

T = TypeVar('T', bound=SQLModel)

@dataclass
class DB:
  engine: Engine
  metadata: MetaData = MetaData()
  tables: dict[str, type] = field(default_factory=dict)
  prefix_: str = ''

  def prefix(self, prefix: str, /):
    return replace(self, prefix_=prefix)

  @classmethod
  def of(cls, db_url: str, /):
    return cls(create_engine(db_url))

  @classmethod
  def at(cls, sqlite_path: str, /):
    import os
    dir = os.path.dirname(sqlite_path)
    if dir:
      os.makedirs(dir, exist_ok=True)
    return cls.of(f'sqlite:///{sqlite_path}')
  
  @property
  def session(self):
    return Session(self.engine)

  def table(self, name: str, type: type[T], /) -> type[T]:
    name = self.prefix_ + name
    # if name in self.tables:
    #   return self.tables[name]
    class _Cls(type, table=True):
      __tablename__ = name # type: ignore
    _Cls.__name__ = type.__name__
    table = _Cls.metadata.tables[name]
    _Cls.metadata._remove_table(name, table.schema)
    self.metadata._add_table(name, table.schema, table)
    try:
      self.metadata.create_all(self.engine, tables=[table])
    except OperationalError:
      ...
    self.metadata._remove_table(name, table.schema)
    self.tables[name] = _Cls

    return _Cls # type: ignore