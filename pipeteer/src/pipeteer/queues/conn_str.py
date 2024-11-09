from typing_extensions import TypeVar
from urllib.parse import urlparse, parse_qs, unquote
from pydantic import BaseModel
from pipeteer.queues import Queue, http

T = TypeVar('T')

class SqlParams(BaseModel):
  table: str

class HttpParams(BaseModel):
  secret: str | None = None

def parse(url: str, type: type[T]) -> Queue[T]:

  parsed_url = urlparse(url) # 'file://path/to/base?prefix=hello'
  scheme = parsed_url.scheme # 'file'
  netloc = parsed_url.netloc # 'path'
  path = unquote(parsed_url.path) # '/to/base'
  endpoint = netloc + path # 'path/to/base'
  query = parse_qs(parsed_url.query) # { 'prefix': ['hello'] }
  query = { k: v[0] for k, v in query.items() }

  if scheme in ('http', 'https'):
    params = HttpParams(**query)
    url = f'{scheme}://{endpoint}'
    return http.QueueClient(url, type=type, secret=params.secret)
  
  elif scheme.startswith('sql+'):
    params = SqlParams(**query)
    from pipeteer.queues import SqlQueue
    proto = scheme.removeprefix('sql+')
    return SqlQueue.new(type, url=f'{proto}://{endpoint}', table=params.table)
  
  else:
    raise ValueError(f'Unknown scheme: {scheme}')