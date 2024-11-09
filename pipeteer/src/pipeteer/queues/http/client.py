from datetime import timedelta
from typing_extensions import AsyncIterable, TypeVar, Generic, ParamSpec, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import jwt
from pydantic import TypeAdapter, ValidationError
try:
  import httpx
except ImportError:
  raise ImportError('Please install `httpx` to use HTTP Queue clients')
from pipeteer.queues import Queue, ReadQueue, WriteQueue, QueueError, InfraError, Transactional

T = TypeVar('T')
U = TypeVar('U')
Ps = ParamSpec('Ps')

def sign_token(secret: str, expiry: datetime | None = None) -> str:
  payload = {} if expiry is None else {'exp': expiry.timestamp()}
  return jwt.encode(payload, secret, algorithm='HS256')

class ClientMixin(Transactional, Generic[T]):
  def __init__(
    self, url: str, *, type: 'type[T]',
    headers: dict[str, str] = {}, secret: str | None = None
  ):
    self.url = url
    self.type = type
    self.adapter = TypeAdapter(type)
    self.client: httpx.AsyncClient | None = None
    self.headers = headers
    self.secret = secret

  def params(self) -> dict[str, str] | None:
    if self.secret:
      return {'token': sign_token(self.secret, datetime.now() + timedelta(minutes=5))}
  
  async def enter(self, other=None):
    self.client = httpx.AsyncClient(headers=self.headers)
  
  async def close(self, other=None):
    if self.client:
      await self.client.aclose()
      self.client = None

  async def with_client(self, f: Callable[[httpx.AsyncClient], Awaitable[U]]) -> U:
    try:
      if self.client is None:
        async with httpx.AsyncClient(headers=self.headers, params=self.params()) as client:
          return await f(client)
      else:
        return await f(self.client)
    except httpx.RequestError as e:
      raise InfraError(e) from e
    
  async def with_client_iter(self, f: Callable[[httpx.AsyncClient], AsyncIterable[U]]) -> AsyncIterable[U]:
    try:
      if self.client is None:
        async with httpx.AsyncClient(headers=self.headers, params=self.params()) as client:
          async for item in f(client):
            yield item
      else:
        async for item in f(self.client):
          yield item
    except httpx.RequestError as e:
      raise InfraError(e) from e

def urljoin(*parts: str) -> str:
  return '/'.join(part.strip('/') for part in parts)

err_adapter = TypeAdapter(QueueError)
@dataclass
class WriteClient(ClientMixin[T], WriteQueue[T], Generic[T]):

  def __init__(self, url: str, type: 'type[T]', headers: dict[str, str] = {}, secret: str | None = None):
    super().__init__(url, type=type, headers=headers, secret=secret)
    self.dump = self.adapter.dump_json

  async def push(self, key: str, value: T):
    async def _push(client: httpx.AsyncClient):
      url = urljoin(self.url, 'write', key)
      r = await client.post(
        url,
        data=self.dump(value), # type: ignore
        headers=self.headers,
      )
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError:
          raise QueueError(f'Error pushing to {self.url}: {r.content}. Key: {key}, Value: {value}')
        
    return await self.with_client(_push)
    
@dataclass
class ReadClient(ClientMixin[T], ReadQueue[T], Generic[T]):

  def __init__(self, url: str, *, type: 'type[T]', headers: dict[str, str] = {}, secret: str | None = None):
    super().__init__(url, type=type, headers=headers, secret=secret)
    self.parse = self.adapter.validate_json
    self.parse_entry = TypeAdapter(tuple[str, self.type]|None).validate_json
  
  async def pop(self, key: str):
    async def _pop(client: httpx.AsyncClient):
      url = urljoin(self.url, 'read/item', key)
      r = await client.delete(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error popping from {self.url}: {r.content}')
        
    return await self.with_client(_pop)
    
  
  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> T:
    async def _read(client: httpx.AsyncClient):
      url = urljoin(self.url, 'read/item', key)
      params = {'reserve': reserve.total_seconds()} if reserve is not None else {}
      r = await client.get(url, params=params)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading from {self.url}: {r.content}')
        
      return self.parse(r.content)
    
    return await self.with_client(_read)

  async def read_any(self, *, reserve: timedelta | None = None) -> tuple[str, T] | None:
    async def _ready_any(client: httpx.AsyncClient):
      url = urljoin(self.url, 'read/item')
      params = {'reserve': reserve.total_seconds()} if reserve is not None else {}
      r = await client.get(url, params=params)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading from {self.url}: {r.content}')
        
      return self.parse_entry(r.content)
    
    return await self.with_client(_ready_any)
  
  def keys(self) -> AsyncIterable[str]:
    async def _keys(client: httpx.AsyncClient):
      url = urljoin(self.url, 'read/keys')
      r = await client.get(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading keys from {self.url}: {r.content}')
        
      for key in TypeAdapter(list[str]).validate_json(r.content):
        yield key

    return self.with_client_iter(_keys)
    
  async def clear(self):
    async def _clear(client: httpx.AsyncClient):
      url = urljoin(self.url, 'read') + '/'
      r = await client.delete(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error clearing {self.url}: {r.content}')
    
    return await self.with_client(_clear)

  def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, T]]:
    return super().items(reserve=reserve, max=max)
    

class QueueClient(Queue[T], WriteClient[T], ReadClient[T], Generic[T]):
  def __init__(self, url: str, *, type: 'type[T]', headers: dict[str, str] = {}, secret: str | None = None):
    ReadClient.__init__(self, url, type=type)
    WriteClient.__init__(self, url, type, headers=headers, secret=secret)
    