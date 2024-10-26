from datetime import timedelta
from typing_extensions import AsyncIterable, TypeVar, Generic, Callable, Awaitable, ParamSpec
from dataclasses import dataclass
from urllib.parse import quote_plus
from pydantic import TypeAdapter, ValidationError
try:
  import httpx
except ImportError:
  raise ImportError('Please install `httpx` to use HTTP Queue clients')
from pipeteer.queues import ReadQueue, WriteQueue, QueueError, InfraError

T = TypeVar('T')
Ps = ParamSpec('Ps')

class ClientMixin(Generic[T]):
  def __init__(self, url: str, type: 'type[T]'):
    self.url = url
    self.type = type
    self.adapter = TypeAdapter(type)
    self._client: httpx.AsyncClient | None = None

  @property
  def client(self):
    if self._client is None:
      raise RuntimeError('Please use the client as an async context manager (`async with client: ...`)')
    return self._client
  
  async def __aenter__(self) -> 'ClientMixin[T]':
    self._client = httpx.AsyncClient()
    return self
  
  async def __aexit__(self, *_):
    if self._client:
      await self._client.aclose()
      self._client = None
  
def urljoin(*parts: str) -> str:
  return '/'.join(part.strip('/') for part in parts)

err_adapter = TypeAdapter(QueueError)
@dataclass
class WriteClient(ClientMixin[T], WriteQueue[T], Generic[T]):

  def __init__(self, url: str, type: 'type[T]'):
    super().__init__(url, type)
    self.dump = self.adapter.dump_json
  
  async def push(self, key: str, value: T):
    try:
      r = await self.client.post(
        urljoin(self.url, 'write', quote_plus(key)),
        data=self.dump(value), # type: ignore
        headers={'Content-Type': 'application/json'},
      )

      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error pushing to {self.url}: {r.content}')
        
    except httpx.RequestError as e:
      raise InfraError(f'Error pushing to {self.url}: {e}')
    
@dataclass
class ReadClient(ClientMixin[T], ReadQueue[T], Generic[T]):

  def __init__(self, url: str, type: 'type[T]'):
    super().__init__(url, type)
    self.parse = self.adapter.validate_json
    self.parse_entry = TypeAdapter(tuple[str, self.type]|None).validate_json
  
  async def pop(self, key: str):
    try:
      url = urljoin(self.url, 'read/item', quote_plus(key))
      r = await self.client.delete(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error popping from {self.url}: {r.content}')
        
    except httpx.RequestError as e:
      raise InfraError(f'Error popping from {self.url}: {e}')
    
  
  async def read(self, key: str, /, *, reserve: timedelta | None = None) -> T:
    try:
      url = urljoin(self.url, 'read/item', quote_plus(key))
      params = {'reserve': reserve.total_seconds()} if reserve is not None else {}
      r = await self.client.get(url, params=params)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading from {self.url}: {r.content}')
        
      return self.parse(r.content)
    
    except httpx.RequestError as e:
      raise InfraError(f'Error reading from {self.url}: {e}')
    

  async def read_any(self, *, reserve: timedelta | None = None) -> tuple[str, T] | None:
    try:
      url = urljoin(self.url, 'read/item')
      params = {'reserve': reserve.total_seconds()} if reserve is not None else {}
      r = await self.client.get(url, params=params)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading from {self.url}: {r.content}')
        
      return self.parse_entry(r.content)
    
    except httpx.RequestError as e:
      raise InfraError(f'Error reading from {self.url}: {e}')
    
  
  async def keys(self) -> AsyncIterable[str]:
    try:
      url = urljoin(self.url, 'read/keys')
      r = await self.client.get(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error reading keys from {self.url}: {r.content}')
        
      for key in TypeAdapter(list[str]).validate_json(r.content):
        yield key
    
    except httpx.RequestError as e:
      raise InfraError(f'Error reading keys from {self.url}: {e}')
    
  async def clear(self):
    try:
      url = urljoin(self.url, 'read') + '/'
      r = await self.client.delete(url)
      if r.status_code != 200:
        try:
          raise err_adapter.validate_json(r.content)
        except ValidationError as e:
          raise QueueError(f'Error clearing {self.url}: {r.content}')
        
    except httpx.RequestError as e:
      raise InfraError(f'Error clearing {self.url}: {e}')
    
  def items(self, *, reserve: timedelta | None = None, max: int | None = None) -> AsyncIterable[tuple[str, T]]:
    return super().items(reserve=reserve, max=max)
    

class QueueClient(WriteClient[T], ReadClient[T], Generic[T]):
  def __init__(self, url: str, type: 'type[T]'):
    ReadClient.__init__(self, url, type)
    WriteClient.__init__(self, url, type)
    