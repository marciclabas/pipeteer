from typing_extensions import TypeVar, Any
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from fastapi import FastAPI
from pipeteer.queues import Queue, Routed
from pipeteer import Backend, Runnable, Inputtable, Observable, Context, http

A = TypeVar('A')
Ctx = TypeVar('Ctx', bound=Context)
AnyT: type = Any # type: ignore

@dataclass
class HttpBackend(Backend):
  public_url: str
  callback_url: str
  secret: str | None = None
  app: FastAPI = field(default_factory=FastAPI)
  id2urls: dict[str, str] = field(default_factory=dict)
  urls2id: dict[str, str] = field(default_factory=dict)
  queues: dict[str, Queue] = field(default_factory=dict)
  pipelines: set[str] = field(default_factory=set)
  mounted: bool = False
  expires_in: timedelta | None = None

  @property
  def expiry(self) -> datetime | None:
    if self.expires_in is not None:
      return datetime.now() + self.expires_in

  def __post_init__(self):
    if self.secret is not None:
      self.app.middleware('http')(http.token_middleware(self.secret, exclude_paths=['/auth']))

  @property
  def url(self) -> str:
    return self.public_url.rstrip('/')
  
  @property
  def cb_url(self) -> str:
    return self.callback_url.rstrip('/')
  
  def query(self, expiry: datetime | None = None) -> str:
    return f'?token={http.sign_token(self.secret, expiry=expiry)}' if self.secret is not None else ''

  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    queue = self.queue(id, type)
    if not id in self.id2urls:
      self.app.mount(f'/callbacks/{id}', http.queue_api(queue, type))
      url = f'{self.cb_url}/callbacks/{id}' + self.query()
      self.id2urls[id] = url
      self.urls2id[url] = id

    return self.id2urls[id], queue
  
  def queue_at(self, url: str, type: type[A]) -> Queue[A]:
    if url in self.urls2id:
      id = self.urls2id[url]
      return self.queue(id, type)
    else:
      return Queue.of(url, type)

  
  def mount(self, pipeline: Runnable[Any, Any, Ctx, Any] | Inputtable[Any, Any, Ctx], ctx: Ctx):
    
    @self.app.get('/auth')
    def authorize(secret: str, expires_in_secs: int | None = 60*60):
      if secret == self.secret:
        self.expires_in = timedelta(seconds=expires_in_secs) if expires_in_secs is not None else None
        token = http.sign_token(secret)
        return {
          'token': token,
          'pipelines': f'{self.url}/pipelines' + self.query(expiry=self.expiry),
        }

    @self.app.get('/pipelines')
    def list_pipelines():
      return {
        id: f'{self.url}/pipelines/{id}' + self.query(expiry=self.expiry)
        for id in self.pipelines
      }
    
    self.pipelines.add(pipeline.id)

    urls = {}
    
    if isinstance(pipeline, Observable):
      urls['queues'] = f'{self.url}/pipelines/{pipeline.id}/queues' + self.query(expiry=self.expiry)
      queues = pipeline.observe(ctx)
      for name, queue in queues.items():
        self.app.mount(f'/pipelines/{pipeline.id}/queues/{name}', http.queue_api(queue, AnyT))

      @self.app.get(f'/pipelines/{pipeline.id}/queues')
      def observe():
        return {
          name: f'{self.url}/pipelines/{pipeline.id}/queues/{name}' + self.query(expiry=self.expiry)
          for name in queues
        }

    if isinstance(pipeline, Inputtable):
      urls['input'] = f'{self.url}/pipelines/{pipeline.id}/input/write' + self.query(expiry=self.expiry)
      self.app.mount(f'/pipelines/{pipeline.id}/input/write', http.write_api(pipeline.input(ctx), Routed[pipeline.Tin]))

    @self.app.get('/pipelines/{id}')
    def list_pipeline(id: str):
      return urls
    