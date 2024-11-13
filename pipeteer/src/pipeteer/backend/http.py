from typing_extensions import TypeVar, Any
from dataclasses import dataclass, field
from fastapi import FastAPI, Request
from pipeteer.queues import http, Queue, Routed
from pipeteer import Backend, Runnable, Inputtable, Observable, Context

A = TypeVar('A')
Ctx = TypeVar('Ctx', bound=Context)
AnyT: type = Any # type: ignore

@dataclass
class HttpBackend(Backend):
  base_url: str
  token: str | None = None
  app: FastAPI = field(default_factory=FastAPI)
  id2urls: dict[str, str] = field(default_factory=dict)
  urls2id: dict[str, str] = field(default_factory=dict)
  queues: dict[str, Queue] = field(default_factory=dict)
  pipelines: set[str] = field(default_factory=set)
  mounted: bool = False

  def __post_init__(self):
    if self.token is not None:
      @self.app.middleware('http')
      async def check_token(request: Request, call_next):
        if request.headers.get('Authorization') != f'Bearer {self.token}':
          return 'Unauthorized', 401
        return await call_next(request)

  @property
  def url(self) -> str:
    return self.base_url.rstrip('/')

  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    queue = self.queue(id, type)
    if not id in self.id2urls:
      self.app.mount(f'/callbacks/{id}', http.queue_api(queue, type))
      url = f'{self.url}/callbacks/{id}'
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
    
    @self.app.get(f'/pipelines')
    def list_pipelines():
      return {
        id: f'{self.url}/pipelines/{id}'
        for id in self.pipelines
      }
    
    self.pipelines.add(pipeline.id)

    urls = {}
    
    if isinstance(pipeline, Observable):
      urls['queues'] = f'{self.url}/pipelines/{pipeline.id}/queues'
      queues = pipeline.observe(ctx)
      for name, queue in queues.items():
        self.app.mount(f'/pipelines/{pipeline.id}/queues/{name}', http.queue_api(queue, AnyT))

      @self.app.get(f'/pipelines/{pipeline.id}/queues')
      def observe():
        return {
          name: f'{self.url}/pipelines/{pipeline.id}/queues/{name}'
          for name in queues
        }

    if isinstance(pipeline, Inputtable):
      urls['input'] = f'{self.url}/pipelines/{pipeline.id}/input/write'
      self.app.mount(f'/pipelines/{pipeline.id}/input/write', http.write_api(pipeline.input(ctx), Routed[pipeline.Tin]))

    @self.app.get('/pipelines/{id}')
    def list_pipeline(id: str):
      return urls
    