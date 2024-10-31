from typing_extensions import TypeVar
from dataclasses import dataclass, field
from fastapi import FastAPI
from pipeteer.queues import http, Queue
from pipeteer.backend import Backend

A = TypeVar('A')

@dataclass
class HttpBackend(Backend):
  base_url: str
  app: FastAPI = field(default_factory=FastAPI)
  id2urls: dict[str, str] = field(default_factory=dict)
  urls2id: dict[str, str] = field(default_factory=dict)
  queues: dict[str, Queue] = field(default_factory=dict)

  @property
  def url(self) -> str:
    return self.base_url.rstrip('/')

  def public_queue(self, id: str, type: type[A]) -> tuple[str, Queue[A]]:
    queue = self.queue(id, type)
    if not id in self.id2urls:
      self.app.mount(f'/queues/{id}', http.queue_api(queue, type))
      url = f'{self.url}/queues/{id}'
      self.id2urls[id] = url
      self.urls2id[url] = id

    return self.id2urls[id], queue
  
  def queue_at(self, url: str, type: type[A]) -> Queue[A]:
    if url in self.urls2id:
      id = self.urls2id[url]
      return self.queue(id, type)
    else:
      return Queue.of(url, type)
  