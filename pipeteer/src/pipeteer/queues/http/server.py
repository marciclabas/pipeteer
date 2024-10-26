from typing_extensions import TypeVar
from pydantic import TypeAdapter, ValidationError
from fastapi import FastAPI, Request, Response, Body
from fastapi.exceptions import RequestValidationError
from pipeteer.queues import ReadQueue, WriteQueue, Queue, QueueError, InexistentItem

T = TypeVar('T')

def write_api(queue: WriteQueue[T]) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)

  parse = TypeAdapter(queue.type).validate_json

  @app.post('/{key}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
  async def push(key: str, req: Request, res: Response):
    try:
      value = parse(await req.body())
      await queue.push(key, value)
    except ValidationError as e:
      raise RequestValidationError(e.errors())
    except InexistentItem as e:
      res.status_code = 404
      return e
    except QueueError as e:
      res.status_code = 500
      return e
    
  return app


def read_api(queue: ReadQueue[T]) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)

  @app.delete('/item/{key}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
  async def pop(key: str, r: Response):
    try:
      return await queue.pop(key)
    except InexistentItem as e:
      r.status_code = 404
      return e
    except QueueError as e:
      r.status_code = 500
      return e
    
  @app.get('/item', responses={500: {'model': QueueError}})
  async def read_any(r: Response) -> tuple[str, T] | None:
    try:
      return await queue.read_any()
    except QueueError as e:
      r.status_code = 500
      return e # type: ignore
    
  @app.get('/item/{key}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
  async def read(key: str, r: Response) -> T:
    try:
      return await queue.read(key)
    except InexistentItem as e:
      r.status_code = 404
      return e # type: ignore
    except QueueError as e:
      r.status_code = 500
      return e # type: ignore
    
  @app.get('/keys', responses={500: {'model': QueueError}})
  async def keys(r: Response) -> list[str]:
    try:
      return [key async for key in queue.keys()]
    except QueueError as e:
      r.status_code = 500
      return e # type: ignore
    
  @app.delete('/', responses={500: {'model': QueueError}})
  async def clear(r: Response):
    try:
      await queue.clear()
    except QueueError as e:
      r.status_code = 500
      return e # type: ignore

  return app


def queue_api(queue: Queue[T]) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)
  app.mount('/write', write_api(queue))
  app.mount('/read', read_api(queue))
  return app