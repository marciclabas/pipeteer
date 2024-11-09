from typing_extensions import TypeVar
from datetime import datetime
import jwt
from pydantic import TypeAdapter, ValidationError
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from pipeteer.queues import ReadQueue, WriteQueue, Queue, QueueError, InexistentItem

T = TypeVar('T')

def verify_token(*, token: str, secret: str, now: datetime | None = None) -> bool:
  now = now or datetime.now()
  try:
    exp = jwt.decode(token, secret, algorithms=['HS256'], options={'verify_exp': False}).get('exp')
    return exp is None or now < datetime.fromtimestamp(exp)
  except jwt.PyJWTError:
    return False
  
def token_middleware(secret: str):
  async def middleware(req: Request, call_next):
    token = req.query_params.get('token')
    if token and verify_token(token=token, secret=secret):
      return await call_next(req)
    else:
      return Response(status_code=401, content='Unauthorized')
  return middleware

def write_api(queue: WriteQueue[T], type: type[T], *, secret: str | None = None) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)
  if secret:
    app.middleware('http')(token_middleware(secret))

  parse = TypeAdapter(type).validate_json

  @app.post('/{key:path}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
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


def read_api(queue: ReadQueue[T], *, secret: str | None = None) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)
  if secret:
    app.middleware('http')(token_middleware(secret))

  @app.delete('/item/{key:path}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
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
    
  @app.get('/item/{key:path}', responses={500: {'model': QueueError}, 404: {'model': InexistentItem}})
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


def queue_api(queue: Queue[T], type: type[T], *, secret: str | None = None) -> FastAPI:
  app = FastAPI(generate_unique_id_function=lambda route: route.name)
  app.mount('/write', write_api(queue, type))
  app.mount('/read', read_api(queue))
  if secret:
    app.middleware('http')(token_middleware(secret))
  return app