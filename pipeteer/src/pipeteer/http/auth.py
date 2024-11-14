from datetime import datetime
import jwt

def sign_token(secret: str, expiry: datetime | None = None) -> str:
  payload = {} if expiry is None else {'exp': expiry.timestamp()}
  return jwt.encode(payload, secret, algorithm='HS256')

def verify_token(*, token: str, secret: str, now: datetime | None = None) -> bool:
  now = now or datetime.now()
  try:
    exp = jwt.decode(token, secret, algorithms=['HS256'], options={'verify_exp': False}).get('exp')
    return exp is None or now < datetime.fromtimestamp(exp)
  except jwt.PyJWTError:
    return False
  
def token_middleware(secret: str, *, exclude_paths: list[str] = []):
  from fastapi import Request, Response
  async def middleware(req: Request, call_next):
    excluded = any(req.url.path.startswith(path) for path in exclude_paths)
    if excluded or (token := req.query_params.get('token')) and verify_token(token=token, secret=secret):
      return await call_next(req)
    else:
      return Response(status_code=401, content='Unauthorized')
  return middleware