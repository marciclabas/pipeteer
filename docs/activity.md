# Activity

An `activity` is the simplest pipeline, a 1-to-1 transformation. It can perform side-effects also.

```python
from pipeteer import activity

@activity()
async def double(x: int) -> int:
  return 2*x
```

## Context

Your activity may want to access some external service, or receive some configuration:

```python
from dataclasses import dataclass
from mylib import MyClient
from pipeteer import activity, Context

@dataclass
class MyContext(Context):
  client: MyClient

@activity()
async def fetch_age(username: str, ctx: MyContext) -> int:
  user = await ctx.client.get_user(username)
  return user.age
```

To run it, you need to provide your context instead of the default one:

```python
from pipeteer import DB

db = DB.at('pipe.db')
ctx = MyContext(db, client=MyClient())

fetch_age.run(ctx)
```