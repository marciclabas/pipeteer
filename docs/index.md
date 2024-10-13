# Welcome to Pipeteer

Pipeteer **simplifies the complexity of durable execution** whilst *not hiding the underlying persistence*.

## A simple example

### Workflow Definition

```python
# workflow.py
from pipeteer import task, workflow, Context, Proxy

@task()
async def double(x: int):
  return 2*x

@task
async def inc(x: int):
  return x+1

@workflow([double, inc])
async def linear(ctx: Context, x: int):
  x2 = await ctx.call(double, x)
  return await ctx.call(inc, x2)

if __name__ == '__main__':
  ctx = Context.sqlite('workflow.db')
  linear.run(ctx)
```

### Workflow Execution

```python
# main.py
import asyncio
from pipeteer import Context
from workflow import Linear

if __name__ == '__main__':
  wkf = Linear()
  ctx = Context.sqlite('workflow.db')
  input_queue = wkf.input(ctx)
  output_queue = ctx.output

  async def pusher():
    for i in range(100000):
      await input_queue.push(f'id-{i}', i)
      await asyncio.sleep(1)

  async def puller():
    async for key, value in output_queue:
      print(f'{key}: {value}')
      await output_queue.pop(key)

  asyncio.run(asyncio.gather(pusher(), puller()))
```

The magic is very thin, though. Let's see how it all works under the hood, using [queues](queues.md)