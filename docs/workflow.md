# Workflow

A `workflow` is the go-to way to compose pipelines in code.

## Purity

It's **very important** that your workflow function is pure. The function will be run many times: think of it as a *way to describe the pipeline*, not to run it.

But don't worry. If it's not pure, `pipeteer` will complain (at runtime).


## Example

```python
import random
from pipeteer import workflow, activity, WorkflowContext

@activity()
async def impure_activity(p: float) -> bool:
  return random.random() < p

@workflow()
async def greet(name: str, ctx: WorkflowContext) -> str:
  hello = await ctx.call(impure_activity, 0.5)
  if hello:
    return f'Hello, {name}!'
  else:
    return f'Goodbye, {name}!'
```

Note that, assuming `ctx.call` returns the same result, then `greet` is pure.

That's what allows us to restart the pipeline after an outage, or a week later, and go on as if nothing had happened.