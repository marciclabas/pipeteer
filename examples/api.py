from multiprocessing import Process
import asyncio
from fastapi import FastAPI
from sqlmodel import Session, select
from pipeteer import task, DB, Context, InputT, Push, workflow, WorkflowContext

@task()
def api(Inp: type[InputT[str]], push: Push[str], ctx: Context):

  app = FastAPI()

  @app.get('/items')
  async def items() -> list[InputT[str]]:
    with Session(ctx.db.engine) as s:
      return list(s.exec(select(Inp)).all())
    
  @app.get('/items/{key}')
  async def item(key: str) -> InputT[str] | None:
    with Session(ctx.db.engine) as s:
      return s.get(Inp, key)

  @app.get('/items/{key}/push')
  async def output(key: str):
    await push(key, 'value')

  return app

@workflow()
async def wkf(inp: str, ctx: WorkflowContext) -> str:
  x = inp
  for i in range(4):
    x = await ctx.call(api, x)
  return x


if __name__ == '__main__':
  import uvicorn
  
  db = DB.at('api.db')
  ctx = Context.of(db)

  def pusher():
    Input = wkf.input(ctx)
    try:
      with Session(db.engine) as s:
        for i in range(10):
          print(f'Pushing {i}')
          s.add(Input(key=f'key-{i}', value=f'value-{i}', output='output'))
        s.commit()
    except Exception as e:
      print(e)

  pusher()

  proc = Process(target=asyncio.run, args=[wkf.run(ctx)])
  proc.start()

  app = api.run(ctx)
  uvicorn.run(app, host='0.0.0.0', port=8000)

  proc.join()