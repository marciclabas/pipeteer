from typing_extensions import TypeVar, Generic, Callable, Awaitable, Any, Protocol, overload, Coroutine
from dataclasses import dataclass
import asyncio
from datetime import timedelta, datetime
import traceback
from sqlmodel import SQLModel, Field, select, Session, or_
from sqlalchemy import delete
from pydantic import TypeAdapter
from sqltypes import ValidatedJSON
from pipeteer.pipelines import Pipeline, Inputtable, Context, Input, Entry, InputT, EntryT
from pipeteer.util import param_type, return_type, race
from .pipeline import A

Aw = Awaitable
# A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
AnyT: type = Any # type: ignore

class Stop(BaseException):
  ...

class WorkflowContext(Protocol):
  async def call(self, pipe: Inputtable[A, B], x: A, /) -> B:
    ...
  @overload
  async def all(self, a: Aw[A], b: Aw[B], /) -> tuple[A, B]: ...
  @overload
  async def all(self, a: Aw[A], b: Aw[B], c: Aw[C], /) -> tuple[A, B, C]: ...
  @overload
  async def all(self, a: Aw[A], b: Aw[B], c: Aw[C], d: Aw[D], /) -> tuple[A, B, C, D]: ...
  @overload
  async def all(self, *coros: Aw[A]) -> tuple[A, ...]: ...

@dataclass
class WkfContext(WorkflowContext):
  ctx: Context
  session: Session
  State: type['State']
  states: list['State']
  key: str
  output: str
  step: int = 0

  async def call(self, pipe: Inputtable[A, B], x: A, /) -> B:
    if self.step < len(self.states):
      state = self.states[self.step]
      if pipe.input_adapter.dump_python(x) != state.param:
        param = pipe.input_adapter.validate_python(state.param)
        self.ctx.log(f'Impure workflow. At step {self.step}, calling "{pipe.id}": expected "{param}" but got "{x}"', level='ERROR')
        raise RuntimeError('Impure workflow')

      self.ctx.log(f'Replaying {pipe.id}, step={self.step}, key="{self.key}"', level='DEBUG')

      self.step += 1
      return pipe.output_adapter.validate_python(state.result)
    
    else:
      self.ctx.log(f'Calling {pipe.id}, step={self.step}, key="{self.key}"', level='DEBUG')
      PipeInp = pipe.input(self.ctx)
      with Session(self.ctx.db.engine) as s:
        s.add(PipeInp(key=f'{self.step}_{self.key}', value=x, output=self.output))
        s.add(self.State(key=self.key, step=self.step, param=x, pipeline=pipe.id))
        s.commit()
      await self.ctx.zmq.pub.send(pipe.id)

      self.step += 1
      raise Stop()
    
  async def all(self, *coros: Awaitable): # type: ignore
    n = len(coros)
    if self.step + n-1 < len(self.states):
      self.ctx.log(f'Replaying all, step={self.step}, key="{self.key}"', level='DEBUG')
      return tuple(await asyncio.gather(*coros))
    
    elif self.step == len(self.states):
      self.ctx.log(f'Calling all, step={self.step}, key="{self.key}"', level='DEBUG')
      for coro in coros:
        try:
          await coro
        except Stop:
          ...
    else:
      received = self.step+n+1 - len(self.states)
      self.ctx.log(f'Ignoring all (received {received}/{n}), step={self.step}, key="{self.key}"', level='DEBUG')
    raise Stop()
  
class State(SQLModel):
  key: str = Field(primary_key=True)
  step: int = Field(primary_key=True)
  param: Any = Field(sa_type=ValidatedJSON(AnyT))
  result: Any | None = Field(default=None, sa_type=ValidatedJSON(AnyT))
  done: bool = False
  pipeline: str
  
class WkfInputT(InputT[A], Generic[A]):
  doing: bool = False

def WkfInput(T: type[A]) -> type[WkfInputT[A]]:
  Inp = Input(T)
  class WkfInput(Inp):
    doing: bool = False
  return WkfInput # type: ignore


class Result(EntryT):
  value: Any = Field(sa_type=ValidatedJSON(AnyT))
  ttl: datetime | None = None

@dataclass
class Workflow(Pipeline[A, B, Context, Coroutine], Generic[A, B]):
  Input = WkfInput
  State = State
  call: Callable[[A, WorkflowContext], Awaitable[B]]
  reserve: timedelta | None = None
  poll_interval: timedelta = timedelta(seconds=1)

  def states(self, ctx: Context):
    return ctx.db.table(self.id + '-states', State)
  
  def input(self, ctx: Context):
    return ctx.db.table(self.id, WkfInput(self.Tin))
  
  def results(self, ctx: Context):
    output = self.id + '-results'
    table = ctx.db.table(output, Result)
    return output, table
  
  def run(self, ctx: Context) -> Coroutine:
      
    self.results(ctx) # trigger creation
    
    async def loop():

      # trigger creation (in-process)
      ctx.zmq.pub
      ctx.zmq.sub(self.id)
      ctx.zmq.sub(self.id + '-results')

      output, Result = self.results(ctx)
      Input = self.input(ctx)
      State = self.states(ctx)

      async def run(*, key: str, results_key: str | None = None, input, states: list['State'] = [], s: Session):
        wkf_ctx = WkfContext(ctx, session=s, State=State, states=states, key=key, output=output)
        ctx.log(f'Rerunning "{key}"', level='DEBUG')
        input = TypeAdapter(self.Tin).validate_python(input)
        out = await self.call(input, wkf_ctx)
        
        item = s.get(Input, key)
        if item is None:
          raise ValueError(f'Input item "{key}" not found')
        
        ctx.log(f'Outputting "{key}" to "{item.output}"', level='DEBUG')
        Output = ctx.db.table(item.output, Entry(self.Tout))
        s.add(Output(key=key, value=out))
        s.exec(delete(State).where(State.key == key)) # type: ignore[call-overload]
        s.exec(delete(Input).where(Input.key == key)) # type: ignore[call-overload]
        if results_key is not None:
          s.exec(delete(Result).where(Result.key == results_key)) # type: ignore[call-overload]

        s.commit()
        await ctx.zmq.pub.send(item.output)
        
      async def input_step(inp: WkfInputT, s: Session):
        ctx.log(f'Input loop: "{inp.key}"', level='DEBUG')
        try:
          await run(key=inp.key, input=inp.value, s=s)
        except Stop:
          inp.doing = True
          s.add(inp)
          s.commit()

      async def results_step(inp: EntryT, s: Session):
        i, key = inp.key.split('_', 1)
        i = int(i)
        ctx.log(f'Results loop: "{key}", step={i}', level='DEBUG')

        if not (input := s.get(Input, key)):
          return
        
        states = s.exec(select(State).where(State.key == key)).all()
        states = sorted(states, key=lambda state: state.step)
        if not states or states[-1].done:
          return
        
        state = states[-1]
        state.done = True
        state.result = inp.value
        s.add(state)

        try:
          await run(key=key, results_key=inp.key, input=input.value, states=states, s=s)
          
        except Stop:
          s.delete(inp)
          s.commit()


      async def input_loop():
        sub = ctx.zmq.sub(self.id)
        while True:
          try:
            with Session(ctx.db.engine) as s:
              free = or_(Input.ttl == None, Input.ttl < datetime.now()) # type: ignore
              inp = s.exec(select(Input).where(Input.doing == False, free)).first()
              if inp is None:
                await race([
                  asyncio.sleep(self.poll_interval.total_seconds()),
                  sub.wait()
                ])
                continue
              elif self.reserve:
                inp.ttl = datetime.now() + self.reserve
                s.commit()
            
              await input_step(inp, s)
          except:
            ctx.log('Error in input loop', traceback.format_exc(), level='ERROR')
            await asyncio.sleep(self.poll_interval.total_seconds())


      async def results_loop():
        sub = ctx.zmq.sub(output)
        while True:
          try:
            with Session(ctx.db.engine) as s:
              inp = s.exec(select(Result)).first()
              if inp is None:
                await race([
                  asyncio.sleep(self.poll_interval.total_seconds()),
                  sub.wait()
                ])
                continue
              elif self.reserve:
                inp.ttl = datetime.now() + self.reserve
                s.commit()
            
              await results_step(inp, s)
          except:
            ctx.log('Error in results loop', traceback.format_exc(), level='ERROR')
            await asyncio.sleep(self.poll_interval.total_seconds())

      await asyncio.gather(input_loop(), results_loop())

    return loop()
  

  async def delete(self, ctx: Context, key: str):
    """Delete a workflow by key"""
    Inp = self.input(ctx)
    State = self.states(ctx)
    _, Result = self.results(ctx)
    with Session(ctx.db.engine) as s:
      s.exec(delete(State).where(State.key == key)) # type: ignore[call-overload]
      s.exec(delete(Result).where(Result.key == key)) # type: ignore[call-overload]
      s.exec(delete(Inp).where(Inp.key == key)) # type: ignore[call-overload]
      s.commit()

  async def restart(self, ctx: Context, key: str):
    """Restart a workflow by key"""
    Inp = self.input(ctx)
    State = self.states(ctx)
    _, Result = self.results(ctx)
    with Session(ctx.db.engine) as s:
      if (inp := s.get(Inp, key)):
        inp.doing = False
        inp.ttl = None
        s.add(inp)
        s.exec(delete(State).where(State.key == key)) # type: ignore[call-overload]
        s.exec(delete(Result).where(Result.key == key)) # type: ignore[call-overload]
        s.commit()
        await ctx.zmq.pub.send(self.id)

  async def step(self, ctx: Context, key: str) -> int | None:
    """At which step is the workflow?"""
    State = self.states(ctx)
    with Session(ctx.db.engine) as s:
      state = s.exec(select(State).where(State.key == key).order_by(State.step.desc())).first() # type: ignore
      if state:
        return state.step

def workflow(
  *, id: str | None = None,
  reserve: timedelta | None = timedelta(minutes=2),
  poll_interval: timedelta = timedelta(minutes=2),
):
  def decorator(fn: Callable[[A, WorkflowContext], Awaitable[B]]) -> Workflow[A, B]:
    Tin = param_type(fn)
    if Tin is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its input parameter')

    Tout = return_type(fn)
    if Tout is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its return value')
    
    return Workflow(
      Tin=Tin, Tout=Tout,
      id=id or fn.__name__,
      call=fn, # type: ignore
      reserve=reserve,
      poll_interval=poll_interval,
    ) # type: ignore	
  return decorator