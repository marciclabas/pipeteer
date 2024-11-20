from dataclasses import dataclass, field
import os
import zmq
from zmq.asyncio import Context
from dslog import Logger

@dataclass
class ZMQ:
  pub_url: str = 'tcp://localhost:5555'
  sub_url: str = 'tcp://localhost:5556'
  _pid: int = os.getpid()
  _subs: dict[str, 'Sub'] = field(default_factory=dict)
  _pub: 'Pub | None' = None

  def __post_init__(self):
    self._pub = Pub(self.pub_url)

  def sub(self, topic: str):
    pid = os.getpid()
    if self._pid != pid:
      self._pid = pid
      self._subs = {}

    if not topic in self._subs:
      self._subs[topic] = Sub(topic, self.sub_url)

    return self._subs[topic]

  @property  
  def pub(self):
    pid = os.getpid()
    if self._pid != pid:
      self._pid = pid
      self._pub = None

    if self._pub is None:
      self._pub = Pub(self.pub_url)

    return self._pub
  
  async def proxy(self, log: Logger = Logger.empty()):
    await proxy(pub_url=self.pub_url, sub_url=self.sub_url, log=log)

@dataclass
class Sub:
  topic: str
  url: str = 'tcp://localhost:5556'

  def __post_init__(self):
    self.pid = os.getpid()
    ctx = Context.instance()
    self.sub = ctx.socket(zmq.SUB)
    self.sub.connect(self.url)
    self.sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)

  async def wait(self):
    if os.getpid() != self.pid:
      raise RuntimeError('Sub is not fork-safe')
    while True:
      topic = await self.sub.recv_string()
      if topic == self.topic: # could be a prefix but not equal
        return

@dataclass
class Pub:
  url: str = 'tcp://localhost:5555'

  def __post_init__(self):
    self.pid = os.getpid()
    ctx = Context.instance()
    self.pub = ctx.socket(zmq.PUB)
    self.pub.connect(self.url)

  async def send(self, topic: str):
    if os.getpid() != self.pid:
      raise RuntimeError('Pub is not fork-safe')
    await self.pub.send_string(topic)


async def proxy(
  pub_url: str = 'tcp://*:5555', sub_url: str = 'tcp://*:5556',
  log: Logger = Logger.empty()
):
  ctx = Context.instance()

  print(f'Proxying {pub_url} -> {sub_url} [VERBOSE]')
  frontend = ctx.socket(zmq.SUB)
  frontend.bind(pub_url)
  frontend.setsockopt_string(zmq.SUBSCRIBE, '')
  
  backend = ctx.socket(zmq.PUB)
  backend.bind(sub_url)

  while True:
    msg = await frontend.recv()
    log('Proxying:', msg, level='DEBUG')
    await backend.send(msg)


