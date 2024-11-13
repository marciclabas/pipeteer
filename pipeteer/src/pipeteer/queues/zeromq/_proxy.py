import zmq
from zmq.asyncio import Context#, Socket

async def proxy(
  pub_addr: str = 'tcp://*:5555', sub_addr: str = 'tcp://*:5556',
):
  ctx = Context.instance()

  print(f'Proxying {pub_addr} -> {sub_addr}')

  frontend = ctx.socket(zmq.XSUB)
  frontend.bind(pub_addr)
  
  backend = ctx.socket(zmq.XPUB)
  backend.bind(sub_addr)

  zmq.proxy(frontend, backend)

# async def proxy(
#   pub_addr: str = 'tcp://*:5555', sub_addr: str = 'tcp://*:5556',
# ):
#   ctx = Context.instance()

#   frontend = ctx.socket(zmq.SUB)
#   frontend.bind(pub_addr)
#   frontend.setsockopt_string(zmq.SUBSCRIBE, '')
  
#   backend = ctx.socket(zmq.PUB)
#   backend.bind(sub_addr)

#   print(f'Proxying {pub_addr} -> {sub_addr}')
#   while True:
#     try:
#       msg = await asyncio.wait_for(frontend.recv(), 2)
#       print('Proxying:', msg)
#       await backend.send(msg)
#     except asyncio.TimeoutError:
#       print('Proxy timeout')