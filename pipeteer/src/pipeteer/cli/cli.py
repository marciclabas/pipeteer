import typer

main = typer.Typer()

@main.callback()
def callback():
  ...

@main.command()
def proxy(
  pub_addr: str = typer.Option('tcp://*:5555', '-p', '--pub'),
  sub_addr: str = typer.Option('tcp://*:5556', '-s', '--sub'),
  verbose: bool = typer.Option(False, '-v', '--verbose')
):
  import asyncio
  from pipeteer.backend import proxy
  asyncio.run(proxy(pub_addr=pub_addr, sub_addr=sub_addr, verbose=verbose))
