import typer

main = typer.Typer()

@main.callback()
def callback():
  ...

@main.command()
def proxy(
  pub_url: str = typer.Option('tcp://*:5555', '-p', '--pub'),
  sub_url: str = typer.Option('tcp://*:5556', '-s', '--sub'),
  verbose: bool = typer.Option(False, '-v', '--verbose')
):
  import asyncio
  from dslog import Logger
  from pipeteer.backend import proxy
  log = Logger.click().prefix('[PROXY]') if verbose else Logger.empty()
  asyncio.run(proxy(pub_url=pub_url, sub_url=sub_url, log=log))
