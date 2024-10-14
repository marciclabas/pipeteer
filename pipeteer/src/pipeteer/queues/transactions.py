import asyncio  
from dataclasses import dataclass

class Transactional:

  async def enter(self, other=None):
    """Enter the transaction
    - Throws `InfraError`
    """
  
  async def commit(self, other=None):
    """Commit the transaction
    - Throws `InfraError`
    """

  async def rollback(self, other=None):
    """Rollback the transaction
    - Throws `InfraError`
    """

  async def close(self, other=None):
    """Close the transaction
    - Throws `InfraError`
    """

  async def __aenter__(self):
    await self.enter()
    return self
  
  async def __aexit__(self, *_):
    await self.close()

@dataclass
class Transaction:
  def __init__(self, agent: Transactional, *agents: Transactional, autocommit: bool = False):
    self.agent = agent
    self.agents = list(agents)
    self.autocommit = autocommit

  async def add(self, agent: Transactional):
    await agent.enter(self.agent)
    self.agents.append(agent)

  async def __aenter__(self):
    await self.agent.enter()
    await asyncio.gather(*[agent.enter(self.agent) for agent in self.agents])
    return self
    
  async def commit(self):
    await asyncio.gather(*[agent.commit(self.agent) for agent in self.agents])
    await self.agent.commit()

  async def close(self):
    await asyncio.gather(*[agent.close(self.agent) for agent in self.agents])
    await self.agent.close()

  async def __aexit__(self, exc_type, exc_value, traceback):
    if exc_type is None and self.autocommit:
      await self.commit()
    await self.close()