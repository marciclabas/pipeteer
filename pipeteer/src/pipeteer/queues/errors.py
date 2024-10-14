from typing_extensions import Literal, Any
from dataclasses import dataclass

@dataclass(eq=False)
class QueueError(Exception):
  """Base class for all queue errors"""

  detail: Any | None = None
  key: str | None = None
  value: Any | None = None

  def __str__(self) -> str:
    return self.__repr__()
  
@dataclass
class InfraError(QueueError):
  """Infrastructure-related error (not logic)"""
  reason: Literal['infra-error'] = 'infra-error'

@dataclass(eq=False)
class InexistentItem(QueueError):
  reason: Literal['inexistent-item'] = 'inexistent-item'
  
ReadError = InfraError | InexistentItem