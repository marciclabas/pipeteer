from .write import tee, prefilter, premap
from .list import singleton, appender
from .delegate import ReadDelegate, WriteDelegate, Delegate

__all__ = [
  'tee', 'prefilter', 'premap',
  'singleton', 'appender',
  'ReadDelegate', 'WriteDelegate', 'Delegate'
]