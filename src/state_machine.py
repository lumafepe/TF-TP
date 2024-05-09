""" Module defining an abstract state machine to use in the implementation of Raft.

Typical usage example:

  class Foo(StateMachine):
      pass
"""

from abc import ABC, abstractmethod
from log import Log

class StateMachine(ABC):
    """ An abstract state machine.

    Raft replicates a state machine across different processes. In order to be usable
    by the algorithm, a class must inherit from this StateMachine and define the required
    methods.
    """
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def apply(self, log: Log) -> None:
        pass