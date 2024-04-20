from abc import ABC, abstractmethod
from state_machine import StateMachine

class MessageHandler(ABC):
    @abstractmethod
    def __init__(self, state_machine: StateMachine):
        pass

    @abstractmethod
    def handle(self, msg):
        pass