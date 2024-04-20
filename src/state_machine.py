from abc import ABC, abstractmethod

class StateMachine(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def apply(self):
        pass