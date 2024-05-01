import logging
from log import Log
from state_machine import StateMachine

class KVStore(StateMachine):
    def __init__(self) -> None:
        self.store = {}

    def read(self, key: object) -> object | None:
        return self.store.get(key)
    
    def write(self, key: object, value: object) -> None:
        self.store[key] = value

    def cas(self, key: object, _from: object, to: object) -> bool | None:
        original = self.store.get(key)

        if original is None:
            return None
        
        if original == _from:
            self.store[key] = to

        return original == _from


    def apply(self, log: Log) -> None:
        if log is not None:
            match log.data["type"]:
                case "write":
                    self.write(log.data["key"], log.data["value"])
                case "cas":
                    self.cas(log.data["key"], log.data["from"], log.data["to"])