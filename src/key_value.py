from state_machine import StateMachine

class KVStore(StateMachine):
    def __init__(self):
        self.store = {}

    def read(self, key: object) -> object | None:
        return self.store.get(key)
    
    def write(self, key: object, value: object):
        self.store[key] = value

    def cas(self, key: object, _from: object, to: object) -> bool | None:
        original = self.store.get(key)

        if original is None:
            return None
        
        if original == _from:
            self.store[key] = to

        return original == _from


    def apply(self, log):
        pass