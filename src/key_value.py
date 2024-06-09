""" Module defining a simple key value store to use as example for Raft implementation.

Typical usage example:

  store = KVStore()
"""

from log import Log
from state_machine import StateMachine
import logging

logging.getLogger().setLevel(logging.DEBUG)


class KVStore(StateMachine):
    """A simple key value store.

    This key value store is a generic hash table supporting the following operations:

        - write: Write the given value to the given key
        - read:  Read the value of the given key
        - cas:   Write the given value to the given key, but only if the current value of the key
        matches another given value

    Attributes:
        store: The dictionary used as the hash table.
    """

    def __init__(self) -> None:
        """Initializes the key value store to be empty."""
        self.store = {}

    def read(self, key: object) -> object | None:
        """Reads the value of a key.

        Args:
          key: The key to read.

        Returns:
          The value of the given key.
        """
        return self.store.get(key)

    def write(self, key: object, value: object) -> None:
        """Writes a value to a given key.

        Args:
          key:   The key to write to.
          value: The value to write.
        """
        self.store[key] = value

    def cas(self, key: object, _from: object, to: object) -> bool | None:
        """Writes the given value to the given key, but only if the current
        value equals another given value.

        Args:
          key:   The key to write to.
          _from: The value that must be assigned to the key in order to perform
          the write
          to:    The val.ue to replace the original with.

        Returns:
          None if the given key does not exist in the key value store. Otherwise,
          it returns a boolean representing whether the original value corresponded
          to the required one.
        """
        original = self.store.get(key)

        if original is None:
            return None

        if original == _from:
            self.store[key] = to

        return original == _from

    def apply(self, log: Log) -> None:
        """Applies a log entry to a key value store.

        Applying an entry corresponds to performing the operation specified by
        that entry.

        Args:
          log: The log entry to apply.
        """
        logging.debug(f"Applying log {log}")
        if log is not None and log.id != 0:
            match log.data["type"]:
                case "write":
                    self.write(log.data["key"], log.data["value"])
                case "cas":
                    self.cas(log.data["key"], log.data["from"], log.data["to"])
