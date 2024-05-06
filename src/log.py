"""A module defining a class representing log entries of the Raft Algorithm.

Raft uses logs to replicate the changes to the replicated state machines to
followers. A log corresponds to an operation requested by a client.

Typical usage example:

  log1 = Log(None, 1, 1)
  log2 = Log(msg, 2, 2)
"""

from types import SimpleNamespace
from ms import Message

class Log:
    """A class representing log entries of the Raft Algorithm.

    Raft uses logs to replicate the changes to the replicated state machines to
    followers. A log corresponds to an operation requested by a client.

    Attributes:
        term: The term the log was generated in
        id:   The id of the log. Corresponds to its index in the
        log array
        data: A dictionary containing the information specifying
        the log
    """
    def __init__(self, message: Message | None, term: int, id: int) -> None:
        """Initializes a log entry from a Maelstrom message, current term and
        id.

        Args:
          message: The message to parse into a log.
          term:    The current term.
          id:      The id of this log.
        """
        self.term = term
        self.id = id
        self.message = message
        self.data = {}
        if message is not None:
            self.__process_message__(message)

    @staticmethod
    def from_namespace(namespace: SimpleNamespace) -> "Log":
        """Initializes a log entry from a SimpleNamespace.

        This is used as Maelstrom, when sending messages, parses classes to SimpleNamespaces.
        As such, to garantee the type consistency of the logs, a method to parse a
        SimpleNamespace to a Log had to be implemented.

        Args:
          namespace: The namespace to parse

        Returns:
          The log corresponding to the namespace
        """
        log = Log(None, namespace.term, namespace.id)
        log.data = vars(namespace.data)
        return log

    def __process_message__(self, message: Message) -> None:
        """Initializes the data dictionary based on the content of the given message.

        Args:
          message: The message to parse.
        """
        self.data["type"] = message.body.type

        match message.body.type:
            case "write":
                self.data["key"] = message.body.key
                self.data["value"] = getattr(message.body, "value")
            case "cas":
                self.data["key"] = message.body.key
                self.data["from"] = getattr(message.body, "from")
                self.data["to"] = message.body.to

    def __eq__(self, other: "Log") -> bool:
        """Checks for equality against another log.

        Two logs are said to be equal if their term, id, and data content are equal.
        

        Args:
          other: The other to check for equality against.

        Returns:
          A bool indicating if the logs are equal
        """
        return type(self) == type(other) and self.data == other.data and self.term == other.term and self.id == other.id

    def __str__(self) -> str:
        """Converts a log to a string for debug purposes.

        Returns
           A string representation of the log
        """
        return str(vars(self))

    def __repr__(self) -> str:
        """Converts a log to a string for debug purposes.

        Returns
           A string representation of the log
        """
        return str(self)