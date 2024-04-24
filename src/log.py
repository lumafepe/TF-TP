from ms import Message

class Log:
    def __init__(self, message: Message | None, term: int, id: int):
        self.term = term
        self.id = id
        self.data = {}
        if message is not None:
            self.__process_message__(message)

    def __process_message__(self, message: Message):
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
        return type(self) == type(other) and self.data == other.data and self.term == other.term and self.id == other.id