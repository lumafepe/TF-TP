""" Module containing useful utilities for sending and receiving Maelstrom messages.

Typical usage example:

  send("n1", self.node_id, type='echo', content='hello world')
  reply(msg)
"""
import logging
from sys import stdin
from json import loads, dumps
from types import SimpleNamespace as sn
from os import _exit
from dataclasses import dataclass
from typing import AsyncIterable, TypeAlias, Any

msg_id = 0

Body: TypeAlias = dict[str, Any]

@dataclass
class Message:
    """
    The data class specifying the structure of a Maelstrom message

    Attributes:
       src:   The node which sent the message
       dest:  The node which the message was sent to
       body:  The body of the message
    """
    src: str
    dest: str
    body: Body


def send(src: str, dest: str, **body: object):
    """
    Sends a given message.
    
    Arguments:
        src:  The node which is sending the message
        dest: The node the message will be sent to
        body: The body of the message (excluding 'msg_id')
    """
    global msg_id
    data = dumps(sn(dest=dest, src=src, body=sn(msg_id=(msg_id:=msg_id+1), **body)), default=vars)
    logging.debug("sending %s", data)
    print(data, flush=True)

def reply(request: Message, **body: object) -> None:
    """
    Sends a reply to a given message.

    A reply is a message with a 'in_reply_to' field in its body, which identifies the message
    it originated from.

    Arguments:
        request: The message to reply to
        body:    The body of the reply message (excluding 'msg_id' and 'in_reply-to')

    """
    send(request.dest, request.src, in_reply_to=request.body.msg_id, **body)

def receiveAll() -> AsyncIterable[Message]:
    """
    Receives all incoming messages until stdin closes.

    Returns
       All the messages, assynchronously.
    """
    while data := stdin.readline():
        logging.debug("received %s", data.strip())
        yield loads(data, object_hook=lambda x: sn(**x))
    