#!/usr/bin/env python

import logging
from key_value import KVStore
import threading
from ms import receiveAll, reply, send

logging.getLogger().setLevel(logging.DEBUG)


class Raft():
    def __init__(self):
        self.node_id = -1
        self.node_ids = []
        self.node_count = 0
        self.kv_store = KVStore()

    def init(self, msg: any):
        self.node_id = msg.body.node_id
        self.node_ids = msg.body.node_ids
        self.node_count = len(self.node_ids)
        logging.info('node %s initialized', self.node_id)
        reply(msg, type='init_ok')

    def read(self, msg: any):
        value = self.kv_store.read(msg.body.key)

        if value is None:
            reply(msg, type='error', code=20)
        else:
            reply(msg, type='read_ok', value=value)

    def write(self, msg: any):
        self.kv_store.write(msg.body.key, getattr(msg.body, 'value'))
        reply(msg, type='write_ok')

    def cas(self, msg: any):
        _from = getattr(msg.body, 'from')
        matches = self.kv_store.cas(msg.body.key, _from, msg.body.to)

        if matches is None:
            reply(msg, type='error', code=20)
        elif not matches:
            reply(msg, type='error', code=22)
        else:
            reply(msg, type='cas_ok')

raft = Raft()

for msg in receiveAll():
    match msg.body.type:
        case 'init':
            raft.init(msg)
        case 'read':
            raft.read(msg)
        case 'write':
            raft.write(msg)
        case 'cas':
            raft.cas(msg)
        case _:
            logging.warning('unknown message type %s', msg.body.type)