#!/usr/bin/env python

import logging
from key_value import KVStore
from enum import Enum
from time import time
from ms import receiveAll, reply, send

logging.getLogger().setLevel(logging.DEBUG)

class RaftRole(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3

class Raft():
    def __init__(self):
        self.node_id = -1
        self.node_ids = []
        self.node_count = 0
        self.kv_store = KVStore()
        self.role = RaftRole.FOLLOWER

        #TODO: Make persistent
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        self.commitIndex = 0
        self.lastApplied = 0

        # Implementation only
        self.votedForMe = set()
        self.electionTimer = time()

        # Leader only
        self.nextIndex = []
        self.matchIndex = []

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

    def check_term(self, term: int):
        if(term > self.currentTerm):
            self.currentTerm = term
            self.change_role(RaftRole.FOLLOWER)

    def change_role(self, role: RaftRole):
        match role:
            case RaftRole.LEADER:
                self.start_heartbeats()
                self.cancel_timeout_check()
            case RaftRole.CANDIDATE:
                self.start_election()
                self.start_timeout_check()
                self.cancel_heartbeats()
            case RaftRole.FOLLOWER:
                self.start_timeout_check()
                self.cancel_heartbeats()

    def start_heartbeats(self):
        pass

    def cancel_heartbeats(self):
        pass

    def start_timeout_check(self):
        pass

    def cancel_timeout_check(self):
        pass

    def start_election(self):
        self.currentTerm += 1
        self.votedFor = self.node_id

        self.votedForMe = set()
        self.votedForMe.add(self.node_id)

        self.electionTimer = time()

        # Broadcast RequestVoteRPC
        for node in self.node_ids:
            if node != self.node_id:
                send(self.node_id, node, type='RequestVoteRPC', term=self.currentTerm, \
                    candidateId=self.node_id, lastLogIndex = len(self.log), lastLogTerm = self.log[-1].term)

    def more_up_to_date(self, lastLogIndex : int, lastLogTerm : int) -> bool:
        if self.currentTerm < lastLogTerm:
            return True

        if self.currentTerm > lastLogTerm:
            return False

        return len(self.log) <= lastLogIndex

    def append_entries(self, msg: any):
        pass

    def append_entries_reply(self, msg: any):
        pass

    def request_vote(self, msg: any):
        self.check_term(msg.body.term)

        if msg.body.term < self.currentTerm:
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)

        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)

        reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = voteGranted)

    def request_vote_reply(self, msg: any):
        pass


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
        case 'AppendEntriesRPC':
            raft.append_entries(msg)
        case 'AppendEntriesRPCReply':
            raft.append_entries_reply(msg)
        case 'RequestVoteRPC':
            raft.request_vote(msg)
        case 'RequestVoteRPCReply':
            raft.request_vote_reply(msg)
        case _:
            logging.warning('unknown message type %s', msg.body.type)