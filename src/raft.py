#!/usr/bin/env python

import logging
from key_value import KVStore
from threading import Lock, Condition, Thread
from time import sleep
from enum import Enum
from time import time
from log import Log
from sched import scheduler
from random import uniform
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
        self.log = [Log({}, 0)]

        self.commitIndex = 0
        self.lastApplied = 0

        # Implementation only
        self.votedForMe = set()
        self.electionTimer = time()

        # Leader only
        self.nextIndex = []
        self.matchIndex = []
        self.leaderId = -1

        self.heartbeat_condition = Condition()
        self.heartbeat_running = False
        self.election_timeout_condition = Condition()
        self.election_timeout_running = True

        heartbeat_thread = Thread(target=self.heartbeat_thread)

        self.backlog = []

        heartbeat_thread.start()
        
    def heartbeat_thread(self):
        while True:
            with self.heartbeat_condition:
                while not self.heartbeat_running:
                    self.heartbeat_condition.wait()
        
            sleep(0.04)
            for node in self.node_ids:
                if node != self.node_id:
                    send(self.node_id, node, type='AppendEntriesRPC',  ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=[],leaderCommit=self.commitIndex)
    
    def election_thread(self, timeout, sch):
        if self.election_timeout_running:
            if(time() - self.electionTimer >= timeout):
                self.change_role(RaftRole.CANDIDATE)
        sch.enter(uniform(0.150, 0.300), 1, lambda: self.election_thread(timeout, sch))
        sch.run(blocking=False)


    def set_election_timer_running(self, value: bool):
        with self.election_timeout_condition:
            self.election_timeout_running = value

    def set_election_timer(self, timer: float):
        logging.info("Timer set")
        self.electionTimer = time()

    def set_heartbeat_running(self, value: bool):
        with self.heartbeat_condition:
            self.heartbeat_running = value

    def init(self, msg: any):
        self.node_id = msg.body.node_id
        self.node_ids = msg.body.node_ids
        self.node_count = len(self.node_ids)
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


    def fromClient(self, msg: any):
        if self.node_id == self.leaderId:
            match msg.body.type:
                case 'read':
                    self.read(msg)
                case 'write':
                    self.write(msg)
                case 'cas':
                    self.cas(msg)
        elif self.leaderId != -1:
            send(msg.src, self.leaderId, body=msg.body)
        else:
            self.backlog.append(msg)




    def check_term(self, term: int):
        if(term > self.currentTerm):
            self.currentTerm = term
            self.change_role(RaftRole.FOLLOWER)

    def change_role(self, role: RaftRole):
        match role:
            case RaftRole.LEADER:
                self.nextIndex = {k: len(self.log) for k in self.node_ids if k != self.node_id}
                self.matchIndex = {k: 0 for k in self.node_ids if k != self.node_id}
                self.start_heartbeats()
                self.cancel_timeout_check()
            case RaftRole.CANDIDATE:
                self.start_election()
                self.start_timeout_check()
                self.cancel_heartbeats()
            case RaftRole.FOLLOWER:
                for msg in self.backlog:
                    self.fromClient(msg)
                self.backlog = []
                self.start_timeout_check()
                self.cancel_heartbeats()

    def start_heartbeats(self):
        self.set_heartbeat_running(True)

    def cancel_heartbeats(self):
        self.set_heartbeat_running(False)

    def start_timeout_check(self):
        self.set_election_timer_running(True)

    def cancel_timeout_check(self):
        self.set_election_timer_running(False)

    def start_election(self):
        logging.debug("Starting election")
        self.currentTerm += 1
        self.votedFor = self.node_id

        self.votedForMe = set()
        self.votedForMe.add(self.node_id)

        self.set_election_timer(time())

        # Broadcast RequestVoteRPC
        for node in self.node_ids:
            if node != self.node_id:
                send(self.node_id, node, type='RequestVoteRPC', term=self.currentTerm, \
                    candidateId=self.node_id, lastLogIndex = len(self.log), lastLogTerm = self.log[-1].term)

    def more_up_to_date(self, lastLogIndex : int, lastLogTerm : int) -> bool:
        if self.currentTerm < lastLogTerm:
            return True

        if self.log[-1].term > lastLogTerm:
            logging.info(f"Failing {self.currentTerm} > {lastLogTerm}")
            return False

        return len(self.log) <= lastLogIndex

    def conflicts(self, entry: Log, index: int) -> bool:
        return index < len(self.log) and self.log[index].term != entry.term

    def append_log(self, entry: Log) -> bool:
        new = entry not in self.log

        if new:
            self.log.append(entry)

        return new

    def setCommitIndex(self, index: int):
        self.commitIndex = index

        while self.lastApplied < index:
            self.lastApplied += 1
            self.kv_store.apply(self.log[self.lastApplied])


    def append_entries(self, msg: any):
        self.check_term(msg.body.term)
        self.set_election_timer(time())
        if msg.body.term < self.currentTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return
        
        self.leaderId = msg.src

        if len(self.log) <= msg.body.prevLogIndex or self.log[msg.body.prevLogIndex].term != msg.body.prevLogTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return

        for i, entry in enumerate(msg.body.entries):
            if self.conflicts(entry, msg.body.prevLogIndex + i + 1):
                self.log = self.log[:msg.body.prevLogIndex + i + 1]

        lastNew = 9223372036854775807
        for i, entry in enumerate(msg.body.entries):
            new = self.append_log(entry)
            if new:
                lastNew = msg.body.prevLogIndex + i + 1

        if msg.body.leaderCommit > self.commitIndex:
            self.setCommitIndex(min(msg.body.leaderCommit, lastNew))
        
        reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = True, ni=msg.body.ni, entries=msg.body.entries)



    def append_entries_reply(self, msg: any):
        self.check_term(msg.body.term)
        
        if msg.body.success:
            self.nextIndex[msg.src]  = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries))
            self.matchIndex[msg.src] = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries) - 1)
        else:
            self.nextIndex[msg.src] -= 1
            reply(msg, type='AppendEntriesRPC', ni=self.nextIndex[msg.src], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[msg.src] - 1, prevLogTerm=self.log[self.nextIndex[msg.src] - 1].term, entries=self.log[self.nextIndex[msg.src]:],leaderCommit=self.commitIndex)

    def request_vote(self, msg: any):
        logging.debug("Received")
        logging.debug("Term checked")
        self.set_election_timer(time())
        self.check_term(msg.body.term)
        logging.debug("Timer")

        logging.info("Aqui1")
        if msg.body.term < self.currentTerm:
            logging.info("Aqui2")
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)
            return

        logging.info("Aqui3")
        logging.info(f"{self.votedFor} {msg.body.candidateId} {msg.body.lastLogIndex} {msg.body.lastLogTerm}")
        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)

        if voteGranted:
            self.votedFor = msg.src

        reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = voteGranted)

    def request_vote_reply(self, msg: any):
        self.check_term(msg.body.term)

        if msg.body.voteGranted:
            self.votedForMe.add(msg.src)

        if len(self.votedForMe) >= self.node_count // 2:
            self.change_role(RaftRole.LEADER)


raft = Raft()
sch = scheduler(time, sleep)

def process(msg):
    match msg.body.type:
        case 'init':
            raft.init(msg)
        case 'AppendEntriesRPC':
            raft.append_entries(msg)
        case 'AppendEntriesRPCReply':
            raft.append_entries_reply(msg)
        case 'RequestVoteRPC':
            raft.request_vote(msg)
        case 'RequestVoteRPCReply':
            raft.request_vote_reply(msg)
        case _:
            raft.fromClient(msg)

sch.enter(uniform(0.075, 0.15), 1, lambda: raft.election_thread(uniform(0.35, 0.75), sch))
for msg in receiveAll():
    sch.enter(0, 1, lambda: process(msg))
    sch.run(blocking=False)