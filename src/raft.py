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
from queue import Queue
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
        if self.heartbeat_running: 
            for node in self.node_ids:
                if node != self.node_id:
                    send(self.node_id, node, type='AppendEntriesRPC',  ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=[],leaderCommit=self.commitIndex)
    
    def election_thread(self, timeout, executor):
        if self.election_timeout_running:
            if(time() - self.electionTimer >= timeout):
                self.change_role(RaftRole.CANDIDATE)

    def set_election_timer_running(self, value: bool):
        with self.election_timeout_condition:
            self.election_timeout_running = value

    def set_election_timer(self, timer: float):
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


    def fromClient(self, msg: any, append: bool = True):
        logging.debug(f"From Client 1 {msg.body.type} {self.node_id} {self.leaderId}")
        if self.node_id == self.leaderId:
            logging.debug("From Client 1.1")
            match msg.body.type:
                case 'read':
                    self.read(msg)
                case 'write':
                    self.write(msg)
                case 'cas':
                    self.cas(msg)
        elif self.leaderId != -1:
            logging.debug("From Client 1.2")
            kwargs = vars(msg.body)
            del kwargs["msg_id"]
            send(msg.src, self.leaderId, **kwargs)
        elif append:
            logging.debug("From Client 1.3")
            self.backlog.append(msg)
        logging.debug("From Client 2")




    def check_term(self, term: int):
        if(term > self.currentTerm):
            self.currentTerm = term
            self.change_role(RaftRole.FOLLOWER)

    def change_role(self, role: RaftRole):
        match role:
            case RaftRole.LEADER:
                self.leaderId = self.node_id
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
                    self.fromClient(msg, append=False)
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
        logging.debug("Request vote begin")
        self.set_election_timer(time())
        logging.debug("Request vote 1")
        self.check_term(msg.body.term)
        logging.debug("Request vote 2")
        if msg.body.term < self.currentTerm:
            logging.debug("Request vote 2.5")
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)
            logging.debug("Request vote end")
            return

        logging.debug("Request vote 3")
        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)
        logging.debug("Request vote 4")
        if voteGranted:
            self.votedFor = msg.src
        logging.debug("Request vote 5")
        reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = voteGranted)
        logging.debug("Request vote end")

    def request_vote_reply(self, msg: any):
        logging.debug("RPCReply")
        self.check_term(msg.body.term)

        if msg.body.voteGranted:
            logging.debug(f"Granted {len(self.votedForMe)} {self.node_count}")
            self.votedForMe.add(msg.src)

        if len(self.votedForMe) >= (self.node_count + 1) // 2:
            self.change_role(RaftRole.LEADER)
            logging.debug("Elected")

queue = Queue()
raft = Raft()

def election_probe():
    while True:
        queue.put("election")
        logging.debug("Election")
        sleep(uniform(0.05, 0.15))

def heartbeat_probe():
    while True:
        queue.put("hearbeat")
        logging.debug("Heartbeat")
        sleep(uniform(0.05, 0.15))


def process():
    timeout = uniform(0.25, 0.75)
    while True:
        logging.debug("Process 1")
        msg = queue.get()
        logging.debug("Process 2")

        if msg == "election":
            raft.election_thread(timeout, None)
            logging.debug("Process 3")
            continue
        elif msg == "hearbeat":
            raft.heartbeat_thread()
            logging.debug("Process 3")
            continue

        logging.debug(msg)
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
        logging.debug("Process 3")
    logging.debug("Process over")

def main():
    for msg in receiveAll():
        queue.put(msg)
        logging.debug("Receive")

hello_thread = Thread(target=process)
hello_thread.start()
hello_thread2 = Thread(target=election_probe)
hello_thread2.start()
hello_thread3 = Thread(target=heartbeat_probe)
hello_thread3.start()
hello_thread4 = Thread(target=main)
hello_thread4.start()

hello_thread4.join()

queue.task_done()