#!/usr/bin/env python

import logging
from key_value import KVStore
from threading import Thread
from time import sleep
from enum import Enum
from time import time
from log import Log
from random import uniform
from queue import Queue
from ms import receiveAll, reply, send, Message

logging.getLogger().setLevel(logging.DEBUG)

class RaftRole(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3

class Raft():
    def __init__(self) -> None:
        self.node_id = -1
        self.node_ids = []
        self.node_count = 0
        self.kv_store = KVStore()
        self.role = RaftRole.FOLLOWER

        #TODO: Make persistent
        self.currentTerm = 0
        self.votedFor = None
        self.log = [Log(None, 0, 0)]

        self.commitIndex = 0
        self.lastApplied = 0

        # Implementation only
        self.votedForMe = set()
        self.electionTimer = time()

        # Leader only
        self.nextIndex = {}
        self.matchIndex = {}
        self.leaderId = -1

        self.heartbeat_running = False
        self.election_timeout_running = True

        heartbeat_thread = Thread(target=self.heartbeat_thread)

        self.backlog = []

        heartbeat_thread.start()
        
    def heartbeat_thread(self) -> None:
        logging.info(vars(self.kv_store))
        if self.heartbeat_running: 
            for node in self.node_ids:
                if node != self.node_id:
                    send(self.node_id, node, type='AppendEntriesRPC',  ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=[],leaderCommit=self.commitIndex)
    
    def election_thread(self, timeout, executor) -> None:
        if self.election_timeout_running:
            if(time() - self.electionTimer >= timeout):
                self.change_role(RaftRole.CANDIDATE)

    def set_election_timer_running(self, value: bool) -> None:
        self.election_timeout_running = value

    def set_election_timer(self, timer: float) -> None:
        self.electionTimer = time()

    def set_heartbeat_running(self, value: bool) -> None:
        self.heartbeat_running = value

    def init(self, msg: Message) -> None:
        self.node_id = msg.body.node_id
        self.node_ids = msg.body.node_ids
        self.node_count = len(self.node_ids)
        reply(msg, type='init_ok')

    def read(self, msg: Message) -> None:
        value = self.kv_store.read(msg.body.key)

        if value is None:
            reply(msg, type='error', code=20)
        else:
            reply(msg, type='read_ok', value=value)

    def write(self, msg: Message) -> None:
        self.kv_store.write(msg.body.key, getattr(msg.body, 'value'))
        reply(msg, type='write_ok')

    def cas(self, msg: Message) -> bool:
        _from = getattr(msg.body, 'from')
        matches = self.kv_store.cas(msg.body.key, _from, msg.body.to)

        if matches is None:
            reply(msg, type='error', code=20)
            return False
        elif not matches:
            reply(msg, type='error', code=22)
            return False
        else:
            reply(msg, type='cas_ok')
            return True


    def fromClient(self, msg: Message, append: bool = True) -> None:
        last_log_index = len(self.log) - 1
        if self.node_id == self.leaderId:
            match msg.body.type:
                case 'read':
                    self.read(msg)
                case _:
                    self.log.append(Log(msg, self.currentTerm, last_log_index + 1))

            for node in self.node_ids:
                if node != self.node_id and last_log_index >= self.nextIndex[node]:
                    send(self.node_id, node, type='AppendEntriesRPC', ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=self.log[self.nextIndex[node]:],leaderCommit=self.commitIndex)

        elif self.leaderId != -1:
            if self.leaderId != self.node_id:
                logging.debug("Forward")
                send(self.node_id, self.leaderId, type="Forward", og=msg)
            else:
                self.fromClient(msg)
        elif append:
            logging.debug("Backlog")
            self.backlog.append(msg)

    def forward(self, msg: Message) -> None:
        logging.debug("Forwarded")
        msg.dest = self.node_id
        self.fromClient(msg.body.og)


    def check_term(self, term: int) -> None:
        if(term > self.currentTerm):
            self.votedFor = None
            self.currentTerm = term
            self.change_role(RaftRole.FOLLOWER)

    def change_role(self, role: RaftRole) -> None:
        match role:
            case RaftRole.LEADER:
                self.leaderId = self.node_id
                self.nextIndex = {k: len(self.log) for k in self.node_ids if k != self.node_id}
                self.matchIndex = {k: 0 for k in self.node_ids if k != self.node_id}
                self.clear_backlog()
                self.start_heartbeats()
                self.cancel_timeout_check()
            case RaftRole.CANDIDATE:
                self.start_election()
                self.start_timeout_check()
                self.cancel_heartbeats()
            case RaftRole.FOLLOWER:
                self.clear_backlog()
                self.start_timeout_check()
                self.cancel_heartbeats()

    def start_heartbeats(self) -> None:
        self.set_heartbeat_running(True)

    def cancel_heartbeats(self) -> None:
        self.set_heartbeat_running(False)

    def start_timeout_check(self) -> None:
        self.set_election_timer_running(True)

    def cancel_timeout_check(self) -> None:
        self.set_election_timer_running(False)

    def start_election(self) -> None:
        logging.info("Starting election")
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

    def clear_backlog(self) -> None:
        logging.debug("Clearing backlog")
        if self.leaderId != -1:
            logging.debug("Actually clearing")
            for msg in self.backlog:
                logging.debug("Forward")
                send(self.node_id, self.leaderId, type="Forward", og=msg)
            self.backlog = []
                


    def setCommitIndex(self, index: int) -> None:
        self.commitIndex = min(index, len(self.log) - 1)

        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            if self.node_id == self.leaderId:
                match self.log[self.lastApplied].message.body.type:
                    case 'write':
                        self.write(self.log[self.lastApplied].message)
                    case 'cas':
                        self.cas(self.log[self.lastApplied].message)
            else:
                self.kv_store.apply(self.log[self.lastApplied])

    def majority_index(self) -> int:
        left = 0
        right = len(self.log) - 1

        for i in range(0, right + 1):
            if self.log[i].term == self.currentTerm:
                left = i
                break

        while left < right:
            mid = (left + right) // 2
            logging.info("Hello")

            count = 0
            for node in self.node_ids:
                if node == self.node_id:
                    continue
                if self.matchIndex[node] >= mid:
                    count += 1

            if count >= (self.node_count + 1) // 2:
                left = mid + 1
            else:
                right = mid

        return max(left - 1, 0)


    def compute_majority(self):
        majority = self.majority_index()
        if majority > self.commitIndex:
            logging.info(f"Majority: {majority}, {self.matchIndex}")
            self.setCommitIndex(majority)

    def append_entries(self, msg: Message):
        self.check_term(msg.body.term)
        self.set_election_timer(time())
        if msg.body.term < self.currentTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return
        
        self.leaderId = msg.src
        self.clear_backlog()

        if len(self.log) <= msg.body.prevLogIndex or self.log[msg.body.prevLogIndex].term != msg.body.prevLogTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return

        entries = list(map(lambda e: Log.from_namespace(e), msg.body.entries))

        for i, entry in enumerate(entries):
            if self.conflicts(entry, msg.body.prevLogIndex + i + 1):
                self.log = self.log[:msg.body.prevLogIndex + i + 1]

        lastNew = 9223372036854775807
        logging.debug(f"{self.log} {msg.body.entries} {entries}")
        for i, entry in enumerate(entries):
            new = self.append_log(entry)
            if new:
                lastNew = msg.body.prevLogIndex + i + 1

        if msg.body.leaderCommit > self.commitIndex:
            self.setCommitIndex(min(msg.body.leaderCommit, lastNew))
        
        reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = True, ni=msg.body.ni, entries=msg.body.entries)



    def append_entries_reply(self, msg: Message):
        self.check_term(msg.body.term)
        
        if msg.body.success:
            self.compute_majority()
            self.nextIndex[msg.src]  = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries))
            self.matchIndex[msg.src] = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries) - 1)
        else:
            self.nextIndex[msg.src] -= 1
            reply(msg, type='AppendEntriesRPC', ni=self.nextIndex[msg.src], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[msg.src] - 1, prevLogTerm=self.log[self.nextIndex[msg.src] - 1].term, entries=self.log[self.nextIndex[msg.src]:],leaderCommit=self.commitIndex)

    def request_vote(self, msg: Message):
        self.set_election_timer(time())
        self.check_term(msg.body.term)

        if msg.body.term < self.currentTerm:
            logging.debug(f"Wrong term {msg.body.term} {self.currentTerm}")
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)
            return

        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)

        logging.debug(f"{self.votedFor} {msg.body.candidateId} {self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)}")
        if voteGranted:
            self.votedFor = msg.src

        reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = voteGranted)

    def request_vote_reply(self, msg: Message):
        self.check_term(msg.body.term)

        if msg.body.voteGranted:
            self.votedForMe.add(msg.src)

        if len(self.votedForMe) >= (self.node_count + 1) // 2:
            self.change_role(RaftRole.LEADER)

queue = Queue()
raft = Raft()

def election_probe():
    while True:
        queue.put("election")
        sleep(uniform(0.05, 0.15))

def heartbeat_probe():
    while True:
        queue.put("hearbeat")
        sleep(uniform(0.05, 0.15))


def process():
    timeout = uniform(0.55, 1.75)
    while True:
        msg = queue.get()

        if msg == "election":
            raft.election_thread(timeout, None)
            continue
        elif msg == "hearbeat":
            raft.heartbeat_thread()
            continue

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
            case 'Forward':
                raft.forward(msg)
            case _:
                raft.fromClient(msg)


def main():
    for msg in receiveAll():
        queue.put(msg)
    logging.debug("End")


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