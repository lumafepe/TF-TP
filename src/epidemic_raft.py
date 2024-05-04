#!/usr/bin/env python

import logging
from key_value import KVStore
from threading import Thread
from time import sleep
from enum import Enum
from time import time
from log import Log
from random import uniform, shuffle
from queue import Queue
from math import log2
from ms import receiveAll, reply, send, forward, Message

logging.getLogger().setLevel(logging.INFO)

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
        self.roundLC = 0
        self.bitmap = []
        self.nextCommit = 1
        self.maxCommit = 0

        #TODO: Make persistent
        self.currentTerm = 0
        self.votedFor = None
        self.log = [Log(None, 0, 0)]

        self.commitIndex = 0
        self.lastApplied = 0
        self.c = 0
        self.fanout = 0
        self.node_permutation = []

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
        if self.heartbeat_running: 
            # Cause leader to randomly fail
            #if uniform(0, 1) >= 0.8 and self.currentTerm == 1:
            #    exit(0)
            self.roundLC += 1
            if self.commitIndex < len(self.log) - 1:
                self.gossip_kwargs(type='AppendEntriesRPC', ni=0, term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.commitIndex - 1, \
                    prevLogTerm=self.log[self.commitIndex - 1].term, entries=self.log[self.commitIndex:],leaderCommit=self.commitIndex, leaderRound=self.roundLC , \
                        isRPC = False, bitmap=self.bitmap, nextCommit=self.nextCommit, maxCommit=self.maxCommit)
            else:
                self.gossip_kwargs(type='AppendEntriesRPC',  ni=0, term = self.currentTerm, leaderId=self.node_id, prevLogIndex=0, prevLogTerm=0, \
                    entries=[],leaderCommit=self.commitIndex, leaderRound=self.roundLC, isRPC=False, bitmap=self.bitmap, \
                        nextCommit=self.nextCommit, maxCommit=self.maxCommit)
    
    def election_thread(self, timeout, executor):
        logging.info(self.kv_store.store)
        if self.election_timeout_running:
            if(time() - self.electionTimer >= timeout):
                self.change_role(RaftRole.CANDIDATE)

    def set_election_timer_running(self, value: bool):
        self.election_timeout_running = value

    def set_election_timer(self, timer: float):
        self.electionTimer = time()

    def set_heartbeat_running(self, value: bool):
        self.heartbeat_running = value

    def init(self, msg: Message):
        self.node_id = msg.body.node_id
        self.node_ids = msg.body.node_ids
        self.node_count = len(self.node_ids)
        self.fanout = round(log2(self.node_count))
        self.node_permutation = [i for i in self.node_ids if i != self.node_id]
        shuffle(self.node_permutation)
        reply(msg, type='init_ok')

    def read(self, msg: Message):
        value = self.kv_store.read(msg.body.key)

        if value is None:
            reply(msg, type='error', code=20)
        else:
            reply(msg, type='read_ok', value=value)

    def write(self, msg: Message):
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

    def forward(self, msg: Message):
        logging.debug("Forwarded")
        msg.dest = self.node_id
        self.fromClient(msg.body.og)


    def fromClient(self, msg: Message, append: bool = True):
        last_log_index = len(self.log) - 1
        if self.node_id == self.leaderId:
            match msg.body.type:
                case 'read':
                    self.read(msg)
                case 'write':
                    self.write(msg)
                    self.log.append(Log(msg, self.currentTerm, last_log_index + 1))
                case 'cas':
                    if self.cas(msg):
                         self.log.append(Log(msg, self.currentTerm, last_log_index + 1))

            self.compute_majority()

            self.roundLC += 1
            self.gossip_kwargs(type='AppendEntriesRPC', ni=0, term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.commitIndex - 1, \
                    prevLogTerm=self.log[self.commitIndex - 1].term, entries=self.log[self.commitIndex:],leaderCommit=self.commitIndex, leaderRound=self.roundLC , \
                        isRPC = False, bitmap=self.bitmap, nextCommit=self.nextCommit, maxCommit=self.maxCommit)

        elif self.leaderId != -1:
            if self.leaderId != self.node_id:
                send(self.node_id, self.leaderId, type="Forward", og=msg)
            else:
                self.fromClient(msg)
        elif append:
            logging.debug("Backlog")
            self.backlog.append(msg)


    def set_term(self, term: int) -> None:
        self.currentTerm = term
        self.votedFor = None
        self.roundLC = 0
        self.nextCommit = self.maxCommit + 1
        self.bitmap = [0 for i in self.node_ids]

    def check_term(self, term: int) -> None:
        if(term > self.currentTerm):
            self.set_term(term)
            self.change_role(RaftRole.FOLLOWER)

    def change_role(self, role: RaftRole):
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

    def start_heartbeats(self):
        self.set_heartbeat_running(True)

    def cancel_heartbeats(self):
        self.set_heartbeat_running(False)

    def start_timeout_check(self):
        self.set_election_timer_running(True)

    def cancel_timeout_check(self):
        self.set_election_timer_running(False)

    def start_election(self):
        logging.info("Starting election")
        self.set_term(self.currentTerm + 1)
        
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

    def update(self, maxCommit: int, nextCommit: int, bitmap: list[int]) -> None:
        if sum(self.bitmap) >= (self.node_count + 1) // 2:
            self.maxCommit = self.nextCommit
            self.bitmap = [0 for i in self.node_ids]
            if self.nextCommit >= len(self.log) - 1 or self.log[-1].term != self.currentTerm:
                self.nextCommit += 1
            else:
                self.nextCommit = len(self.log) - 1
                self.bitmap[self.node_ids.index(self.node_id)] = 1


    def merge(self, maxCommit: int, nextCommit: int, bitmap: list[int]) -> None:
        self.maxCommit = max(self.maxCommit, maxCommit)
        if self.nextCommit <= nextCommit:
            self.bitmap = [a | b for a, b in zip(self.bitmap, bitmap)]
        if self.nextCommit <= self.maxCommit:
            self.bitmap = bitmap
            self.nextCommit = nextCommit

    def conflicts(self, entry: Log, index: int) -> bool:
        return index < len(self.log) and self.log[index].term != entry.term

    def append_log(self, entry: Log) -> bool:
        new = entry not in self.log

        if new:
            self.log.append(entry)

        return new


    def gossip(self, msg: Message):
        for i in range(self.fanout):
            body = vars(msg.body)
            if "msg_id" in body:
                del body["msg_id"]
            send(self.node_id, self.node_permutation[(self.c + i) % (self.node_count - 1)], **body)
        self.c += self.fanout
        self.c %= (self.node_count - 1)


    def gossip_kwargs(self, **body: Message):
        for i in range(self.fanout):
            send(self.node_id, self.node_permutation[(self.c + i) % (self.node_count - 1)], **body)
        self.c += self.fanout
        self.c %= (self.node_count - 1)

    def clear_backlog(self):
        logging.debug("Clearing backlog")
        if self.leaderId != -1:
            logging.debug("Actually clearing")
            for msg in self.backlog:
                send(self.node_id, self.leaderId, type="Forward", og=msg)
            self.backlog = []
                


    def setCommitIndex(self, index: int):
        self.commitIndex = min(index, len(self.log) - 1)

        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
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

        return max(left, 0)


    def compute_majority(self) -> None:
        majority = self.majority_index()
        if majority > self.commitIndex:
            self.setCommitIndex(majority)

    def append_entries(self, msg: Message) -> None:
        self.check_term(msg.body.term)
        if msg.body.term < self.currentTerm:
            send(self.node_id, self.leaderId, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return

        self.set_election_timer(time())

        if self.leaderId == self.node_id:
            self.merge(msg.body.nextCommit, msg.body.maxCommit, msg.body.bitmap)
            return
        else:
            self.update(msg.body.nextCommit, msg.body.maxCommit, msg.body.bitmap)
        
        if msg.body.leaderRound <= self.roundLC and not msg.body.isRPC:
            return

        needs_reply = msg.body.leaderRound > self.roundLC or msg.body.isRPC

        
        if not msg.body.isRPC:
            self.roundLC = msg.body.leaderRound

        self.leaderId = msg.body.leaderId
        self.clear_backlog()

        if len(self.log) <= msg.body.prevLogIndex or self.log[msg.body.prevLogIndex].term != msg.body.prevLogTerm:
            if needs_reply:
                send(self.node_id, self.leaderId, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return
        
        entries = list(map(lambda e: Log.from_namespace(e), msg.body.entries))

        for i, entry in enumerate(entries):
            if self.conflicts(entry, msg.body.prevLogIndex + i + 1):
                self.log = self.log[:msg.body.prevLogIndex + i + 1]

        for i, entry in enumerate(entries):
            new = self.append_log(entry)

        self.update(msg.body.nextCommit, msg.body.maxCommit, msg.body.bitmap)

        if self.currentTerm == self.log[-1].term:
            self.setCommitIndex(max(len(self.log) - 1, self.maxCommit))

        if self.nextCommit <= len(self.log) - 1 and self.log[self.nextCommit].term == self.currentTerm:
            self.bitmap[self.node_ids.index(self.node_id)] = 1

        if not msg.body.isRPC:
            self.gossip_kwargs(type='AppendEntriesRPC', term=self.currentTerm, leaderId=self.leaderId, prevLogIndex=msg.body.prevLogIndex, \
                prevLogTerm=msg.body.prevLogTerm, entries=msg.body.entries, leaderRound = self.roundLC, isRPC=False, bitmap=self.bitmap, \
                    maxCommit = self.maxCommit, nextCommit = self.nextCommit)



    def append_entries_reply(self, msg: Message) -> None:
        self.check_term(msg.body.term)
        
        if msg.body.success:
            self.nextIndex[msg.src]  = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries))
            self.matchIndex[msg.src] = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries) - 1)
        else:
            self.nextIndex[msg.src] -= 1
            reply(msg, type='AppendEntriesRPC', ni=self.nextIndex[msg.src], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[msg.src] - 1, prevLogTerm=self.log[self.nextIndex[msg.src] - 1].term, entries=self.log[self.nextIndex[msg.src]:],leaderCommit=self.commitIndex, leaderRound=self.roundLC, isRPC=True)

    def request_vote(self, msg: Message):
        self.set_election_timer(time())
        self.check_term(msg.body.term)

        if msg.body.term < self.currentTerm:
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)
            return

        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)

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