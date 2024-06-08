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
        self.currentTerm = 0         # Latest term server has seen
        self.votedFor = None         # CandidateId that received vote in current term
        self.log = [Log(None, 0, 0)] # Artificial log entry so that Indexes start at 1

        self.commitIndex = 0         # Index of highest log entry known to be committed
        self.lastApplied = 0         # Index of highest log entry applied to state machine

        # Implementation only
        self.votedForMe = set()      # Set of nodes that voted for me
        self.electionTimer = time()  # Timer for election timeout

        # Leader only
        self.nextIndex = {}          # For each server, index of the next log entry to send to that server
        self.matchIndex = {}         # For each server, index of highest log entry known to be replicated on server
        self.leaderId = -1           # Current leader

        # To activate or deactivate the heartbeat and election threads
        self.heartbeat_running = False          
        self.election_timeout_running = True

        self.backlog = []           # Entries received when there is no leader

    def set_election_timer_running(self, value: bool) -> None:
        self.election_timeout_running = value

    def set_election_timer(self) -> None:
        self.electionTimer = time()

    def set_heartbeat_running(self, value: bool) -> None:
        self.heartbeat_running = value
        
    def start_heartbeats(self) -> None:
        self.set_heartbeat_running(True)
        
    def cancel_heartbeats(self) -> None:
        self.set_heartbeat_running(False)

    def start_timeout_check(self) -> None:
        self.set_election_timer_running(True)

    def cancel_timeout_check(self) -> None:
        self.set_election_timer_running(False)

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

    # When receiving message from client
    def fromClient(self, msg: Message) -> None:
        new_entry_index = len(self.log) # index of the new entry
        
        # When we are the leader
        if self.role == RaftRole.LEADER:
            self.log.append(Log(msg, self.currentTerm, new_entry_index))
            
            # Could also send immediate append entries to followers to reduce latency
            """
             for node in self.node_ids:
                if node != self.node_id and last_log_index >= self.nextIndex[node]:
                    send(self.node_id, node, type='AppendEntriesRPC', ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=self.log[self.nextIndex[node]:],leaderCommit=self.commitIndex)
            """
        
        # When we are not the leader, but we know a leader
        elif self.leaderId != -1:
            send(self.node_id, self.leaderId, type="Forward", og=msg)
        
        # When we don't know the leader  
        else: 
            self.backlog.append(msg)

    def forward(self, msg: Message) -> None:
        msg.dest = self.node_id
        
        # Treat the message as if it came from the client
        self.fromClient(msg.body.og)
        
    def heartbeat_thread(self) -> None:
        if self.heartbeat_running: 
            for node in self.node_ids:
                if node != self.node_id:
                    # In a heartbeat, we send to the follower entries we think are missing
                    send(self.node_id, node, type='AppendEntriesRPC', ni=self.nextIndex[node], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[node] - 1, prevLogTerm=self.log[self.nextIndex[node] - 1].term, entries=self.log[self.nextIndex[node]:],leaderCommit=self.commitIndex)

    def election_thread(self, timeout) -> None:
        if self.election_timeout_running:
            if(time() - self.electionTimer >= timeout):
                self.change_role(RaftRole.CANDIDATE)

    def check_term(self, term: int) -> None:
        if(term > self.currentTerm):
            self.votedFor = None
            self.currentTerm = term
            self.change_role(RaftRole.FOLLOWER)
            self.set_election_timer()

    def change_role(self, role: RaftRole) -> None:
        self.role = role
        match role:
            case RaftRole.LEADER:
                self.leaderId = self.node_id
                self.nextIndex = {k: len(self.log) for k in self.node_ids if k != self.node_id}
                self.matchIndex = {k: 0 for k in self.node_ids if k != self.node_id}
                self.clear_backlog()
                self.start_heartbeats()
                self.heartbeat_thread()     # Send a heartbeat to its followers
                self.cancel_timeout_check() # Leader doesn't timeout election timer
            case RaftRole.CANDIDATE:
                self.start_election()
                self.start_timeout_check()
                self.cancel_heartbeats()
            case RaftRole.FOLLOWER:
                self.clear_backlog()
                self.start_timeout_check()
                self.cancel_heartbeats()
                
    # Send all entries saved in the backlog to the leader, if any
    def clear_backlog(self) -> None:
        if self.leaderId != -1:
            for msg in self.backlog:
                send(self.node_id, self.leaderId, type="Forward", og=msg)
            self.backlog = []

    def start_election(self) -> None:
        self.currentTerm += 1
        self.votedFor = self.node_id
        self.votedForMe = set()
        self.votedForMe.add(self.node_id)

        self.set_election_timer()

        # Broadcast RequestVoteRPC
        for node in self.node_ids:
            if node != self.node_id:
                send(self.node_id, node, type='RequestVoteRPC', term=self.currentTerm, \
                    candidateId=self.node_id, lastLogIndex = len(self.log), lastLogTerm = self.log[-1].term)
    
    def request_vote(self, msg: Message):
        self.check_term(msg.body.term) # If receiving message from a leader with a higher term, convert to follower

        # If the candidate's term is less than the current term, it is rejected
        if msg.body.term < self.currentTerm:
            reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = False)
            return

        # Vote is granted if:
        # 1. Receiver hasn't voted for another candidate
        # 2. Candidate's log is at least as up-to-date as receiver's log
        voteGranted = (self.votedFor is None or self.votedFor == msg.body.candidateId) and \
            self.more_up_to_date(msg.body.lastLogIndex, msg.body.lastLogTerm)

        if voteGranted:
            self.set_election_timer()
            self.votedFor = msg.src

        reply(msg, type='RequestVoteRPCReply', term=self.currentTerm, voteGranted = voteGranted)

    def request_vote_reply(self, msg: Message):
        # Shouldn't proccess if not a candidate or if the term is different
        if self.role != RaftRole.CANDIDATE or msg.body.term != self.currentTerm:
            self.check_term(msg.body.term)
            return

        if msg.body.voteGranted:
            self.votedForMe.add(msg.src)

        # When the candidate receives votes from a majority of the servers, it becomes the leader
        if len(self.votedForMe) >= (self.node_count + 2) // 2:
            self.change_role(RaftRole.LEADER)

    # True if candidate's log is at least as up-to-date as receiver's log, False otherwise
    def more_up_to_date(self, lastLogIndex : int, lastLogTerm : int) -> bool:
        # If terms are different, the candidate with the most recent term wins
        if self.log[-1].term < lastLogTerm:
            return True

        if self.log[-1].term > lastLogTerm:
            return False

        # If terms are the same, the candidate with the longest log wins
        return len(self.log) <= lastLogIndex

    def append_entries(self, msg: Message):
        self.check_term(msg.body.term)
        
        if self.role == RaftRole.CANDIDATE:
            self.change_role(RaftRole.FOLLOWER)
        
        # Message from an outdated leader -> ignore
        if msg.body.term < self.currentTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return
        
        # Received message from a valid leader -> reset election timer
        self.set_election_timer()
        
        self.leaderId = msg.src
        self.clear_backlog()
        
        # If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm -> reject
        if len(self.log) <= msg.body.prevLogIndex or self.log[msg.body.prevLogIndex].term != msg.body.prevLogTerm:
            reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = False)
            return

        entries = list(map(lambda e: Log.from_namespace(e), msg.body.entries))

        index = 0 # This is the index where the conflict occurs
        for i, entry in enumerate(entries):
            if self.conflicts(entry, msg.body.prevLogIndex + i + 1):
                # When a conflict is found, delete the conflicting entry and all that follow it
                self.log = self.log[:msg.body.prevLogIndex + i + 1]
                index = i+1
                break

        # Append new entries not already in the log
        self.log = self.log + entries[index:]
        
        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if msg.body.leaderCommit > self.commitIndex:
            self.setCommitIndex(min(msg.body.leaderCommit, len(self.log)-1))
        
        reply(msg, type='AppendEntriesRPCReply', term = self.currentTerm, success = True, ni=msg.body.ni, entries=msg.body.entries)

    def append_entries_reply(self, msg: Message):
        # If received a reply (false) from a server with a higher term, convert to follower
        self.check_term(msg.body.term)
        
        if msg.body.success and self.role == RaftRole.LEADER and msg.body.term == self.currentTerm:
            self.compute_majority()
            
            # Next index is updated to the last entry the follower could add in his log
            self.nextIndex[msg.src]  = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries))
            self.matchIndex[msg.src] = max(self.nextIndex[msg.src], msg.body.ni + len(msg.body.entries) - 1)
        elif self.role == RaftRole.LEADER and msg.body.term == self.currentTerm:
            self.nextIndex[msg.src] -= 1
            reply(msg, type='AppendEntriesRPC', ni=self.nextIndex[msg.src], term = self.currentTerm, leaderId=self.node_id, prevLogIndex=self.nextIndex[msg.src] - 1, prevLogTerm=self.log[self.nextIndex[msg.src] - 1].term, entries=self.log[self.nextIndex[msg.src]:],leaderCommit=self.commitIndex)
    
    # Check if there is a conflict in a given index
    def conflicts(self, entry: Log, index: int) -> bool:
        # Conflict when:
        # 1. The index is in the log
        # and
        # 2. The term of the entry in the index is different from the term of the entry to be inserted
        return index < len(self.log) and self.log[index].term != entry.term

    def setCommitIndex(self, index: int) -> None:
        if index < self.commitIndex:
            return 
        self.commitIndex = index

        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            if self.node_id == self.leaderId:
                match self.log[self.lastApplied].message.body.type:
                    case 'write':
                        self.write(self.log[self.lastApplied].message)
                    case 'read':
                        self.read(self.log[self.lastApplied].message)
                    case 'cas':
                        self.cas(self.log[self.lastApplied].message)
            else:
                self.kv_store.apply(self.log[self.lastApplied])

    def majority_index(self) -> int:
        left = 0
        right = len(self.log) 

        # Encontrar primeira entrada do termo atual
        for i in range(0, right):
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

            if count >= (self.node_count) // 2:
                left = mid + 1
            else:
                right = mid

        return max(left - 1, 0)

    def compute_majority(self):
        majority = self.majority_index()
        if majority > self.commitIndex:
            self.setCommitIndex(majority)

queue = Queue()
raft = Raft()

def election_probe():
    while True:
        queue.put("election")
        sleep(uniform(0.05, 0.15))

def heartbeat_probe():
    while True:
        queue.put("heartbeat")
        sleep(uniform(0.05, 0.15))


def process():
    timeout = uniform(0.55, 1.75)
    while True:
        msg = queue.get()

        if msg == "election":
            raft.election_thread(timeout)
            continue
        elif msg == "heartbeat":
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