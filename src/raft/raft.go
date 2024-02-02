package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sort"

	// "bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry is a struct for raft log
type LogEntry struct {
	Data  interface{}
	Term  int
	Index int
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state
	currentTerm int
	voteFor     int // -1 for null
	log         []LogEntry
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leader
	nextIndex  []int
	matchIndex []int
	// the field I add
	serverState ServerState
	// XXX(zhr): the guideline says `Don't use Go's time.Timer`
	electionTimer time.Time
	// the votes get in elections
	votes    int
	leaderId int
	// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages
	applyCh chan ApplyMsg
	// lastIncludedTerm by snapshot
	lastIncludedTerm int
	// lastIncludedIndex by snapshot
	lastIncludedIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.serverState == Leader
	rf.mu.RUnlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	raftState := rf.encodePersistState()
	rf.persister.Save(raftState, rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	raftState := rf.encodePersistState()
	rf.persister.Save(raftState, snapshot)
}

func (rf *Raft) encodePersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		rf.debug("[persist] persist currentTerm fail")
		return nil
	}
	if err := e.Encode(rf.voteFor); err != nil {
		rf.debug("[persist] persist voteFor fail")
		return nil
	}
	if err := e.Encode(rf.log); err != nil {
		rf.debug("[persist] persist log fail")
		return nil
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		rf.debug("[persist] persist lastIncludedIndex fail")
		return nil
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		rf.debug("[persist] persist lastIncludedTerm fail")
		return nil
	}
	raftState := w.Bytes()
	return raftState
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(Data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		rf.debug("[readPersist] fail to read persist info")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastApplied
	}
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	indexPosition := rf.getIndexPos(index)
	if indexPosition < 0 {
		// rf.debug("[Snapshot] the snapshot is out of date")
		return
	}
	newLastIncludedTerm := rf.log[indexPosition].Term
	rf.deleteCompactedLog(indexPosition)
	// update the raft status
	rf.lastIncludedTerm = newLastIncludedTerm
	rf.lastIncludedIndex = index
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// rf.debug(fmt.Sprintf("[Snapshot] after receive snapshot: lastIncludedTerm: %d, "+
	//	"lastIncludedIndex: %d, commitIndex: %d, lastIncludedIndex: %d",
	//	rf.lastIncludedTerm, rf.lastIncludedIndex, rf.commitIndex, rf.lastApplied))
	rf.persistWithSnapshot(snapshot)
	// rf.debug(fmt.Sprintf("[Snapshot] finish persistWithSnapshot, the snapshot: %+v", rf.persister.ReadSnapshot()))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	// candidate's Term
	Term int
	// candidate requesting vote
	CandidateId int
	// Index of candidate's last log entry
	LastLogIndex int
	// Term of candidate's last log entry
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your Data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// leader’s Term
	Term int
	// so follower can redirect clients
	LeaderId int
	// Index of log entry immediately preceding new ones
	PrevLogIndex int
	// Term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []LogEntry
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	// the Term of the conflicting entry and the first Index it stores for that Term
	// to reduce the number of rejected AppendEntries RPCs
	ConflictTerm int
	// ConflictIndex index of first entry with that term (if any)
	ConflictIndex int
	// log length
	ConflictLength int
}

type InstallSnapshotArgs struct {
	// leader’s term
	Term int
	// follower can redirect client
	LeaderId int
	// the snapshot replaces all entries up through and including this index
	LastIncludedIndex int
	// term of lastIncludedIndex
	LastIncludedTerm int
	// raw bytes of the snapshot chunk, starting at offset
	Data []byte
}

type InstallSnapshotReply struct {
	// currentTerm, for leader to update itself
	Term int
}

// some helper function

func (rf *Raft) convertToFollower(isChangeTerm bool) {
	rf.serverState = Follower
	rf.votes = 0
	// NOTE(zhr): voteFor indicates candidateId that received vote in current term, if term not change, no need to reset voteFor
	if isChangeTerm {
		rf.voteFor = -1
	}

}

func (rf *Raft) convertToCandidate() {
	rf.serverState = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.electionTimer = time.Now()
	rf.votes = 1
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.serverState = Leader
	rf.voteFor = -1
	rf.persist()

	// reinitialize the leader's volatile states
	leaderLastLogIndex := 1
	if len(rf.log) > 0 {
		leaderLastLogIndex = rf.log[len(rf.log)-1].Index
	}
	serverCount := len(rf.nextIndex)
	for i := 0; i < serverCount; i++ {
		rf.nextIndex[i] = leaderLastLogIndex
		rf.matchIndex[i] = 0
	}
	if len(rf.log) > 0 {
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	}
}

func (rf *Raft) getLastIndexAndTerm() (int, int) {
	selfLastLogIndex := 0
	selfLastLogTerm := -1
	logLength := len(rf.log)
	if logLength > 0 {
		selfLastLogTerm = rf.log[logLength-1].Term
		selfLastLogIndex = rf.log[logLength-1].Index
	} else {
		selfLastLogTerm = rf.lastIncludedTerm
		selfLastLogIndex = rf.lastIncludedIndex
	}
	return selfLastLogIndex, selfLastLogTerm
}

func (rf *Raft) getMajorityVote() bool {
	serverCount := len(rf.peers)
	return rf.votes*2 > serverCount
}

func (rf *Raft) debug(message string) {
	pid := os.Getpid()

	serverStatus := "Leader"
	if rf.serverState == Follower {
		serverStatus = "Follower"
	} else if rf.serverState == Candidate {
		serverStatus = "Candidate"
	}
	logMessage := fmt.Sprintf("[pid %d] [ServerId: %d] [ServerStatus %s] [Term %d] %s\n", pid, rf.me, serverStatus, rf.currentTerm, message)
	fmt.Printf(logMessage)
}

// checkLogMatch check whether log contains an entry at prevLogIndex whose Term matches prevLogTerm
// TODO(zhr): need to change after applying snapshot, current Index equal to position in log
func (rf *Raft) checkLogMatch(args *AppendEntriesArgs) (bool, int, int, int) {
	if args.PrevLogIndex < 0 {
		// rf.debug("[checkLogMatch] there must be something wrong for AppendEntriesArgs")
		return false, 0, 0, 0
	}

	// if the leader's log start from the beginning, return true
	if args.PrevLogIndex == 0 {
		return true, 0, 0, 0
	}

	conflictTerm := -1
	conflictIndex := -1
	conflictLength := -1

	isMatch := true
	entryPosition := rf.getIndexPos(args.PrevLogIndex)
	// rf.debug(fmt.Sprintf("[checkLogMatch] args.PrevLogIndex: %d, args.PrevLogTerm: %d, entryPosition: %d, logs: %+v", args.PrevLogIndex, args.PrevLogTerm, entryPosition, rf.log))
	if entryPosition == -1 {
		// the leader's log is too new
		// if the leader just has no new log, return true
		// just the snapshot?
		if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm == rf.lastIncludedTerm {
			isMatch = true
		} else {
			isMatch = false
			// XXX(zhr): maybe need to change after applying the snapshot
			conflictLength = len(rf.log) + rf.lastIncludedIndex
		}
	} else if rf.log[entryPosition].Term != args.PrevLogTerm {
		// optimize by include the term of the conflicting entry and the first index it stores for that term
		isMatch = false
		conflictTerm = rf.log[entryPosition].Term
		conflictIndex = rf.log[entryPosition].Index
		for i := entryPosition; i >= 0; i-- {
			if rf.log[i].Term != conflictTerm {
				break
			}
			conflictIndex = rf.log[i].Index
		}
	}

	return isMatch, conflictIndex, conflictTerm, conflictLength
}

// getIndexPos convert Index to the entry position in log, prepare for snapshot
// NOTE(zhr): the first index is 1
func (rf *Raft) getIndexPos(index int) int {
	relativePosition := index - rf.lastIncludedIndex
	if relativePosition < 0 || relativePosition > len(rf.log) {
		return -1
	}
	return relativePosition - 1
}

func (rf *Raft) getMajorCommit() int {
	serverCount := len(rf.peers)
	commitIndexCopy := make([]int, serverCount)
	copy(commitIndexCopy, rf.matchIndex)
	sort.Ints(commitIndexCopy)
	return commitIndexCopy[serverCount/2]
}

func (rf *Raft) updateCommitIndex() {
	majorCommit := rf.getMajorCommit()
	// XXX(zhr): maybe need to change after applying snapshot
	commitPosition := rf.getIndexPos(majorCommit)
	if majorCommit > rf.commitIndex && rf.log[commitPosition].Term == rf.currentTerm {
		rf.commitIndex = majorCommit
	}
}

func (rf *Raft) handleApplyEntries() {
	rf.mu.Lock()
	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	commitIndex := rf.commitIndex
	// rf.debug("begin apply entries")
	// increase lastApplied
	rf.lastApplied += 1
	entriesForApply := make([]LogEntry, commitIndex-rf.lastApplied+1)
	copy(entriesForApply, rf.log[rf.getIndexPos(rf.lastApplied):rf.getIndexPos(rf.commitIndex)+1])
	// rf.debug(fmt.Sprintf("[handleApplyEntries] log for apply: %+v\n", entriesForApply))
	rf.mu.Unlock()
	for _, entry := range entriesForApply {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Data,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		rf.applyCh <- applyMsg
		// rf.debug(fmt.Sprintf("[handleApplyEntries] commitIndex %d, data: %+v", applyMsg.CommandIndex, applyMsg.Command))
	}

	rf.mu.Lock()
	// update lastApplied
	rf.lastApplied = commitIndex
	rf.mu.Unlock()
}

// deleteCompactedLog deleted the compacted entries [0, pos], and ensure the memory can be released
func (rf *Raft) deleteCompactedLog(pos int) {
	// FIXME(zhr): whether need deep copy here?
	rf.log = rf.log[pos+1:]
}

// applySnapshot reset the state machine using the snapshot
func (rf *Raft) applySnapshot(args *InstallSnapshotArgs) {
	rf.mu.RLock()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	if msg.Snapshot == nil {
		panic("the snapshot cannot be nil")
	}
	rf.mu.RUnlock()
	rf.applyCh <- msg
}

// end helper function

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rf.debug(fmt.Sprintf("get request vote, data: %+v", args))
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// rf.debug("get the lock")
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		// rf.debug(fmt.Sprintf("get request from %d, but Term too small", args.CandidateId))
		return
	}

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// rf.debug(fmt.Sprintf("get request from %d, current Term too small, convert to follower", args.CandidateId))
		rf.convertToFollower(true)
		rf.persist()
	}

	// If votedFor is null or candidateId
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		// if candidate's log is at least as up-to-update
		selfLastLogIndex, selfLastLogTerm := rf.getLastIndexAndTerm()
		// rf.debug(fmt.Sprintf("selfLastLogIndex: %d, selfLastLogTerm: %d, args.LastLogIndex: %d, args.LastLogTerm: %d", selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm))
		if args.LastLogTerm > selfLastLogTerm || (args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex) {
			// rf.debug(fmt.Sprintf("vote for %d", args.CandidateId))
			rf.voteFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			return
		}
	}

	reply.VoteGranted = false
	return
}

// AppendEntries RPC handler
// XXX: leaderId in args is unused, maybe there is something incorrect
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower(true)
		rf.persist()
	}

	// update election timeout
	rf.electionTimer = time.Now()
	// rf.debug("update election timer")
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
		rf.convertToFollower(false)
	}

	// NOTE(zhr): according to the guidance, for heartbeat, also need to check prevLogIndex and prevLogTerm
	// Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm
	isMatch, conflictIndex, conflictTerm, conflictLength := rf.checkLogMatch(args)
	if !isMatch {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = conflictTerm
		reply.ConflictLength = conflictLength
		// rf.debug(fmt.Sprintf("AppendEntries fail, args: %+v, reply: %+v logs: %+v", args, reply, rf.log))
		return
	}

	reply.Success = true

	// delete all unmatched entries in receiver and existed entries in args
	// FIXME(zhr): maybe some bug, the committed log is not consistent
	newEntryPos := 0
	for _, entry := range args.Entries {
		entryPosition := rf.getIndexPos(entry.Index)
		if entryPosition == -1 {
			// the leader's log is longer, no need to delete
			break
		}
		if entry.Term == rf.log[entryPosition].Term {
			newEntryPos += 1
		} else {
			rf.log = rf.log[:entryPosition]
			break
		}
	}

	// Append any new entries not already in the log
	entries := args.Entries[newEntryPos:]
	rf.log = append(rf.log, entries...)
	rf.persist()
	//if len(entries) > 0 {
	//	rf.debug(fmt.Sprintf("[AppendEntries] replicate log success, args: %+v, entries: %+v, logs: %+v", args, entries, rf.log))
	//}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, Index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex
		if len(entries) > 0 {
			lastNewIndex = entries[len(entries)-1].Index
		}
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		// rf.debug(fmt.Sprintf("[AppendEntries] update commit index to: %d, logs: %+v\n, args: %+v", rf.commitIndex, rf.log, args))
	}
}

// handleAppendEntries responsible for sending AppendEntries RPC to followers and handle the response
func (rf *Raft) handleAppendEntries(id int, isHeartBeat bool) {
	for {
		// check whether we need to send snapshot
		rf.mu.RLock()
		if rf.serverState != Leader {
			rf.mu.RUnlock()
			break
		}
		// rf.debug(fmt.Sprintf("[handleAppendEntries] id: %d, rf.nextIndex[id]: %d, rf.lastIncludedIndex: %d", id, rf.nextIndex[id], rf.lastIncludedIndex))
		if rf.nextIndex[id] <= rf.lastIncludedIndex && rf.lastIncludedIndex != 0 {
			rf.mu.RUnlock()
			// rf.debug(fmt.Sprintf("[handleAppendEntries] the follower %d's log is too late, send snapshot, follower's nextIndex: %d, rf.lastIncludedIndex: %d", id, rf.nextIndex[id], rf.lastIncludedIndex))
			rf.handleInstallSnapshot(id)
			break
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[id] - 1,
			PrevLogTerm:  -1,
			LeaderCommit: rf.commitIndex,
		}
		if args.PrevLogIndex > 0 {
			startPosition := rf.getIndexPos(args.PrevLogIndex)
			if startPosition != -1 {
				args.PrevLogTerm = rf.log[startPosition].Term
				// need deep copy here
				if !isHeartBeat {
					args.Entries = make([]LogEntry, len(rf.log)-startPosition-1)
					copy(args.Entries, rf.log[startPosition+1:])
				}
			} else if args.PrevLogIndex == rf.lastIncludedIndex {
				args.PrevLogTerm = rf.lastIncludedTerm
				if !isHeartBeat {
					args.Entries = make([]LogEntry, len(rf.log))
					copy(args.Entries, rf.log[:])
				}
			}
		} else if args.PrevLogIndex == 0 && !isHeartBeat {
			args.Entries = make([]LogEntry, len(rf.log))
			copy(args.Entries, rf.log[:])
		}

		rf.mu.RUnlock()
		reply := AppendEntriesReply{}
		// rf.debug(fmt.Sprintf("[handleAppendEntries] id: %d, args: %+v", id, args))
		ret := rf.sendAppendEntries(id, &args, &reply)
		if ret {
			// rf.debug("[handleAppendEntries] sendAppendEntries success")
			rf.mu.Lock()
			// rf.debug("[handleAppendEntries] sendAppendEntries success, get the lock")
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			// rf.debug(fmt.Sprintf("[handleAppendEntries] id: %d, args: %+v, reply: %+v", id, args, reply))
			if reply.Term > rf.currentTerm {
				// rf.debug(fmt.Sprintf("[handleAppendEntries] convert to follower, new term: %+v", reply.Term))
				rf.currentTerm = reply.Term
				rf.convertToFollower(true)
				rf.mu.Unlock()
				return
			}

			if rf.serverState != Leader || isHeartBeat {
				rf.mu.Unlock()
				return
			}

			// If successful: update nextIndex and matchIndex for follower
			if reply.Success {
				if len(args.Entries) > 0 {
					rf.nextIndex[id] = args.Entries[len(args.Entries)-1].Index + 1
					rf.matchIndex[id] = rf.nextIndex[id] - 1
					rf.updateCommitIndex()
					// rf.debug(fmt.Sprintf("[handleAppendEntries] update follower %d info, nextIndex %d, matchIndex %d, commitIndex %d", id, rf.nextIndex[id], rf.matchIndex[id], rf.commitIndex))
				}
				rf.mu.Unlock()
				return
			} else {
				// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				// decrement nextIndex to bypass all of the conflicting entries in that term
				// skip the conflict term directly
				//if reply.ConflictIndex <= 0 {
				//	rf.nextIndex[id] = 0
				//} else {
				//	noConflictIndexPos := rf.getIndexPos(reply.ConflictIndex - 1)
				//	rf.nextIndex[id] = rf.log[noConflictIndexPos].Index
				//}
				// XXX(zhr): maybe need to change after applying snapshot
				// case3: follower's log is too short:
				// rf.debug(fmt.Sprintf("[handleAppendEntries] handle id: %d fail, args: %+v, reply info: %+v, leader log info: %+v", id, args, reply, rf.log))
				if reply.ConflictTerm < 0 {
					rf.nextIndex[id] = reply.ConflictLength
				} else {
					// case2: leader has ConflictTerm, nextIndex = leader's last entry for ConflictTerm
					startPosition := rf.getIndexPos(rf.nextIndex[id])
					if startPosition <= 0 {
						startPosition = len(rf.log) - 1
					}
					found := false
					for ; startPosition >= 0; startPosition-- {
						if rf.log[startPosition].Term == reply.ConflictTerm {
							found = true
							rf.nextIndex[id] = rf.log[startPosition].Index + 1
							break
						}
					}
					// case1: leader doesn't have ConflictTerm, nextIndex = ConflictIndex
					if !found {
						rf.nextIndex[id] = reply.ConflictIndex
					}
				}
				// rf.debug(fmt.Sprintf("[handleAppendEntries] update id %d's nextIndex to %d", id, rf.nextIndex[id]))
				rf.mu.Unlock()
				continue
			}
		}
		break
	}
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower(true)
		rf.persist()
	}

	// update election timeout
	rf.electionTimer = time.Now()

	// decide what to do with its existing log entries
	leaderIndexPosition := rf.getIndexPos(args.LastIncludedIndex)
	// case1: snapshot contains new information not already in the recipient’s log,
	// follower discards its entire log
	if leaderIndexPosition == -1 {
		if args.LastIncludedIndex > rf.lastIncludedIndex {
			rf.deleteCompactedLog(len(rf.log) - 1)
			// rf.debug("[InstallSnapshot] delete entire log")
		} else {
			// the snapshot is not correct
			// rf.debug(fmt.Sprintf("[InstallSnapshot] the snapshot is out of date, args lastIncludedIndex: %d, follower lastIncludedIndex: %d", args.LastIncludedIndex, rf.lastIncludedIndex))
			rf.mu.Unlock()
			return
		}
	} else {
		// case2: If instead the follower receives a snapshot that describes a prefix of its log (due to retransmission or by mistake),
		// then log entries covered by the snapshot are deleted but entries following the snapshot are still valid and must be retained.
		rf.deleteCompactedLog(leaderIndexPosition)
		// rf.debug(fmt.Sprintf("[InstallSnapshot] delete log from %d", leaderIndexPosition))
	}

	// update the raft status
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persistWithSnapshot(args.Data)
	// rf.debug(fmt.Sprintf("[InstallSnapshot] after install snapshot, lastIncludedTerm: %d, "+
	//	"lastIncludedIndex: %d, commitIndex: %d, lastIncludedIndex: %d",
	//	rf.lastIncludedTerm, rf.lastIncludedIndex, rf.commitIndex, rf.lastApplied))

	// reset the state machine using snapshot content
	rf.mu.Unlock()
	go rf.applySnapshot(args)

}

// handleInstallSnapshot responsible for sending InstallSnapshot RPC to followers and handle the response
func (rf *Raft) handleInstallSnapshot(id int) {
	rf.mu.RLock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.RUnlock()
	reply := InstallSnapshotReply{}
	if args.Data == nil {
		panic("[handleInstallSnapshot] the snapshot cannot be nil")
	}
	// rf.debug(fmt.Sprintf(fmt.Sprintf("[handleInstallSnapshot] arg: %+v", args)))
	ret := rf.sendInstallSnapshot(id, &args, &reply)
	if ret {
		rf.mu.Lock()
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.convertToFollower(true)
			rf.mu.Unlock()
			return
		}
		// update the peer's information
		rf.matchIndex[id] = args.LastIncludedIndex
		rf.nextIndex[id] = args.LastIncludedIndex + 1
		// rf.debug(fmt.Sprintf("[handleInstallSnapshot] update id %d info: matchIndex: %d, nextIndex: %d", id, rf.matchIndex[id], rf.nextIndex[id]))
		rf.mu.Unlock()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.RLock()
	peer := rf.peers[server]
	rf.mu.RUnlock()
	ok := peer.Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.RLock()
	peer := rf.peers[server]
	rf.mu.RUnlock()
	ok := peer.Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.RLock()
	peer := rf.peers[server]
	rf.mu.RUnlock()
	ok := peer.Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if this server isn't the leader, returns false
	if rf.serverState != Leader {
		return index, term, false
	}

	// start the agreement
	selfLastLogIndex, _ := rf.getLastIndexAndTerm()
	index = selfLastLogIndex + 1
	term = rf.currentTerm

	// append entry to local log
	rf.log = append(rf.log, LogEntry{command, term, index})
	rf.persist()
	// update matchIndex
	rf.matchIndex[rf.me] = index
	// rf.debug(fmt.Sprintf("[Start] add new log entry: %+v", rf.log))
	// issues AppendEntries RPCs in background
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat() {
	// rf.debug("begin to send heartbeat")
	for {
		rf.mu.RLock()
		if rf.serverState != Leader {
			rf.mu.RUnlock()
			break
		}

		if rf.killed() {
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		for id, _ := range rf.peers {
			rf.mu.RLock()
			if id == rf.me {
				rf.mu.RUnlock()
				continue
			}

			rf.mu.RUnlock()
			go rf.handleAppendEntries(id, true)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		timeoutGap := 100 + (rand.Int63() % 400)
		rf.mu.RLock()
		currentGap := time.Now().Sub(rf.electionTimer).Milliseconds()
		// rf.debug(fmt.Sprintf("Check if a leader election should be started, currentGap: %d, timeGap: %d, rf.voteFor: %d", currentGap, timeoutGap, rf.voteFor))
		// follower: If election timeout elapses without receiving AppendEntries RPC from current leader
		// or granting vote to candidate: convert to candidate
		// candidate: If election timeout elapses: start new election
		// if (currentGap > timeoutGap && rf.serverState != Leader) || (rf.serverState == Follower && rf.voteFor == -1) {
		if currentGap > timeoutGap && rf.serverState != Leader {
			rf.mu.RUnlock()
			// rf.debug(fmt.Sprintf("begin election, the currentGap: %d, timeoutGap: %d", currentGap, timeoutGap))
			rf.mu.Lock()
			rf.convertToCandidate()
			rf.mu.Unlock()
			rf.mu.RLock()
			selfLastLogIndex, selfLastLogTerm := rf.getLastIndexAndTerm()
			// prepare the param and reply
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: selfLastLogIndex,
				LastLogTerm:  selfLastLogTerm,
			}
			// rf.debug(fmt.Sprintf("[ticker] begin re election, args: %+v", args))
			rf.mu.RUnlock()
			for id, _ := range rf.peers {
				rf.mu.RLock()
				if id == rf.me {
					// rf.debug(fmt.Sprintf("the same id %d, continue", id))
					rf.mu.RUnlock()
					continue
				}
				rf.mu.RUnlock()
				go func(id int) {
					if rf.killed() {
						return
					}
					reply := RequestVoteReply{}
					// rf.debug(fmt.Sprintf(fmt.Sprintf("send request to %d", id)))
					ret := rf.sendRequestVote(id, &args, &reply)
					if ret {
						// check reply value
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							// rf.debug("convert to follower after send request vote")
							rf.currentTerm = reply.Term
							rf.convertToFollower(true)
							rf.persist()
							rf.mu.Unlock()
						} else if reply.VoteGranted && rf.serverState == Candidate {
							// rf.debug(fmt.Sprintf("get reply.VoteGranted from %d, current vote count: %d", id, rf.votes))
							rf.votes += 1
							// rf.debug(fmt.Sprintf("get reply.VoteGranted from %d, current vote count: %d", id, rf.votes))
							// If votes received from majority of servers: become leader
							if rf.getMajorityVote() && rf.serverState != Leader {
								// rf.debug(fmt.Sprintf("get majority vote, convert to leader"))
								rf.convertToLeader()
								// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
								// rf.debug("begin to call send heartbeat")
								rf.mu.Unlock()
								go rf.sendHeartbeat()
							} else {
								rf.mu.Unlock()
							}
						} else {
							rf.mu.Unlock()
						}
					}
				}(id)
			}
		} else {
			rf.mu.RUnlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// applier apply the committed log to the state machine
func (rf *Raft) applier() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.handleApplyEntries()
	}
}

// replicator replicate the new log entries to the followers
// TODO(zhr): need change the replicated model according to the https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab2.md
func (rf *Raft) replicator() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.RLock()
		if rf.serverState != Leader {
			rf.mu.RUnlock()
			continue
		}
		if rf.killed() {
			rf.mu.RUnlock()
			return
		}
		rf.mu.RUnlock()
		for id, _ := range rf.peers {
			rf.mu.RLock()
			if id == rf.me {
				rf.mu.RUnlock()
				continue
			}
			// if the follower's log is up-to-date, no need to send AppendEntries RPC
			if len(rf.log) > 0 && rf.matchIndex[id] < rf.log[len(rf.log)-1].Index {
				rf.mu.RUnlock()
				// rf.debug(fmt.Sprintf("[replicator] for follower %d, matchIndex: %d, lastIndex: %d", id, rf.matchIndex[id], rf.log[len(rf.log)-1].Index))
				go rf.handleAppendEntries(id, false)
			} else {
				rf.mu.RUnlock()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	indexLength := len(rf.peers)
	rf.nextIndex = make([]int, indexLength)
	rf.matchIndex = make([]int, indexLength)
	rf.serverState = Follower
	rf.electionTimer = time.Now()
	rf.votes = 0
	rf.leaderId = -1
	rf.applyCh = applyCh
	rf.lastIncludedTerm = -1
	rf.lastIncludedIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start replicator goroutine to replicate log entries
	go rf.replicator()
	// start applier goroutine to apply committed logs
	go rf.applier()

	return rf
}
