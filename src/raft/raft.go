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
	"fmt"
	//	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry is a struct for raft log
type LogEntry struct {
	Data  int
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	// XXX: the guideline says `Don't use Go's time.Timer`
	electionTimer time.Time
	// the votes get in elections
	votes    int
	leaderId int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.serverState == Leader
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
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// the Term of the conﬂicting entry and the ﬁrst Index it stores for that Term
	// to reduce the number of rejected AppendEntries RPCs
	ConflictTerm  int
	ConflictIndex int
}

// some helper function

func (rf *Raft) convertToFollower() {
	rf.serverState = Follower
	rf.votes = 0
	rf.voteFor = -1
}

func (rf *Raft) convertToCandidate() {
	rf.serverState = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.electionTimer = time.Now()
	rf.votes = 1
}

func (rf *Raft) convertToLeader() {
	rf.serverState = Leader

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
}

func (rf *Raft) getLastIndexAndTerm() (int, int) {
	selfLastLogIndex := -1
	selfLastLogTerm := -1
	logLength := len(rf.log)
	if logLength > 0 {
		selfLastLogTerm = rf.log[logLength-1].Term
		selfLastLogIndex = rf.log[logLength-1].Index
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
// TODO: need to change after applying snapshot, current Index equal to position in log
func (rf *Raft) checkLogMatch(args *AppendEntriesArgs) (bool, int, int) {
	if args.PrevLogIndex < 0 {
		return false, 0, 0
	}

	conflictTerm := 0
	conflictIndex := 0
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(rf.log) <= args.PrevLogIndex {
			conflictTerm, conflictIndex = rf.getLastIndexAndTerm()
		} else {
			conflictTerm, conflictIndex = args.PrevLogTerm, args.PrevLogIndex
		}
		for i := conflictIndex; i >= 0; i-- {
			if rf.log[rf.getIndexPos(i)].Term != conflictTerm {
				break
			} else {
				conflictIndex = rf.log[rf.getIndexPos(i)].Index
			}
		}
		return false, conflictIndex, conflictTerm
	}

	return true, 0, 0
}

// getIndexPos convert Index to the entry position in log, prepare for snapshot
func (rf *Raft) getIndexPos(index int) int {
	if len(rf.log) < index {
		return -1
	}
	return index
}

// end helper function

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.debug(fmt.Sprintf("get request vote, data: %+v", args))
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.debug("get the lock")
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.debug(fmt.Sprintf("get request from %d, but Term too small", args.CandidateId))
		return
	}

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.debug(fmt.Sprintf("get request from %d, current Term too small, convert to follower", args.CandidateId))
		rf.convertToFollower()
	}

	// If votedFor is null or candidateId
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		// if candidate's log is at least as up-to-update
		selfLastLogIndex, selfLastLogTerm := rf.getLastIndexAndTerm()
		rf.debug(fmt.Sprintf("selfLastLogIndex: %d, selfLastLogTerm: %d, args.LastLogIndex: %d, args.LastLogTerm: %d", selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm))
		if args.LastLogTerm > selfLastLogTerm || (args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex) {
			rf.debug(fmt.Sprintf("vote for %d", args.CandidateId))
			rf.voteFor = args.CandidateId
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
	if args.Term < rf.currentTerm {
		return
	}

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	// update election timeout
	rf.electionTimer = time.Now()
	// rf.debug("update election timer")
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
		rf.convertToFollower()
	}

	// if it is not a heartbeat
	if len(args.Entries) != 0 {
		// Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm
		isMatch, conflictIndex, conflictTerm := rf.checkLogMatch(args)
		if !isMatch {
			reply.Success = false
			reply.ConflictIndex = conflictIndex
			reply.ConflictTerm = conflictTerm
			return
		}

		// delete all unmatched entries in receiver and existed entries in args
		newEntryPos := 0
		for _, entry := range args.Entries {
			if entry.Term == rf.log[rf.getIndexPos(entry.Index)].Term {
				newEntryPos += 1
			} else {
				rf.log = rf.log[:rf.getIndexPos(entry.Index)]
				break
			}
		}
		// Append any new entries not already in the log
		entries := args.Entries[newEntryPos:]
		rf.log = append(rf.log, entries...)
	}

	reply.Success = true
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, Index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		selfLastLogIndex, _ := rf.getLastIndexAndTerm()
		if args.LeaderCommit < selfLastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = selfLastLogIndex
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.debug("begin to send heartbeat")
	for {
		if rf.serverState != Leader {
			break
		}

		if rf.killed() {
			break
		}

		for id, _ := range rf.peers {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[id],
				Entries:      make([]LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			// in case log is empty
			pos := rf.getIndexPos(rf.nextIndex[id])
			if pos > 0 {
				args.PrevLogTerm = rf.log[pos].Term
			} else {
				args.PrevLogTerm = -1
			}

			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(id, &args, &reply)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// rf.debug("Check if a leader election should be started")
		timeoutGap := 100 + (rand.Int63() % 400)
		// If election timeout elapses without receiving AppendEntries RPC
		// from current leader or granting vote to candidate: convert to candidate
		if time.Now().Sub(rf.electionTimer).Milliseconds() > timeoutGap && ((rf.serverState == Follower && rf.voteFor == -1) || rf.serverState == Candidate) {
			rf.debug(fmt.Sprintf("begin election, the timegap: %d", time.Now().Sub(rf.electionTimer).Milliseconds()))
			rf.convertToCandidate()

			selfLastLogIndex, selfLastLogTerm := rf.getLastIndexAndTerm()
			// prepare the param and reply
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: selfLastLogIndex,
				LastLogTerm:  selfLastLogTerm,
			}
			reply := RequestVoteReply{}
			for id, _ := range rf.peers {
				if id == rf.me {
					rf.debug(fmt.Sprintf("the same id %d, continue", id))
					continue
				}
				go func(id int) {
					if rf.killed() {
						return
					}
					rf.debug(fmt.Sprintf(fmt.Sprintf("send request to %d", id)))
					ret := rf.sendRequestVote(id, &args, &reply)
					if ret {
						// check reply value
						if reply.Term > rf.currentTerm {
							rf.debug("convert to follower after send request vote")
							rf.convertToFollower()
						} else if reply.VoteGranted {
							rf.debug(fmt.Sprintf("get reply.VoteGranted from %d", id))
							rf.mu.Lock()
							rf.votes += 1
							rf.debug(fmt.Sprintf("current vote count: %d", rf.votes))
							// If votes received from majority of servers: become leader
							if rf.getMajorityVote() && rf.serverState != Leader {
								rf.debug(fmt.Sprintf("get majority vote, convert to leader"))
								rf.convertToLeader()
								// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
								rf.debug("begin to call send heartbeat")
								rf.mu.Unlock()
								go rf.sendHeartbeat()
							} else {
								rf.mu.Unlock()
							}
						}
					}
				}(id)
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10 + (rand.Int63() % 100)
		// ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
