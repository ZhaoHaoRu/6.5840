package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

const RaftTimeOut = 5000 * time.Millisecond

type RaftApplyResult struct {
	Err    Err
	Config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()
	// sessionMap record clerk's corresponding latest seq number
	sessionMap     map[int64]Op
	notifyChannels map[int]chan RaftApplyResult
	configs        []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	ConfigNum int
	OpType    string
	ClerkId   int64
	SeqNumber int
}

func (sc *ShardCtrler) debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] [ServerId %d] %s\n", pid, sc.me, message)
	fmt.Printf(logMessage)
}

func (sc *ShardCtrler) getNotifyChan(index int) chan RaftApplyResult {
	resultCh, ok := sc.notifyChannels[index]
	if !ok {
		sc.notifyChannels[index] = make(chan RaftApplyResult)
	} else {
		// NOTE(zhr): there might be two requests with the same index, let the preceding one fail
		resultCh <- RaftApplyResult{
			Err: OutOfDateErr,
		}
		sc.notifyChannels[index] = make(chan RaftApplyResult)
	}
	resultCh, _ = sc.notifyChannels[index]
	return resultCh
}

// operationHandler handle Join/Leave/Move/Query logic
func (sc *ShardCtrler) operationHandler(op *Op) RaftApplyResult {
	result := RaftApplyResult{Err: OK}

	// process the operation with raft
	index, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		result.Err = NotLeaderErr
		return result
	}

	sc.mu.Lock()
	notifyCh := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result = <-notifyCh:
	case <-time.After(RaftTimeOut):
		result.Err = TimeoutErr
	}
	sc.mu.Lock()
	delete(sc.notifyChannels, index)
	sc.mu.Unlock()
	return result
}

func (sc *ShardCtrler) createConfig() Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Groups: make(map[int][]string),
	}
	for key, value := range oldConfig.Groups {
		newConfig.Groups[key] = make([]string, len(value))
		for i, server := range oldConfig.Groups[key] {
			newConfig.Groups[key][i] = server
		}
	}
	for i, gid := range oldConfig.Shards {
		newConfig.Shards[i] = gid
	}
	return newConfig
}

// reBalance shards between groups
// NOTE(zhr): the shard re balancing needs to be deterministic
func (cf *Config) reBalance() {
	// shardMap only records valid GIDs
	shardMap := make(map[int][]int)
	unAssignedShards := make([]int, 0)
	for shardId, gid := range cf.Shards {
		if gid == 0 {
			unAssignedShards = append(unAssignedShards, shardId)
		} else {
			shardMap[gid] = append(shardMap[gid], shardId)
		}
	}
	groupList := make([]int, 0)
	for gid, _ := range cf.Groups {
		groupList = append(groupList, gid)
		if _, ok := shardMap[gid]; !ok {
			shardMap[gid] = make([]int, 0)
		}
	}
	groupSize := len(groupList)
	for {
		sort.Slice(groupList, func(i, j int) bool {
			return len(shardMap[groupList[i]]) < len(shardMap[groupList[j]]) || len(shardMap[groupList[i]]) == len(shardMap[groupList[j]]) && groupList[i] < groupList[j]
		})

		if len(groupList) == 0 {
			break
		}

		// if some un assigned remains, assign it first
		if len(unAssignedShards) > 0 {
			candidate := unAssignedShards[0]
			unAssignedShards = unAssignedShards[1:]
			cf.Shards[candidate] = groupList[0]
			shardMap[groupList[0]] = append(shardMap[groupList[0]], candidate)
			continue
		}

		if groupSize <= 1 || len(shardMap[groupList[groupSize-1]])-len(shardMap[groupList[0]]) <= 1 {
			break
		}

		candidate := shardMap[groupList[groupSize-1]][0]
		cf.Shards[candidate] = groupList[0]
		shardMap[groupList[groupSize-1]] = shardMap[groupList[groupSize-1]][1:]
		shardMap[groupList[0]] = append(shardMap[groupList[0]], candidate)
	}
}

func (sc *ShardCtrler) handleJoin(op *Op) {
	newConfig := sc.createConfig()
	// add new group
	for gid, newServers := range op.Servers {
		servers, ok := newConfig.Groups[gid]
		if ok {
			servers = append(servers, newServers...)
		} else {
			newConfig.Groups[gid] = newServers
		}
	}
	// re balance
	newConfig.reBalance()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleLeave(op *Op) {
	newConfig := sc.createConfig()
	for _, GID := range op.GIDs {
		delete(newConfig.Groups, GID)
	}
	for i, gid := range newConfig.Shards {
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Shards[i] = 0 // Invalid gid
		}
	}

	newConfig.reBalance()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleMove(op *Op) {
	newConfig := sc.createConfig()
	// create a new configuration in which the
	// shard is assigned to the group
	newConfig.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleQuery(op *Op) Config {
	// If the number is -1 or bigger than the biggest known
	// configuration number, the shardctrler should reply with the
	// latest configuration
	result := Config{}
	if op.ConfigNum == -1 || op.ConfigNum >= len(sc.configs) {
		result = sc.configs[len(sc.configs)-1]
	} else {
		result = sc.configs[op.ConfigNum]
	}
	return result
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// sc.debug(fmt.Sprintf("[Join] args: %+v", args))
	reply.WrongLeader = false
	reply.Err = OK

	sc.mu.Lock()
	// check duplicated request
	if lastOp, ok := sc.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= lastOp.SeqNumber {
			reply.Err = OutOfDateErr
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	// generate Op for raft
	op := Op{
		Servers:   args.Servers,
		OpType:    JoinOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	processResult := sc.operationHandler(&op)
	if processResult.Err == NotLeaderErr {
		reply.WrongLeader = true
	}
	reply.Err = processResult.Err
	//if !reply.WrongLeader {
	//	sc.debug(fmt.Sprintf("[Join] reply: %+v, config: %+v", reply, sc.configs))
	//}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// sc.debug(fmt.Sprintf("[Leave] args: %+v", args))
	reply.WrongLeader = false
	reply.Err = OK

	sc.mu.Lock()
	// check duplicated request
	if lastOp, ok := sc.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= lastOp.SeqNumber {
			reply.Err = OutOfDateErr
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	// generate Op for raft
	op := Op{
		GIDs:      args.GIDs,
		OpType:    LeaveOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	processResult := sc.operationHandler(&op)
	if processResult.Err == NotLeaderErr {
		reply.WrongLeader = true
	}
	reply.Err = processResult.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// sc.debug(fmt.Sprintf("[Move] args: %+v", args))
	reply.WrongLeader = false
	reply.Err = OK

	sc.mu.Lock()
	// check duplicated request
	if lastOp, ok := sc.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= lastOp.SeqNumber {
			reply.Err = OutOfDateErr
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	// generate Op for raft
	op := Op{
		Shard:     args.Shard,
		GID:       args.GID,
		OpType:    MoveOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	processResult := sc.operationHandler(&op)
	if processResult.Err == NotLeaderErr {
		reply.WrongLeader = true
	}
	reply.Err = processResult.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// sc.debug(fmt.Sprintf("[Query] args: %+v", args))
	reply.WrongLeader = false
	reply.Err = OK

	sc.mu.Lock()
	// check duplicated request
	if lastOp, ok := sc.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= lastOp.SeqNumber {
			reply.Err = OutOfDateErr
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	// generate Op for raft
	op := Op{
		ConfigNum: args.Num,
		OpType:    QueryOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	processResult := sc.operationHandler(&op)
	if processResult.Err == NotLeaderErr {
		reply.WrongLeader = true
	}
	reply.Err = processResult.Err
	reply.Config = processResult.Config
	//if !reply.WrongLeader {
	//	sc.debug(fmt.Sprintf("[Query] reply: %+v", reply))
	//}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if sc.killed() {
				break
			}
			applyResult := RaftApplyResult{
				Err: OK,
			}
			if msg.CommandValid {
				if cmd, ok := msg.Command.(Op); ok {
					sc.mu.Lock()
					// check whether the command already been applied
					isValid := true
					isLast := false
					if lastOp, ok := sc.sessionMap[cmd.ClerkId]; ok {
						if cmd.SeqNumber <= lastOp.SeqNumber {
							isValid = false
						}
						if cmd.SeqNumber == lastOp.SeqNumber && cmd.OpType == lastOp.OpType {
							isLast = true
						}
					}
					// sc.debug(fmt.Sprintf("[applier] begin to apply raft result: %+v", cmd))
					// apply to the state machine
					switch cmd.OpType {
					case JoinOp:
						if isValid {
							sc.handleJoin(&cmd)
						}
					case LeaveOp:
						if isValid {
							sc.handleLeave(&cmd)
						}
					case MoveOp:
						if isValid {
							sc.handleMove(&cmd)
						}
					case QueryOp:
						if isValid || isLast {
							applyResult.Config = sc.handleQuery(&cmd)
						}
					}

					// update the session map
					if isValid {
						sc.sessionMap[cmd.ClerkId] = cmd
					}

					// notify the operation handler
					term, isLeader := sc.rf.GetState()
					if term == msg.CommandTerm && isLeader {
						notifyCh := sc.notifyChannels[msg.CommandIndex]
						sc.mu.Unlock()
						notifyCh <- applyResult
					} else {
						sc.mu.Unlock()
					}
				}
			}
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.sessionMap = make(map[int64]Op)
	sc.notifyChannels = make(map[int]chan RaftApplyResult)
	go sc.applier()
	return sc
}
