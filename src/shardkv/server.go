package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
	Config   = "Config"
	Migrate  = "Migrate"
	Reclaim  = "Reclaim"
	UnKnown  = "UnKnown"
)

const RaftTimeOut = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClerkId   int64
	SeqNumber int
	ShardId   int
}

type ConfigOp struct {
	Config shardctrler.Config
	Op     string
}

type ShardOp struct {
	Num    int
	Shards map[int]*Shard
	Op     string
}

type ReclaimOp struct {
	Num      int
	ShardIds []int
	Op       string
}

type RaftApplyResult struct {
	Err   Err
	Value string
}

// NOTE(zhr): ShardStatus refer https://zhuanlan.zhihu.com/p/464097239
type ShardStatus int

const (
	// The group serves and owns the shard.
	Serving ShardStatus = iota
	// The group serves the shard, but does not own the shard yet.
	Pulling
	// The group does not serve and own the partition.
	Invalid
	// The group owns but does not serve the shard.
	Erasing
	// The group own the shard and serve it, but it's waiting for ex-owner to delete it
	Waiting
)

type Shard struct {
	ShardStatus ShardStatus
	Storage     map[string]string
	SessionMap  map[int64]Op
}

func (sd *Shard) deepCopy() *Shard {
	copySd := &Shard{
		ShardStatus: sd.ShardStatus,
	}

	if sd.Storage != nil {
		copySd.Storage = make(map[string]string)
		for k, v := range sd.Storage {
			copySd.Storage[k] = v
		}
	}

	if sd.SessionMap != nil {
		copySd.SessionMap = make(map[int64]Op)
		for k, v := range sd.SessionMap {
			copySd.SessionMap[k] = Op{
				Key:       v.Key,
				Value:     v.Value,
				Op:        v.Op,
				ClerkId:   v.ClerkId,
				SeqNumber: v.SeqNumber,
				ShardId:   v.ShardId,
			}
		}
	}
	return copySd
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()
	// stateMachine is the backend for Shard
	stateMachine   map[int]*Shard
	notifyChannels map[int]chan RaftApplyResult
	// lastApplied indicating the last included index for snapshot
	lastApplied int
	// lastConfig is the previous Config
	lastConfig shardctrler.Config
	// curConfig is the latest Config
	curConfig       shardctrler.Config
	persister       *raft.Persister
	shardController *shardctrler.Clerk
}

func (kv *ShardKV) debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] [ServerId %d] [GID %d] %s\n", pid, kv.me, kv.gid, message)
	fmt.Printf(logMessage)
}

func (kv *ShardKV) getNotifyChan(index int) chan RaftApplyResult {
	resultCh, ok := kv.notifyChannels[index]
	if !ok {
		kv.notifyChannels[index] = make(chan RaftApplyResult)
	} else {
		// NOTE(zhr): there might be two requests with the same index, let the preceding one fail
		resultCh <- RaftApplyResult{
			Err: ErrOutOfDate,
		}
		kv.notifyChannels[index] = make(chan RaftApplyResult)
	}
	resultCh, _ = kv.notifyChannels[index]
	return resultCh
}

func (kv *ShardKV) operationHandler(op interface{}) RaftApplyResult {
	result := RaftApplyResult{Err: OK}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		result.Err = ErrWrongLeader
		return result
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result = <-notifyCh:
	case <-time.After(RaftTimeOut):
		result.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.notifyChannels, index)
	kv.mu.Unlock()
	return result
}

func (kv *ShardKV) checkShardMatch(shardId int) bool {
	shard, ok := kv.stateMachine[shardId]
	if !ok {
		return false
	}
	isMatch := kv.curConfig.Shards[shardId] == kv.gid && (shard.ShardStatus == Serving || shard.ShardStatus == Waiting)
	kv.debug(fmt.Sprintf("[checkShardMatch] shardId: %+v, target gid: %d, kv.gid: %+v, shard: %+v, result: %+v, curConfig: %+v", shardId, kv.curConfig.Shards[shardId], kv.gid, shard, isMatch, kv.curConfig))
	return isMatch
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK

	// kv.debug(fmt.Sprintf("[ShardKV.Get] args: %+v", args))
	kv.mu.Lock()
	// check shard info
	if !kv.checkShardMatch(args.ShardId) {
		reply.Err = ErrWrongGroup
		// kv.debug(fmt.Sprintf("[ShardKV.Get] wrong group, args: %+v, reply: %+v", args, reply))
		kv.mu.Unlock()
		return
	}

	// check duplicated request
	shard := kv.stateMachine[args.ShardId]
	if lastOp, ok := shard.SessionMap[args.ClerkId]; ok {
		if args.SeqNumber < lastOp.SeqNumber {
			reply.Err = ErrOutOfDate
			// kv.debug(fmt.Sprintf("[ShardKV.Get] request out of date, args: %+v, lastOp: %+v", args, lastOp))
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	// generate Op
	op := Op{
		Key:       args.Key,
		Op:        GetOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
		ShardId:   args.ShardId,
	}
	result := kv.operationHandler(op)
	reply.Err = result.Err
	reply.Value = result.Value
	if reply.Err == OK || reply.Err == ErrOutOfDate {
		kv.debug(fmt.Sprintf("[Get] reply: %+v, args: %+v", reply, args))
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK

	// check shard info
	kv.mu.Lock()
	if !kv.checkShardMatch(args.ShardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// check duplicated request
	shard := kv.stateMachine[args.ShardId]
	if lastOp, ok := shard.SessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= lastOp.SeqNumber {
			reply.Err = ErrOutOfDate
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	// generate Op
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
		ShardId:   args.ShardId,
	}

	result := kv.operationHandler(op)
	reply.Err = result.Err
	if reply.Err == OK {
		kv.debug(fmt.Sprintf("[PutAppend] reply: %+v, args: %+v", reply, args))
	}
}

func (kv *ShardKV) MigrateShard(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	reply.Err = OK

	// kv.debug(fmt.Sprintf("[MigrateShard] begin, args: %+v", args))
	// only the leader can respond to shard migration
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// only process the corresponding configNum request
	if args.Num > kv.curConfig.Num {
		reply.Err = ErrNotReady
		kv.debug(fmt.Sprintf("[MigrateShard] arg too new, configNum: %d, kv.curConfig: %+v", args.Num, kv.curConfig))
		return
	}
	if args.Num < kv.curConfig.Num {
		reply.Err = ErrOutOfDate
		return
	}

	// NOTE(zhr): the data should be deep copy
	reply.Shards = make(map[int]*Shard)
	reply.Num = kv.curConfig.Num
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = kv.stateMachine[shardId].deepCopy()
	}
	// kv.debug(fmt.Sprintf("[MigrateShard] get reply, reply: %+v, args: %+v", reply, args))
}

func (kv *ShardKV) ReclaimShard(args *ShardReclaimArgs, reply *ShardReclaimReply) {
	reply.Err = OK

	// only the leader can respond to shard deletion
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// only process the corresponding configNum request
	if args.Num > kv.curConfig.Num {
		reply.Err = OK
		kv.debug(fmt.Sprintf("[ReclaimShard] the arg's config number is too new %d, cur config num: %d", args.Num, kv.curConfig.Num))
		kv.mu.Unlock()
		return
	}
	if args.Num < kv.curConfig.Num {
		reply.Err = ErrOutOfDate
		kv.debug(fmt.Sprintf("[ReclaimShard] the arg's config number is out of date %d, cur config num: %d", args.Num, kv.curConfig.Num))
		kv.mu.Unlock()
		return
	}

	// generate Op
	op := ReclaimOp{
		Num:      args.Num,
		ShardIds: args.ShardIds,
		Op:       Reclaim,
	}
	kv.mu.Unlock()
	result := kv.operationHandler(op)
	reply.Err = result.Err
}

func (kv *ShardKV) applyOperation(cmd *Op, result *RaftApplyResult) {
	isValid := true
	isLast := false
	var shard *Shard
	if !kv.checkShardMatch(cmd.ShardId) {
		result.Err = ErrWrongGroup
		return
	} else {
		shard = kv.stateMachine[cmd.ShardId]
		if lastOp, ok := shard.SessionMap[cmd.ClerkId]; ok {
			if cmd.SeqNumber <= lastOp.SeqNumber {
				isValid = false
			}
			if cmd.SeqNumber == lastOp.SeqNumber && cmd.Op == lastOp.Op && cmd.Key == lastOp.Key {
				isLast = true
			}
		}
	}

	switch cmd.Op {
	case GetOp:
		if isValid || isLast {
			var ok bool
			result.Value, ok = shard.Storage[cmd.Key]
			if !ok {
				result.Value = ""
				result.Err = ErrNoKey
			}
		} else {
			result.Err = ErrOutOfDate
		}
	case PutOp:
		if isValid {
			shard.Storage[cmd.Key] = cmd.Value
		} else {
			result.Err = ErrOutOfDate
		}
	case AppendOp:
		if isValid {
			if original, ok := shard.Storage[cmd.Key]; ok {
				shard.Storage[cmd.Key] = original + cmd.Value
			} else {
				shard.Storage[cmd.Key] = cmd.Value
			}
		} else {
			result.Err = ErrOutOfDate
		}
	}

	if isValid {
		// update session map
		shard.SessionMap[cmd.ClerkId] = *cmd
	}
}

func (kv *ShardKV) applyConfiguration(newConfig *shardctrler.Config, result *RaftApplyResult) {
	if newConfig.Num == kv.curConfig.Num+1 {
		// for initialization, change status to serving
		if newConfig.Num == 1 {
			for _, shard := range kv.stateMachine {
				shard.ShardStatus = Serving
			}
			result.Err = OK
			kv.lastConfig = kv.curConfig
			kv.curConfig = *newConfig
			statusMap := make(map[int]ShardStatus)
			for i, shard := range kv.stateMachine {
				statusMap[i] = shard.ShardStatus
			}
			kv.debug(fmt.Sprintf("[applyConfiguration] kv.curConfig: %+v, status: %+v", kv.curConfig, statusMap))
			return
		}

		inputShards := make([]int, 0, shardctrler.NShards)
		outputShards := make([]int, 0, shardctrler.NShards)

		oldConfig := kv.curConfig.Copy()

		for i := 0; i < shardctrler.NShards; i++ {
			if newConfig.Shards[i] == kv.gid {
				if oldConfig.Shards[i] != kv.gid {
					inputShards = append(inputShards, i)
				}
			} else {
				if oldConfig.Shards[i] == kv.gid {
					outputShards = append(outputShards, i)
				}
			}
		}

		// for input shard, change status to pulling
		for _, shardId := range inputShards {
			kv.stateMachine[shardId].ShardStatus = Pulling
		}

		// for output shard, change status to erasing
		for _, shardId := range outputShards {
			kv.stateMachine[shardId].ShardStatus = Erasing
		}

		// update last config and current config
		kv.lastConfig = oldConfig
		kv.curConfig = *newConfig

		statusMap := make(map[int]ShardStatus)
		for i, shard := range kv.stateMachine {
			statusMap[i] = shard.ShardStatus
		}
		kv.debug(fmt.Sprintf("[applyConfiguration] kv.curConfig: %+v, status: %+v", kv.curConfig, statusMap))

		result.Err = OK
	} else {
		result.Err = ErrOutOfDate
	}
}

func (kv *ShardKV) applyShardMigration(shardOp *ShardOp, result *RaftApplyResult) {
	if shardOp.Num != kv.curConfig.Num {
		kv.debug(fmt.Sprintf("[applyShardMigration] the reclaim config num is incorrect, args: %+v, curConfig: %+v", shardOp, kv.curConfig))
		result.Err = ErrOutOfDate
		return
	}

	kv.debug(fmt.Sprintf("[applyShardMigration] kv.curConfig: %+v", kv.curConfig))
	for shardId, shard := range shardOp.Shards {
		if kv.stateMachine[shardId].ShardStatus != Pulling {
			kv.debug(fmt.Sprintf("[applyShardMigration] the shard %d status %d is unexpected", shardId, kv.stateMachine[shardId].ShardStatus))
			break
		}

		// update the status
		for k, v := range shard.Storage {
			kv.stateMachine[shardId].Storage[k] = v
		}
		kv.stateMachine[shardId].ShardStatus = Waiting
		for clerkId, op := range shard.SessionMap {
			if prevOp, ok := kv.stateMachine[shardId].SessionMap[clerkId]; ok {
				if prevOp.SeqNumber < op.SeqNumber {
					kv.stateMachine[shardId].SessionMap[clerkId] = op
				}
			} else {
				kv.stateMachine[shardId].SessionMap[clerkId] = op
			}
		}
		kv.debug(fmt.Sprintf("[applyShardMigration] new shard %d state: %+v", shardId, shard))
	}

	result.Err = OK
}

func (kv *ShardKV) applyShardReclaim(reclaimOp *ReclaimOp, result *RaftApplyResult) {
	if reclaimOp.Num != kv.curConfig.Num {
		kv.debug(fmt.Sprintf("[applyShardReclaim] the reclaim config num is incorrect: %+v, curConfig: %+v", reclaimOp, kv.curConfig))
		result.Err = ErrOutOfDate
		return
	}

	kv.debug(fmt.Sprintf("[applyShardReclaim] kv.curConfig: %+v, args: %+v", kv.curConfig, reclaimOp))
	// update the status
	for _, shardId := range reclaimOp.ShardIds {
		if kv.stateMachine[shardId].ShardStatus == Waiting {
			kv.stateMachine[shardId].ShardStatus = Serving
		} else if kv.stateMachine[shardId].ShardStatus == Erasing {
			kv.stateMachine[shardId].ShardStatus = Invalid
		} else {
			kv.debug(fmt.Sprintf("[applyShardReclaim] the shard status is in incorrect: %+v", kv.stateMachine[shardId]))
		}
		kv.debug(fmt.Sprintf("[applyShardReclaim] shard %d: %+v", shardId, kv.stateMachine[shardId]))
	}

	result.Err = OK
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	curSize := kv.rf.GetStateSize()
	upperBound := 0.8 * float64(kv.maxraftstate)
	return curSize >= int(upperBound)
}

// NOTE(zhr): also need to persist config
func (kv *ShardKV) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.stateMachine)
	_ = e.Encode(kv.lastConfig)
	_ = e.Encode(kv.curConfig)
	return w.Bytes()
}

func (kv *ShardKV) applySnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newStateMachine map[int]*Shard
	var lastConfig shardctrler.Config
	var curConfig shardctrler.Config
	if d.Decode(&newStateMachine) != nil || d.Decode(&lastConfig) != nil || d.Decode(&curConfig) != nil {
		return
	}
	kv.stateMachine = newStateMachine
	kv.lastConfig = lastConfig
	kv.curConfig = curConfig
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			applyResult := RaftApplyResult{
				Err: OK,
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				if msg.SnapshotIndex > kv.lastApplied {
					kv.applySnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {
				kv.mu.Lock()
				if cmd, ok := msg.Command.(Op); ok {
					kv.applyOperation(&cmd, &applyResult)
				} else if cmd, ok := msg.Command.(ConfigOp); ok {
					kv.applyConfiguration(&cmd.Config, &applyResult)
				} else if cmd, ok := msg.Command.(ShardOp); ok {
					kv.applyShardMigration(&cmd, &applyResult)
				} else if cmd, ok := msg.Command.(ReclaimOp); ok {
					kv.applyShardReclaim(&cmd, &applyResult)
				}

				// update last applied
				// XXX(zhr): whether need to check cmd validation here?
				if msg.CommandIndex > kv.lastApplied {
					kv.lastApplied = msg.CommandIndex
				}

				// notify the operation handler
				term, isLeader := kv.rf.GetState()
				if term == msg.CommandTerm && isLeader {
					notifyCh := kv.notifyChannels[msg.CommandIndex]
					kv.mu.Unlock()
					notifyCh <- applyResult
					kv.mu.Lock()
				}

				if kv.needSnapshot() {
					snapshot := kv.generateSnapshot()
					kv.rf.Snapshot(kv.lastApplied, snapshot)
				}
				kv.mu.Unlock()
			}
		}
	}
}

// configFetcher periodically fetch the latest configuration from the shardctrler
func (kv *ShardKV) configFetcher() {
	for !kv.killed() {
		needFetch := true

		kv.mu.Lock()
		// void unstable state being overwritten
		for shardId, shard := range kv.stateMachine {
			if shard.ShardStatus != Invalid && shard.ShardStatus != Serving {
				needFetch = false
				kv.debug(fmt.Sprintf("[configFetcher] not need fetch, because shardId: %d shard: %+v, curConfig: %+v", shardId, shard, kv.curConfig))
				break
			}
		}

		if needFetch {
			newConfigNum := kv.curConfig.Num + 1
			kv.mu.Unlock()
			newConfig := kv.shardController.Query(newConfigNum)
			if newConfig.Num == newConfigNum {
				// applyOperation to current group
				op := ConfigOp{
					Config: newConfig,
					Op:     Config,
				}
				//op := Op{
				//	Config: newConfig,
				//	Op:     ConfigOp,
				//}
				_ = kv.operationHandler(op)
			}
		} else {
			kv.mu.Unlock()
		}

		// kv.debug(fmt.Sprintf("[configFetcher] end fetch config"))
		// polls roughly every 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}
}

// get the group ids and according shards for pulling data
func (kv *ShardKV) getTargetGroups(status ShardStatus) map[int][]int {
	result := make(map[int][]int)
	for shardId, shard := range kv.stateMachine {
		if shard.ShardStatus != status {
			continue
		}
		if list, ok := result[kv.lastConfig.Shards[shardId]]; ok {
			result[kv.lastConfig.Shards[shardId]] = append(list, shardId)
		} else {
			result[kv.lastConfig.Shards[shardId]] = []int{shardId}
		}
	}
	return result
}

// dataMigrator periodically migrate data from other group for new configuration
func (kv *ShardKV) dataMigrator() {
	for !kv.killed() {
		kv.mu.Lock()
		gidToShards := kv.getTargetGroups(Pulling)
		// next round should wait for this round ended
		if len(gidToShards) != 0 {
			kv.debug(fmt.Sprintf("[dataMigrator] begin migration, gidToshard: %+v, curConfig: %+v", gidToShards, kv.curConfig))
		}
		var wg sync.WaitGroup
		for gid, shardIds := range gidToShards {
			wg.Add(1)
			go func(servers []string, shardIds []int, configNum int) {
				defer wg.Done()
				shardMigrationArgs := ShardMigrationArgs{
					Num:      configNum,
					ShardIds: shardIds,
				}
				for _, server := range servers {
					srv := kv.make_end(server)
					var reply ShardMigrationReply
					ok := srv.Call("ShardKV.MigrateShard", &shardMigrationArgs, &reply)
					// kv.debug(fmt.Sprintf("[dataMigrator] get reply, reply: %+v, args: %+v", reply, shardMigrationArgs))
					if ok && reply.Err == OK {
						// apply through raft group
						shardOp := ShardOp{
							Num:    reply.Num,
							Shards: reply.Shards,
							Op:     Migrate,
						}
						_ = kv.operationHandler(shardOp)
					}
				}
			}(kv.lastConfig.Groups[gid], shardIds, kv.curConfig.Num)
		}
		kv.mu.Unlock()
		wg.Wait()
		// kv.debug(fmt.Sprintf("[configFetcher] end dataMigrator"))
		// polls roughly every 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) dataReclaimer() {
	for !kv.killed() {
		kv.mu.Lock()
		gidToShards := kv.getTargetGroups(Waiting)
		// next round should wait for this round ended
		if len(gidToShards) != 0 {
			kv.debug(fmt.Sprintf("[dataReclaimer] begin reclaim, gidToshard: %+v, curConfig: %+v", gidToShards, kv.curConfig))
		}
		var wg sync.WaitGroup
		for gid, shardIds := range gidToShards {
			wg.Add(1)
			go func(servers []string, shardIds []int, configNum int) {
				defer wg.Done()
				shardReclaimArgs := ShardReclaimArgs{
					Num:      configNum,
					ShardIds: shardIds,
				}
				for _, server := range servers {
					srv := kv.make_end(server)
					var reply ShardReclaimReply
					ok := srv.Call("ShardKV.ReclaimShard", &shardReclaimArgs, &reply)
					// kv.debug(fmt.Sprintf("[dataMigrator] get reply, server: %s, reply: %+v, args: %+v", server, reply, shardReclaimArgs))
					if ok && (reply.Err == OK || reply.Err == ErrOutOfDate) {
						// apply through raft group
						reclaimOp := ReclaimOp{
							Num:      configNum,
							ShardIds: shardIds,
							Op:       Reclaim,
						}
						_ = kv.operationHandler(reclaimOp)
					}
				}
			}(kv.lastConfig.Groups[gid], shardIds, kv.curConfig.Num)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ConfigOp{})
	labgob.Register(ShardOp{})
	labgob.Register(ReclaimOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.dead = 0
	kv.stateMachine = make(map[int]*Shard)
	kv.notifyChannels = make(map[int]chan RaftApplyResult)
	kv.lastApplied = 0
	// Use something like this to talk to the shardctrler:
	kv.shardController = shardctrler.MakeClerk(kv.ctrlers)
	// NOTE(zhr): must use the default initialization
	kv.curConfig = shardctrler.Config{}
	kv.lastConfig = shardctrler.Config{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.stateMachine[i] = &Shard{
			ShardStatus: Invalid,
			Storage:     make(map[string]string),
			SessionMap:  make(map[int64]Op),
		}
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// When the server restarts, it should read the snapshot from persister and restore its state from the snapshot.
	if kv.maxraftstate != -1 {
		kv.applySnapshot(kv.rf.GetLastSnapshot())
		kv.lastApplied = kv.rf.GetLastIndex()
	}

	go kv.applier()
	go kv.configFetcher()
	go kv.dataMigrator()
	go kv.dataReclaimer()
	return kv
}
