package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cf *Config) Copy() Config {
	newConfig := Config{
		Num:    cf.Num,
		Shards: cf.Shards,
		Groups: map[int][]string{},
	}
	for key, value := range cf.Groups {
		newConfig.Groups[key] = make([]string, len(value))
		for i, server := range value {
			newConfig.Groups[key][i] = server
		}
	}
	return newConfig
}

const (
	OK               = "OK"
	OutOfDateErr     = "The request is out of date"
	NotLeaderErr     = "Current raft peer is not a leader"
	TimeoutErr   Err = "Operation timeout"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClerkId   int64
	SeqNumber int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClerkId   int64
	SeqNumber int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClerkId   int64
	SeqNumber int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
	ClerkId     int64
	SeqNumber   int
}

type QueryArgs struct {
	Num       int // desired config number
	ClerkId   int64
	SeqNumber int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
