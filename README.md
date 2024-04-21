# Write a Fault-tolerant Sharded Key/Value Service

## Process

- [x] Project1
- [x] Project2
- [x] Project3
- [x] Project4
  - [ ] Challenge1
  - [x] Challenge2

## Impl & Debug Hints

### Project1

- coordinator里面的共享数据需要加锁
- worker有时候需要等待,比如当map任务都分发出去了,有的worker完成后又来申请任务，此时还有map未完成,reduce不能开始，这个worker需要等待下
- map reduce函数都是通过go插件装载 (.so文件)
- 中间文件命名 mr-X-Y X是map任务号，y是reduce任务号  
- worker的map方法用json存储中间kv对，reduce再读回来，因为真正分布式worker都不在一个机器上，涉及网络传输，所以用json编码解码走个过场。
- 确保没有人在出现崩溃时观察部分写入的文件，用ioutil.TempFile创建临时文件，用os.Rename重命名

### Project2

- 维护了如下几个后台协程：
	- ticker：共 1 个，用来触发 heartbeat timeout 和 election timeout
	- replicator: 向follower复制log
	- applier：共 1 个，用来往 applyCh 中 push 提交的日志并保证 exactly once
- 复制模型：包装一个函数，其负责向所有 follower 发送一轮同步。不论是心跳超时还是上层服务传进来一个新 command，都去调一次这个函数来发起一轮同步。
- 锁的使用：发送 rpc，接收 rpc，push channel，receive channel 的时候一定不要持锁，否则很可能产生死锁
- commit 日志：raft leader 只能提交当前 term 的日志，不能提交旧 term 的日志。因此 leader 根据 matchIndex[] 来 commit 日志时需要判断该日志的 term 是否等于 leader 当前的 term，即是否为当前 leader 任期新产生的日志，若是才可以提交
- `voteFor`指的是`candidateId that received vote in current term`，因此在candidate竞选leader失败、收到相同term的新leader的Heartbeat的时候，不应该重置当前`voteFor`
- 在`AppendEntries`RPC中，`If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)`，这里的new entry指的是RPC argument中最后一个entry，如果`entries`为空的话，就是`prevLogIndex`
- 在发送`AppendEntries`RPC并处理结果的过程中，如果需要回退重试，在重试前需要检查当前节点是否还是leader，节点的状态可能已经发生了更改

### Project3

- 日志的 snapshot 不仅需要包含状态机的状态，还需要包含用来去重的 `sessionMap`哈希表。
- 读请求直接生成一条 raft 日志去同步，这样可以以最简单的方式保证线性一致性（raft论文中描述得比较复杂）
- 对于`sessionMap`，不仅作为leader的`KVServer`要更新，作为client的也要更新
- 对于clerk发送的sequence number已经过期的request，clerk收到`OutOfDateErr`的回复之后，应该立刻返回而不是重试
- 如果出现两个相同index的entry共用一个reply channel，说明前一个entry已经失效，需要发送`OutOfDateErr`然后新创建一个channel

### Project4

- system arch：
  ![img](https://gitee.com/zhaohaoru/pic-bed/raw/master/assets/v2-94638b6f44e64804e921354b1965d528_r.jpg)
- 在实现的时候参考了以下一些很不错的reference：
  - [OneSizeFitsQuorum/MIT6.824-2021: 4 labs + 2 challenges + 4 docs (github.com)](https://github.com/OneSizeFitsQuorum/MIT6.824-2021)
  - [mit-6.824 2021 Lab4：ShardKV - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/464097239)