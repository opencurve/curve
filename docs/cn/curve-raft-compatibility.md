# 背景

兼容性改造主要是为了使用后期替换braft，对现有的结构不会有改动，包括集成测试等。

# 1. 配置持久化

## 1.1. 问题

**（1）复制组的成员信息持久化**

etcd-raft基于**init-cluster**的模式，也就是启动一个节点的时候，复制组成员都是通过参数传递进去的，所以在etcd-raft的`ConfState`仅仅有变更的peer id和变更的成员地址，但是对于multi-raft来说，尽管创建复制组的时候可以携带成员信息，但是如果不持久化，那么重启了就没了，那么如果要持久化要持久化哪些信息？下面是etcd-raft持久化的内容，仅仅包含了复制组的peer id信息，并不包含任何关于地址的信息

```protobuf
// 快照
message Snapshot {
	optional bytes            data     = 1; // snapshot的数据，一般实现都是uri
	optional SnapshotMetadata metadata = 2; // snapshot的元数据
}

// 快照meta
message SnapshotMetadata {
	optional ConfState conf_state = 1;  // 复制组的配置
	optional uint64    index      = 2;  // snapshot last include op log index
	optional uint64    term       = 3;  // snapshot last include op log term
}

// 复制组的配置
message ConfState {
	repeated uint64 nodes    = 1;   // Follower id list
	repeated uint64 learners = 2;   // Learner id list
}
```

- init复制组

通过配置文件会携带上所有的成员信息，放在配置文件或者在启动命令中指定：

`./etcd --name cd0 --initial-advertise-peer-urls http://127.0.0.1:2380 --listen-peer-urls http://127.0.0.1:2380 --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 --initial-cluster-token etcd-cluster-1 --initial-cluster cd0=http://127.0.0.1:2380,cd1=http://127.0.0.1:2480,cd2=http://127.0.0.1:2580 --initial-cluster-state new`

- 配置变更

以添加节点为例：

1. 首先初始一个集群`{cd0、cd1、cd2}`
2. 配置变更增加`cd3`
3. 成功之后，启动`cd3`，init的配置是`{cd0、cd1、cd2、cd3}`

老的成员如何知道`cd3`的地址？配置变更AddPeer会携带上`cd3`的地址信息，

```
// peer
struct Peer {
	// peer id
	required uint64 id      = 1;
	// peer 上下文，一般是peer的地址信息
	required string context = 2;
}
// 创建一个etcd-raft RawNode的接口
func NewRawNode(config *Config, peers []Peer) (*RawNode, error)
```

- 更新peer地址

提供了一种`ConfChangeUpdateNode`的配置变更选项，用于更新peer的地址信息，这种接口仅仅能满足修改**单个Peer**的address信息，并不能修改所有副本的address信息。



**（2）如何应对拔盘换机器，机房迁移这种场景？**

在这种场景下相当于chunkserver没变，仅仅地址变了

（2.1）拔盘（机器的维修：无论是软件固件的升级 抑或是 硬件的升级。）

相当于**复制组单个peer**的address信息发生了变更

（2.2）整体搬迁

相当于**复制组多个peer**的address信息发生了变更

尤其是2.2.整体搬迁，如果持久化address信息，那么没有办法进行更新，因为更新需要通过配置变更更新，但是所有地址都变，配置变更本身已经不能进行了

所以类似**braft做法**，不依赖第三方获取peer address在面对拔盘和迁移的时候并不可行

- 紧耦合的使用地址作为peer id `{ip:port:index}`；
- 配置变更日志，包含了所有peers的peer id list，那么就包含了复制组所有成员的地址了
- 新建的copyset，还没有打过快照，那么重启将无法获取peers的地址，所以braft刚当选leader的时候会通过flush当前的配置，替换noop，这样保证copyset创建之后，尽快将peers的地址复制给其他peer
- 每次重启，从snapshot和op log中加载配置变更entry，且将最后一个配置变更entry作为当前配置，因为配置变更每次只允许一个一个变更，那么可以保证op log entry中有且仅有一个配置变更的entry是可能没提交的，如果起来自己依然是leader，那么会通过flush一个没有做任何配置变更的配置变更entry提交前面的所有entry， 包含了最后一条配置变更entry，然后才开始提供服务（braft中配置变更采用的是new配置成员做投票决策的，这种方案需要配置变更只能一个一个的做）

缺点：

- 耦合性比较大，拔盘换机器，机房搬迁这种情况不好处理
- 现在创建copyset成功没有等leader选举成功，第一条配置变更entry commit之后在返回，那么可能会存在copyset创建成功之后，但是leader还没选举成功，那么重启之后，复制组成员信息还没有持久化，从而造成copyset不知道复制组成员是谁

好处：

- 不依赖任何第三方可以知道peers的地址信息
- 通过flush配置变更，使得不需要额外找地方持久化复制组的peer成员信息（考虑一种情况：尽管复制组配置信息会持久化到snapshot中，但是对于刚启动的复制组，还没有snapshot，就相当于配置信息没有持久化，造成节点重启的时候无法获取复制组成员和peers的地址信息）

braft这种方案尽管不像etcd-raft那样依赖`init-cluster`的方式指定peer成员，但是强耦合peer id和address信息，对于拔盘迁移和机房搬迁并不友好



**（3）解耦方案改动大**

tikv使用三层结构，完全解构peer和address

```protobuf
message Region {
    uint64 id = 1;
    // Region key range [start_key, end_key).
    bytes start_key = 2;
    bytes end_key = 3;
    RegionEpoch region_epoch = 4;
    repeated Peer peers = 5;
}
message Peer {      
    uint64 id = 1;
    uint64 store_id = 2;
    bool is_learner = 3;
}
message Store {
    uint64 id = 1;
    string address = 2;
    StoreState state = 3;
    repeated StoreLabel labels = 4;
    string version = 5;
    // more attributes......
}
```



- 配置变更中仅仅保留peer id和peer所对应的store id（相当于chunkserver），没有保存具体的address信息
- 复制组的peers信息保存在本地的RocksDB上面，随着配置变更日志apply而修改,raft内部依然和etcd-raft一样仅仅保存peer的id，peer相关的store信息持久化在rocksdb上面，raft statemachine不感知

好处：

- 可以做到数据和位置无关，无论是数据拷贝之后，还是拨盘换机器，都不会有任何问题，因为copyset peer没有和任何位置信息绑定，以拔盘换机器为例，这个copyset peer的数据重启之后，注册的时候带上peer id，就可以让pd知道自己的位置

缺点：

- 没有保存address，那么意味着store id -> address的映射需要依赖pd
- peer相关的store新需要在rocksdb上面持久化



这种方案扩展性好，但是引入了`Store`层，所以对于当前mds的改动会比较大



**总结：**

核心的问题如下：

1. 复制组成员信息peer id持久化
2. peer address如何获取，并且能够很好的使用拔盘和迁移



## 1.2. 方案

特性分析：

1. 当前chunkserver id和peer id是一一对应的，如果使用chunkserver id做为peer id，那么单个copyset在单个chunkserver上不能支持多个副本
2. 要适应拔盘和迁移，（2.1）最本质就是raft不能持久化address信息，raft address信息应该从外部获取，raft内部并不感知tikv中三层结构中的`Store`层，raft lib只关心peer id，并且可以通过peer id可以获取到address即可；（2.2）或者持久化，但不用配置变更entry持久化，因为一旦通过配置变更entry持久化了，就只能用配置变更修改



详细方案：

- （1）curve层面：chunkserver id作为peer id

考虑到chunkserver id和peer id在实际情况中是1对1的关系（因为一个副本一旦分配到某个指定的副本之后，就固定下来了，再也不会改变了），且这样有利于兼容当前mds的设计，那么在curve这个层面，看到的peer id就是chunkserver id，逻辑结果如下：

```protobuf
message Copyset {
    uint64 groupId		= 1;	// 复制组 id
    repeated Peer peers = 2;
}
message Peer {      
    uint64 peerId    	 = 1;
    bool isLearner 		 = 2;	// 是否是learner（暂时不支持）
    string address		 = 3;	// address，ip:port
}
```

所以原本Copyset -> Peer -> ChunkServer的三层结构可以优化成两层，只要不通过配置变更日志持久化Peer对应的address信息，那么就可以让ChunkServer id和address解耦

1. 关于learner角色，外部必须感知，保证后续可能会除了数据恢复需要learner角色。
2. peer id和chunk server id一一对应有一个缺点就是ChunkServer这个层面一个ChunkServer上面只能有一个副本，但是curve-raft层面没有这个限制，所以不会影响curve-raft的测试



- （2）raft层面：不感知tikv中的Store层或者说我们ChunkServer层面

etcd-raft state machine 不感知复制组id，仅仅感知 peer id和peer对应的可以获取地址的context，

```protobuf
// etcd-raft state machine peer
message Peer {
	// peer id
	required uint64 id      = 1;
	// peer 上下文，可以通过其获取地址信息
	required string context = 2;
}
```

在multi-raft那一层才感知复制组id，etcd-raft state machine的 msg出来之后会被添加上相应的groupId，然后再转发出去

```protobuf
// etcd-raft peers之间传递的消息
message Message {
	optional MessageType type        = 1;	// Msg类型
	optional uint64      to          = 2; 	// 接收端的id
	optional uint64      from        = 3; 	// 发送端的id
	optional uint64      term        = 4; 	// 0本地消息，非0非本地消息
	optional uint64      logTerm     = 5; 	// 以选举为例，最新日志的term
	optional uint64      index       = 6; 	// 以选举为例，最新日志的index
	repeated Entry       entries     = 7; 	// Op Log、配置变更、ReadOnly等
	optional uint64      commit      = 8; 	// 当前最新的commit
	optional Snapshot    snapshot    = 9; 	// 快照
	optional bool        reject      = 10; 	// 以日志复制为例，此次日志没有匹配上，拒绝reject=true
	optional uint64      rejectHint  = 11; 	// 在MsgApp被reject的时候，会携带上Follower last log index返回
	optional bytes       context     = 12;	// transfer leader阶段的选举投票，会将campaignTransfer放置在context上，让此次选举忽略Leader lease判断，也就是即使最近和Leader有通信，也会投赞成票，保证transfer leader能够选举成功
}

// multi-raft peer之间的msg传递
message StepRequest {
	message StepMessage {
		optional uint64 groupId  	= 1;    // 复制组id
		optional Message message 	= 2;	// etcd-raft message
	}
	repeated StepMessage stepMsg	= 1;	// 可以实现batch
}
```



- （3）配置信息持久化

1）copyset peer id 和 address信息持久化到meta文件中，这里定义为`copyset_meta`文件，curve-raft感知，chunkserver不感知，那么现在copyset的目录结构如下：

```
copyset_dir/
           /raft_meta/
                     /raft_meta
           /raft_log/
                    /log_meta
           			/log_inprogress_4
           		    /log_inprogress_1_3
           /raft_snapshot/
    			         /snapshot_meta
    			         /chunk_1
    			         /chunk_2
           /copyset_meta
```

copyset_meta持久化为`message Copyset`的反序列化结果

其中有一点需要注意，AddPeer为了让老复制组知道新增加Peer的地址，新增的Peer地址信息会携带在配置变更的Entry中

```protobuf
// curve-raft Peer
message Peer {      
    optional uint64 peerId    	 = 1;
    optional bool isLearner 		 = 2;	// 是否是learner
    optional string address		 = 3;	// address，ip:port
}
// 用户接收到的AddPeer
message AddPeerRequest {
    required uint64 groupId         = 1; // 全局唯一，复制组id
    required uint64 to              = 2; // 全局唯一，leader id
    required Peer addPeer           = 3; // add peer
}
// etcd-raft state machine处理的add peer配置变更
// 配置变更
message ConfChange {
	optional uint64          id      = 1;
	optional ConfChangeType  type    = 2;
	optional uint64          nodeID  = 3;
	optional bytes           context = 4;	// 配置变更AddPeerRequest的序列化结果或者add peer的address
}

 /**
  * @brief: curve-raft Node 增加一个成员接口
  * @param peerId[in]: 增加的成员
  * @param done[in]: 用户定义回调，一般包含上下文
  */
 void AddPeer(const Peer &addPeer, Closure *done);
 // 可以和下面的braft对比，PeerId是string类型，构成：{ip}:{port}:{index}
 virtual void add_peer(const PeerId& peer, Closure* done);
```

3）重启从从copyset meta中解析复制组成员信息，如果有，使用这些成员信息初始化；如果没有，那么使用NodeOption传入的init成员配置初始化

4）初始化创建复制组

刚开始生成一个copyset，会将copyset的每个peer分配到指定的chunkserver，然后携带上copyset详细信息去这个chunkserver上面创建相应的peer

ChunkServer收到的Copyset创建request

```protobuf
message CopysetRequest {
    required uint32 logicPoolId = 1;
    required uint32 copysetId 	= 2;
    repeated Peer peers 		= 3; // 当前复制组配置
};
```

到raft层面，转换成init成员配置，然后持久化到copyset_meta文件，然后执行其它必要的初始化工作，就可以返回了。这样保证一旦向用户返回copyset创建成功，那么复制组的成员信息就持久化了

- （4）peer address查询

尽管copyset_meta中持久化了peer的详细信息，包括address信息， 但是在拔盘或者迁移的场景中，address会发生变更，raft需要外面提供获取peer address的接口，一般由raft lib使用者实现并传入，例如在我们的场景中就是chunkserver

```
/**
 * 通过Peer id获取peer对应的address
 */
class PeerAddrResolver {
 public:
    virtual ~PeerAddrResolver() = default;

    /**
     * @brief: 通过peer id解析获取peer对应的address
     * @param peerId[in]: peer的唯一标识id
     * @param addr[out]: peer的地址
     * @return 0成功，-1失败
     */
    virtual int Resolve(uint64_t peerId,
                        butil::EndPoint addr);
};
```

curve-raft虽然持久化peer address信息，但是不保证其实最新的，curve-raft state machine仅仅持久peer id信息，一旦curve-raft不知道peer的address或者通过此address无法联系上peer，那么就会通过上面的接口获取最新的Peer address，关于Peer address信息保存，为了避免类似tikv直接向pd（mds）查询，可以通过心跳更新

以拔盘换机器为了：

1. 拔盘换到新的机器，重启
2. chunkserver注册更新address
3. mds通过心跳更新此chunkserver上的所有address信息
4. 复制组其他peer发现联系不上拔盘的peer，通过`Resolver`更新peer新的address重试，如果resolver没有获取到最新的地址，raft会反复重试，知道拿到最新的地址

**notes**

之所以不直接从mds直接拉，是因为不能区分是offline的peer，还是old address，因为在配置变更踢出去之前，会一直反复重试从`Resolver`解析peer最新地址重试，如果直接从mds拉取，可能会给mds造成比较大的压力



## 1.3. 修改

可能引入修改内容如下：

### 1.3.1. ChunkServer

（1）当前ChunkServer设计上一个ChunkServer只能存放一个复制组的一个副本，所以`ChunkService`这一层仅仅依赖复制组id，并不依赖任何的Peer信息

```
// read/write 的实际数据在 rpc 的 attachment 中
message ChunkRequest {
    required CHUNK_OP_TYPE opType = 1;  // for all
    required uint32 logicPoolId = 2;    // for all  // logicPoolId 实际上 uint16，但是 proto 没有 uint16
    required uint32 copysetId = 3;      // for all
    required uint64 chunkId = 4;        // for all
    optional uint64 appliedIndex = 5;   // for read
    optional uint32 offset = 6;         // for read/write
    optional uint32 size = 7;           // for read/write/clone 创建快照请求中表示请求创建的chunk大小
    optional QosRequestParas deltaRho = 8; // for read/write
    optional uint64 sn = 9;             // for write/read snapshot 写请求中表示文件当前版本号，读快照请求中表示请求的chunk的版本号
    optional uint64 correctedSn = 10;   // for create clone chunk 用于修改chunk的correctedSn
    optional string location = 11;      // for clone chunk
};
```

所以，这一个部分并不需要任何修改，ChunkServer收到ChunkRequest之后，会将ChunkServer id作为peer id，打包成Task Propose

（2）测试，单进程多ChunkServer支持

当前设计上一个ChunkServer只能放置一个复制组副本；而且我们认为ChunkServer id和peer id相同，那么就不容易实现单进程多ChunkServer了，可行的办法是，通过单个进程起多个端口，不同的ChunkServer走不同的端口，这样不同的ChunkServer就不能有依赖的东西，目前来看，将单例的CopysetNodeManager改成非单例的即可

（3）配置变更相关的接口

当前braft的peer id是`string`类型，组成是`{ip}:{port}:{index}`，Chunk的read/write对peer不感知，但是集成测试需要用到配置变更相关的接口，会和braft完全不一样，当前的配置变更接口

```protobuf
// 1. braft场景: id不使用，address为braft里面的PeerId，格式为{ip}:{port}:{index}
// 2. curve-raft场景：id是peer id，address为{ip}:{port}
message Peer {
    optional uint64 id          = 1;    // peer id，全局唯一
//  optional bool isLearner     = 2;    // 是否是learner (暂时不支持)
    optional string address     = 3;    // peer的地址信息
}
// 这里都用 logicPoolId, copysetId,进入 rpc service 之后，会转换成 string
// 类型的 groupId，在传给 raft
// |        groupId          |
// | logicPoolId | copysetId |
message AddPeerRequest {
    optional uint32 logicPoolId = 1;    // logicPoolId 实际上 uint16，但是 proto 没有 uint16
    optional uint32 copysetId 	= 2;
    optional Peer leader 		= 3
    // required string leader_id = 3;	// braft
    optional Peer addPeer 		= 4;
    // required string peer_id  = 4;	// braft
}
```

为了兼容性，leader和addPeer都是用`Peer`，这样在底层使用braft的时候， id不使用，address为braft里面的PeerId，格式为{ip}:{port}:{index}，如果是curve-raft场景，那么id是peer id，address为{ip}:{port}



（4）创建Copyset Service

当前的创建Copyset的接口严重依赖braft string类型的peerId

```protobuf
message CopysetRequest {
    // logicPoolId 实际上 uint16，但是 proto 没有 uint16
    required uint32 logicPoolId = 1;
    required uint32 copysetId = 2;
    repeated string peerid = 3; // 当前复制组配置，可以为空
};
```

需要将其替换成`Peer`

```protobuf
// curve-raft Peer
message Peer {      
    optional uint64 peerId    	 = 1;
    optional bool isLearner      = 2;	// 是否是learner
    optional string address		 = 3;	// address，ip:port
}
message CopysetRequest {
    // logicPoolId 实际上 uint16，但是 proto 没有 uint16
    required uint32 logicPoolId = 1;
    required uint32 copysetId = 2;
    repeated Peer peers = 3;
};
```

传递给braft的时候只用Peer里面的address，peer id目前可以暂时忽略到，后期替换的时候再用。相应的CopysetNode接口相应的如下接口，也将PeerId替换成Peer即可

```
   butil::Status TransferLeader(const PeerId& peerId);

    /**
     * @brief 复制组添加新成员
     * @param[in] peerId 新成员的ID
     * @return 心跳任务的引用
     */
    butil::Status AddPeer(const PeerId& peerId);

    /**
     * @brief 复制组删除成员
     * @param[in] peerId 将要删除成员的ID
     * @return 心跳任务的引用
     */
    butil::Status RemovePeer(const PeerId& peerId);
```



（4）Peer address Cache（这个前期可以不用适配，后面替换之后在适配即可）

因为peer address信息是通过mds的heartbeat更新peer address信息，所以ChunkServer再实现`PeerAddrResolver`接口的时候需要实现Peer address的Cache（不持久化）



（5）CopysetNode接口修改

实际上使用面向下面这种接口的方式，还是比较困难，因为CopysetNode上面的接口比较多，不太容易完全抽象到基类上面，而且后期的`StateMachine`也不是完全一致的，所以将不采用面向接口的方式去修改，直接修改一致性协议相关的接口，让其替换成`Peer`，后期StateMachine重新实现之后。

```
// CopysetNode接口
class CopysetNode : public braft::StateMachine;

// braft具体实现
class BRaftNode : public CopysetNode;

// curve-raft具体实现
class CRaftNode : public CopysetNode;
```







### 1.3.2. Client

（1）目前client仅仅使用了`GetLeader`的查询接口：

```protobuf
message GetLeaderRequest {
    required uint32 logicPoolId = 1;
    required uint32 copysetId = 2;
    optional string peer_id = 3;
}
message GetLeaderResponse {
    required string leader_id = 1;
}
// service
service CliService {
    rpc get_leader(GetLeaderRequest) returns (GetLeaderResponse);
};
```

需要修改成如下接口：

```protobuf
// 方案1
message GetLeaderRequest {
    required uint32 logicPoolId = 1;
    required uint32 copysetId 	= 2;
    optional Peer peer 			= 3;
}
```

直接使用`Peer`，braft只用`Peer`的 address，后期替换只用`Peer`的peer id

client的元数据组织方式似乎不需要改动，只是这里的chunkserver id和peer id是等价的

`index-> chunk id，logic pool id，copyset id`

`copyset id -> peers info { chunkserver id，chunkserver addr }`



### 1.3.3 Mds

（1）需要增加learner角色支持（暂时不支持）

对于当前的恢复来说，可以不用让mds感知learner，chunkserver自己感知就好。但是考虑到后期的可扩展性，可以考虑在topology逻辑相应的peer info里面增加相应的`isLearner`的字段，目前来看raft只需要判断是否是learner的角色，可以不考虑`role`来实现更大的扩展性

（2）heartbeat request/reponse（**现在是否实现通过mds更新peer address**）

当前hearbeat request/reponse都是使用的都是

```protobuf
message Peer {      
    uint64 peerId    	 = 1;
//    bool isLearner 		 = 2;	// 是否是learner（暂时不支持）
    string address		 = 3;	// address，ip:port
}

message CopysetInfo {
    required uint32 logicalPoolId = 1;
    required uint32 copysetId = 2;
    // copyset replicas, IP:PORT:ID, e.g. 127.0.0.1:8200:0
    // repeated Peer peers = 3;
    repeated string peers = 3;
    // epoch, 用来标记配置变更，每变更一次，epoch会增加
    required uint64 epoch = 4;
    // 该复制组的leader
    // repeated uint64 leaderPeer = 5;
    required string leaderPeer = 5;
    // 配置变更相关信息
    optional ConfigChangeInfo configChangeInfo = 6;
    // copyset的性能信息
    optional CopysetStatistics stats = 7;
};

// TODO(合并)： add information of peerID
message ConfigChangeInfo {
	// required Peer peer = 1;
    required string peer = 1;
    // 配置变更是否成功
    required bool finished = 2;
    // 变更的error信息
    optional CandidateError err = 3;
};

message CopysetStatistics {
    required uint32 readRate = 1;
    required uint32 writeRate = 2;
    required uint32 readIOPS = 3;
    required uint32 writeIOPS = 4;
}

message ChunkServerHeartbeatRequest {
    required uint32 chunkServerID = 1;
    required string token = 2;
    required DiskState diskState = 3;
    required uint64 diskCapacity = 4;
    required string ip = 5;
    required uint32 port = 6;
    required uint64 diskUsed = 7;
    // 返回该chunk上所有copyset的信息
    repeated CopysetInfo copysetInfos = 8;
    // 时间窗口内该chunkserver上leader的个数
    required uint32 leaderCount = 9;
    // 时间窗口内该chunkserver上copyset的个数
    required uint32 copysetCount = 10;
    // chunkServer相关的统计信息
    required ChunkServerStatisticInfo stats = 11;
};

// TODO(合并): ConfigchangeInfo-ConfigChangeType
enum ConfigChangeType {
    // 配置变更命令： leader转换
    TRANSFER_LEADER = 1;
    // 配置变更命令： 复制组增加一个成员
    ADD_PEER = 2;
    // 配置变更命令： 复制组删除一个成员
    REMOVE_PEER = 3;
    // 配置变更命令： 没有配置变更
    NONE = 4;
};

message CopysetConf {
    required uint32 logicalPoolId = 1;
    required uint32 copysetId = 2;
    // repeated Peer peers = 3;
    repeated string peers = 3;
    required uint64 epoch = 4;
    optional ConfigChangeType type = 5;
    optional string configchangeItem = 6;
};

message ChunkServerHeartbeatResponse {
    // 返回需要进行变更的copyset的信息
    repeated CopysetConf needUpdateCopysets = 1;
};
```

主要是需要将上面的string类型的peer id替换成Peer



（3）查询peer address

有两种方案：

（3.1）mds通过hearbeat更新（**这个修改下个迭代是否包含**）

chunkserver在拔盘移动之后，会向mds注册更新，mds发现chunkserver address更新了，那么会更新此ChunkServer上面对应的peer的address信息，通过heartbeat传给chunkserver

（3.2）mds接口查询

拓扑逻辑-增加查询指定ChunkServer地址的 service，这种方式有一个**缺点：会产生大量的想mds查询的动作**，因为地址更新和offline并不能区别。



# 2. 快照数据面

## 2.1. 问题

当前raft内部install snapshot的时候，会从leader上面拷贝chunk，这部分chunk的生成并没有经过chunkserver的chunkfile pool，所以需要通过接口导入接管raft内部的chunk生成。

## 2.2. 方案

主要考虑两个点的接管：

1）create一个chunk file（从chunk file pool取）

install snapshot的时候，follower下载snapshot内部的chunk file

2）delete一个chunk file（回收给chunk file pool）

install snapshot的时候，如果安装出错，那么将在Snapshot close的时候删除destroy snapshot



（1）通过braft的`NodeOptions`的Snapshot `FileSystemAdaptor`传入

```c++
struct NodeOptions {
    scoped_refptr<FileSystemAdaptor>* snapshot_file_system_adaptor;    
};
```

那么需要重新实现`FileSystemAdaptor`的`open`和`delete_file`文件接口

```c++
class FileSystemAdaptor : public butil::RefCountedThreadSafe<FileSystemAdaptor> {
public:
    virtual FileAdaptor* open(const std::string& path, int oflag, 
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e) = 0;
    virtual bool delete_file(const std::string& path, bool recursive) = 0;
}
```

具体实现，可以继承`FileSystemAdaptor`的默认实现`PosixFileSystemAdaptor`并重写`open`和`delete_file`两个接口



（2）增加一个`FilePool`接口传入，实际的实现者需要组合依赖ChunkfilePool来实现FilePool GetChunk和RecycleChunk接口，然后通过`NodeOptions`导入

```c++
struct NodeOptions {
    std::shared_ptr<FilePool> filePool_;    
};

/**
 * FilePool接口，用于braft内部接管snapshot的数据，实际的实现者需要组合ChunkfilePool
 * 来实现FilePool GetChunk和RecycleChunk接口
 */
class FilePool {
 public:
    /**
     * @brief: datastore通过GetChunk接口获取新的chunk，GetChunk内
     * 部会将metapage原子赋值后返回。
     * @param: chunkpath[in]: 是新的chunkfile路径
     * @param: metapage[in]: 是新的chunk的metapage信息
     */
    virtual int GetChunk(const std::string& chunkpath) = 0;

    /**
     * @brief: datastore删除chunk直接回收，不真正删除
     * @param: chunkpath[in]: 是需要回收的chunk路径
     */
    virtual int RecycleChunk(const std::string& chunkpath) = 0;
};
```



# TODO & 计划

## 计划


mds端

- topology等mds相关模块接口方面依赖peer id的改成Peer
- heartbeat相关Message替换成Peer，相关接口依赖peer id替换成 Peer（通过heartbeat更新address，后期再做）

chunkserver端

- cli service 和 copyset service 替换成Peer（支持一次创建多个copyset）
- 重新封装CopysetNode接口，最好改成面向接口的方式（CopysetNode、BRaftNode、CRaftNode）
- chunkfile pool数据面接管



## TODO

#1 目前暂时用不上learner，可以考虑后面再全面添加learner支持

#2 数据面的完全兼容（这个很难，以后curve-raft持久化很难和braft完全兼容）

































