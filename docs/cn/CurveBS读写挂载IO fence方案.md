# CurveBS读写挂载IO fence方案

## 背景

目前云原生数据库团队，对CurveBS共享存储，提出了读写挂载的需求，要求CurveBS提供这样的能力：

假设数据库有两个计算节点A、B、C，分别挂载了pfs文件系统。其中A为leader，文件系统读写权限。B和C为follower，有只读权限。若A节点进程宕机，B或C节点被选为新leader，需要开启文件系统读写权限。

当数据库集群出现脑裂时，现有的方案另一个从库无法重新以读写模式挂载上去。

## 设计方案

### 设计目标：

1.  当读写挂载发生切换时，旧的写挂载节点假死或者脑裂场景下，新的节点能够重新以写挂载模式挂载上去，并且新的节点写挂载后，阻止旧的写挂载节点的inflight写请求，从而保证读写的一致性。  

### 设计思路：

1.  基本思路还是以epoch的方式，提供对旧请求的隔离，每次发生写挂载的client切换时，对epoch+1，从而隔离旧的client携带旧epoch的请求;
2.  epoch需要记录到chunkserver端，因为client对segment进行了缓存，通常写请求只会跟chunkserver通信，因此，需要chunkserver对比本地保存的epoch与写请求携带的epoch，从而阻止旧的epoch的请求;
3.  epoch的增加考虑在mds端，epoch的递增应当是原子的，因此需要一个中心节点，使用原子变量（或锁）去原子的增加epoch，从而避免并发场景下的数据竞险; 
4.  在mds端原子的增加epoch后，需要向chunkserver广播epoch的变更，使chunkserver感知epoch的变化; 那么由谁去广播，这里选择client去广播epoch，原因有二： 一是如果由mds去广播，那么整个超时时间不好设置，随着集群的增大，广播的总时间必然会越来越长；二是存在一种特殊的网络分区情况，即mds与chunkserver之间无法通信，而client与chunkserver之间可以通信的情况下，由client去广播可以避免这一问题;
5.  epoch以卷的粒度去记录，而不是chunk的粒度，使用chunk的粒度记录epoch的更新的广播请求过多。例如一个20T的卷，如果采用chunk粒度去记录，则最多需要20T / 16M = 1310720个请求；因此，使用卷的粒度去记录epoch;
6.  由上，epoch需要持久化在mds中： mds端，考虑记录在卷的FileInfo中；chunkserver端，记录一个map，提供卷ID到epoch的映射，不需要持久化，而是每次chunkserver启动时从mds获取缓存;
7.  epoch的更新采用独立接口，独立于client原有接口之外，考虑到使用epoch的方式会对性能有影响，大多数情况下不需要使用，只在数据库写节点切换等特定场景下，通过主动调用接口的方式去实施；


### 数据结构：

- mds端可将epoch保存在FileInfo结构中：

```
 message FileInfo {
    optional    uint64      id = 1;
    optional    string      fileName = 2;
    optional    uint64      parentId = 3;
    optional    FileType    fileType = 4;
    optional    string      owner = 5;
    optional    uint32      chunkSize = 6;
    optional    uint32      segmentSize = 7;
    optional    uint64      length = 8;
    optional    uint64      ctime  = 9;
    optional    uint64      seqNum =  10;
    optional    FileStatus  fileStatus = 11;
    //用于文件转移到回收站的情况下恢复场景下的使用,
    //RecycleBin（回收站）目录下使用/其他场景下不使用
    optional    string      originalFullPathName = 12;

    // cloneSource 当前用于存放克隆源(当前主要用于curvefs)
    // 后期可以考虑存放 s3相关信息
    optional    string      cloneSource   =    13;

    // cloneLength 克隆源文件的长度，用于clone过程中进行extent
    optional    uint64      cloneLength =  14;
    optional    uint64      stripeUnit = 15;
    optional    uint64      stripeCount = 16;
 	// 新增
	optional    uint64      epoch = 17;
}
```

- chunkserver端epoch记录在内存map中,  在每次启动时从mds拉取epoch信息保存到内存中，因此不需要持久化

- client端写请求需要修改，需增加file id和epoch字段：

```
 message ChunkRequest {
    required CHUNK_OP_TYPE opType = 1;  // for all
    required uint32 logicPoolId = 2;    // for all  // logicPoolId 实际上 uint16，但是 proto 没有 uint16
    required uint32 copysetId = 3;      // for all
    required uint64 chunkId = 4;        // for all
    optional uint64 appliedIndex = 5;   // for read
    optional uint32 offset = 6;         // for read/write
    optional uint32 size = 7;           // for read/write/clone 读取数据大小/写入数据大小/创建快照请求中表示请求创建的chunk大小
    optional QosRequestParas deltaRho = 8; // for read/write
    optional uint64 sn = 9;             // for write/read snapshot 写请求中表示文件当前版本号，读快照请求中表示请求的chunk的版本号
    optional uint64 correctedSn = 10;   // for CreateCloneChunk/DeleteChunkSnapshotOrCorrectedSn 用于修改chunk的correctedSn
    optional string location = 11;      // for CreateCloneChunk
    optional string cloneFileSource = 12;   // for write/read
    optional uint64 cloneFileOffset = 13;   // for write/read
    // 新增
	optional uint64 fileId = 14;
	optional uint64 epoch = 15;
};
```

### 流程：

#### 数据库写挂载切换场景：

初始状态:

1.  client1 第一次挂载为写节点， client1从mds处获取最新的fileInfo，此时epoch为1（初始值，也可从0开始），client1分配segment后，携带file id 和 epoch 开始写第一个chunk
2.  chunkserver 首先判断写请求中的epoch与chunkserver端保存的epoch，此时发现chunkserver端的epoch不存在， 更新chunkserver端epoch，创建chunk


client切换：

1.  client2 挂载为写节点，首先调用IncreaseEpoch接口，调用mds 更新epoch，此时epoch更新为2，并持久化到fileInfo中
2.  mds向client2返回新的Epoch和需要广播的chunkserver列表，client2广播epoch更新到所有chunkserver
3.  chunkserver更新对应file id的新的epoch，并返回成功给client2
4.  client2发起mount（对应curve卷的open），此时需要拉取当前卷的Epoch信息；
5.  client2开始携带file id 和epoch = 2发送写请求
6.  由于保证了chunkserver已经更新epoch，client1携带epoch = 1的请求将被chunkserver拒绝，从而返回错误；
7.  对于未分配segment的chunkserver，我们还需要在allocsegment的接口中检查epoch，从而阻止client1分配新的segment；另外，client2新分配的segment，携带epoch=2的写请求在发现chunkserver端epoch未更新时，也需要更新epoch；

### 异常场景考虑

-   mds异常的场景下，只需要简单重试就可以了
-   chunkserver存在异常的场景下，client在广播epoch时，如果部分chunkserver offline不能通知到，并且在之后如果online了怎么办？  
    client将epoch信息广播到chunkserver，如果遇到chunkserver 联系不上时，先进行重试，如果确认chunkserver已经hostdown，则延迟通知该chunkserver； 待到chunkserver online时，由心跳或者主动获取一次Epoch，从而更新chunkserver上的epoch；

## 接口设计：

PFS层对外接口新增：
```
// 执行成功返回0， 执行失败返回-1，并设置errno
// 入参与pfsd_mount一致
int pfsd_increase_epoch(const char *cluster, const char *pbdname, int hostid, int flags);


此外，考虑到对PG的兼容性，也可以在pfsd启动参数中开启IO fence功能，开启该功能后，对于写挂载的pfsd_mount调用，内部自动调用pfsd_increase_epoch接口；
```
  

Client端接口：
```
// 执行成功返回0， 执行失败返回-1
// filename: curve卷的path
int cbd_libcurve_increase_epoch(const char* filename);
```
  

## 改动工作量评估：

client端：

1.  需要增加一个IncreaseEpoch接口，该接口调用mds IncreaeEpoch接口，并返回最新的Epoch和需要更新Epoch的chunkserver列表
2.  client需要更新epoch到chunkserver，发起UpdateEpoch请求到chunkserver
3.   写请求相关逻辑需要修改，写请求需要携带卷Id和epoch
4.  GetOrAllocSegment相关接口需要修改，需要携带epoch
5.  Open接口需要修改，需要从mds获取epoch信息，并缓存
6.  错误处理，对于返回EPOCH_TOO_OLD错误的写请求，可以直接丢弃，不向上层返回，对于返回EPOCH_TOO_OLD错误的GetOrAllocSegment的请求，一般是写触发的，同样丢弃触发该请求的写请求；如果是读请求触发的GetOrAllocSegment，一般是获取的已分配的segment，不需要处理；


mds端：

1.增加一个IncreaseEpoch接口，该接口首先原子的自增保存在FileInfo中的epoch，并返回client epoch和需要更新epoch的chunkserver列表
2. GetOrAllocSegment接口需要修改，增加对epoch的判断，如果epoch判断失败，则阻止 GetOrAllocSegment，返回EPOCH_TOO_OLD错误


chunkserver端：

1.写Chunk接口需要修改，首先需要判断epoch是否大于等于当前保存的epoch，如果未保存epoch，则返回成功；如果epoch小于当前保存的epoch，则拒绝当前写请求，并返回EPOCH_TOO_OLD错误
2. 需要提供UpdateEpoch接口，并将卷ID和Epoch的映射关系更新到内存中；
3. chunksever启动时，需要主动拉取所有卷的epoch信息，保证chunkserver总是有最新的epoch信息；  

PFS修改：

1.增加pfsd_increase_epoch接口，调用client的相应接口
2. 增加pfsd启动参数，开启IOFence的情况下，在pfsd_mount中调用pfsd_increase_epoch。
