<!--
 * @Project: curve
 * @Date: 2022-02-21 14:35:19
 * @Author: chenwei
-->

# 1、背景

curvefs 的元数据保存在的 metaserver 上，元数据主要是 fs 的 inode 和 dentry 信息。元数据的一致性和可靠性通过 copyset 来保证。为了应对海量小文件，curvefs 的元数据会通过一定的规则进行分片，分片的单位为 partition。每个 metaserver 上有多个 copyset， 每个 copyset 管理多个 partition，每个 paritition 管理着一个fs的一段范围的 inode 和 dentry。copyset 目前不支持分裂，上面的数据不能拆分。copyset 可以从一个 metaserver 迁移到其他 metaserver。

metaserver → multi copyset → multi partition

当前 curvefs 的元数据都存储在 metaserver 集群上，copyset 分布在哪些 metaserver 上，partition 在的哪个copyset上，在创建 copyset 和创建 partition 的时候决定的。随着系统的不断使用，元数据集群的上面的元数据集群会慢慢变得不均衡。需要一种机制来调整元数据集群中的元数据的分布。

当前 curvefs 的元数据全部缓存在内存，且当前的 curvefs 的 inode 实现机制决定了，inode 的大小可能会膨胀到上百MB，这种巨无霸类型的 inode 可能会耗尽 metaserver 上的内存资源，为了防止 metaserver 上资源耗尽导致文件系统更新元数据失败，需要提前监控 metaserver 的资源使用情况，发现 metaserver 资源使用到一定限度之后，提前采取措施进行元数据的均衡。

# 2、方案

curvefs 元数据的均衡这里从两个方面考虑：

> 1、创建 partition 和 copyset 时尽量均衡；
> 2、在系统的使用过程中，根据 metaserver 的资源使用情况，动态的对资源进行均衡。

## 2.1 元数据均衡考虑因素

如果 metaserver 的元数据全部缓存，那么 metaserver 资源最大的限制是内存，metaserver 的内存使用情况是第一优先级因素。

如果 metaserver 采用DB的方式而不是元数据全缓存的方式保存元数据，内存就不再是限制 metaserver 的主要因素。因为 DB 的方式来保存元数据，元数据保存在本地 DB 中，metaserver 使用的内存可以配置。这种情况下，限制 metaserver 的能力主要因素是是 DB 能够使用的磁盘的空间。

主要有以下考虑点：

> 1、metaserver 的元数据全缓存和 DB 方式保存，这两方案的是否并存，是否两种调度都需要支持。
>
> >两种方式都保留，mds 不需要关注 metaserver 的实现方式，由 metaserver 自己上报需要的资源情况以及当前的资源使用情况作为调度决策依据。
>
> 2、curvefs的部署架构是什么样的：metaserver 服务是否需要混布。每台server上部署部署几个 metaserver。每块盘上部署一个 metaserver，还是多个 metaserver 共享一块盘。
>
> >metaserver会考虑混布，每台server上部署1至多个 metaserver，每个 metaserver 独占一块盘。
>
> 3、如果采用 kv 方式的部署，对资源的占用情况是什么样的，比如的有没有 CPU 最低要求，内存的最低要求，磁盘的预留情况。
>
>> 相关信息，由 metaserver 向 mds 上报。

## 2.2 均衡需要的决策信息的收集

### 2.2.1 metaserver registe 信息上报

一部分信息在 metaserver 向 mds 注册时候收集，这些信息会保存在 mds 的 topology 模块中。供后续的均衡决策。

当前上报的信息：

```
// MetaServer message
message MetaServerRegistRequest {
    required string hostName = 1;
    required string internalIp = 2;
    required uint32 internalPort = 3;
    required string externalIp = 4;
    required uint32 externalPort = 5;
};
```

需要增加的上报信息：

```
上报信息暂时不需要新增
```

### 2.2.2 metaserver heartbeat信息上报

还有一部分信息，在 metasever 服务运行中上报。这些信息是 metasever 使用情况的统计，通过 metasever 定期向 mds 发送心跳收集，心跳一方面同步给 mds 的 topology 模块，一方面同步给 mds 的 schedule 模块。

当前上报的信息：

```
message MetaServerHeartbeatRequest {
    required uint32 metaServerID = 1;
    required string token = 2;
    required string ip = 3;
    required uint32 port = 4;
    required uint64 startTime = 5;
    repeated CopySetInfo copysetInfos = 6;
    required uint32 leaderCount = 7;
    required uint32 copysetCount = 8;
    required uint64 metadataSpaceTotal = 9;  // KB
    required uint64 metadataSpaceUsed = 10;  // KB
    required uint64 memoryUsed = 11;  // KB, 机器的内存使用情况
};

message CopySetInfo {
    required uint32 poolId = 1;
    required uint32 copysetId = 2;
    // copyset replicas, IP:PORT:ID, e.g. 127.0.0.1:8200:0
    repeated common.Peer peers = 3;
    // Used to mark configuration changes, each time there is a change, the epoch will increase
    required uint64 epoch = 4;
    required common.Peer leaderPeer = 5;
    repeated common.PartitionInfo partitionInfoList = 6;
    optional ConfigChangeInfo configChangeInfo = 7;
};
```

需要增加的上报信息：

```
message MetaServerSpaceStatus {
    // The metaserver needs to consider the disk reservation.
    // The value passed here is the physical capacity of the disk multiplied
    // by a certain percentage.
    required uint64 diskThresholdByte = 1;
    // Minimum disk requirements for a copyset
    required uint64 diskCopysetMinRequireByte = 2;
    // The current disk usage of this metaserver
    required uint64 diskUsedByte = 3;

    // The maximum amount of memory this metaserver can use
    required uint64 memoryThresholdByte = 4;
    // Minimum memory requirement for copy on this metaserver
    required uint64 memoryCopySetMinRequireByte = 5;
    // The amount of memory used by the current process of this metaserver
    required uint64 memoryUsedByte = 6;
}

message MetaServerHeartbeatRequest {
    required uint32 metaServerID = 1;
    required string token = 2;
    required string ip = 3;
    required uint32 port = 4;
    required uint64 startTime = 5;
    repeated CopySetInfo copysetInfos = 6;
    required uint32 leaderCount = 7;
    required uint32 copysetCount = 8;
    required MetaServerSpaceStatus spaceStatus = 9;
};

```

## 2.3 创建partition和copyset时的元数据均衡

mds 的 topology 有整个 curvefs 集群的 metaserver 的使用情况，作为创建 partition 和 copyset 的决策依据。

### 2.3.1 当前 partition 和 copyset 的创建算法

#### 2.3.1.1 copyset的创建

```
1、mds 收到来自 client 的创建1到多个 partition 请求。
2、topology 模块的查询可以用的 copyset，每个 copyset 上面的 partition 有个上限，这个上限可在 mds.conf 中进行配置，超过 partition 个数上限的 copyset 不能继续创建 partition。
3、如果有可用 copyset，选择上面的 partition 个数最少的 copyset，对每个 paritition 都用同样的方式挑选一个的 copsyet。
4、如果创建 partition 时，没有可用 copyset，需要先创建 copyset，然后再选择 copyset。
5、创建 copyset 时，在所有 online 且还有磁盘剩余的 metaserver 上，选择同一个 pool 的不同 zone 的 metaserver。尽量 metaserver 的磁盘使用率更低的 metaserver。
```

#### 2.3.1.2 partition的创建

```
创建 partition 时，在可用的 copyset 中选择一个 partition 个数较少的 copyset。
一个 copyset 是否可以继续创建 partition，由上面的 partition 的个数决定。
```

创建 copyset 时，由 metaserver 上面的资源情况决定。

### 2.3.2 新的 partition 和 copyset 的创建算法

当前 partition 和 copyset 的创建算法，没有考虑的 metaserver 的内存的使用情况。在全内存设计中，相比于 metaserver 的磁盘使用情况，metasever 的内存使用情况，才是制约 metaserver 上保存的 meta 量的决定性因素。

#### 2.3.2.1 创建partition时选copyset的算法

```
1、创建 paritition 一次创建 1~多 个 parititon，对于每个 partition 创建，执行以下步骤2~4。
2、获取可用的 copyset 列表；
3、如果可用的 copyset 个数小于系统设定值 n（默认为10），创建 x 个 copyset，x = n - copyset个数；
4、在所有可用的 copyset 中，随机选择一个 copyset，选中这个 copyset 创建 partition。
```

#### 2.3.2.2 创建copyset的算法

**算法1：**

该算法在系统第一次创建 copyset 时调用，一次创建一批 copyset。
理念是尽量均衡的在系统中创建copyset。
```
1、轮询所有 pool，每个每次轮询到的 pool 创建1个 copyset。
2、每个 pool 中，如果 zone 的个数小于副本数 n（配置文件默认为3），退出，进入下一次循环。
3、随机选择 n 个zone，这 n 个 zone 中，每个zone随机选择一个metaserver，由这些 metaserver 组成一个 copyset。
```

**算法2：**

该算法在系统的可用的 copyset 设定的值时，补齐可用的copyset的时候使用。

```
1、从 topology 获取集群所有 metaserver 信息。
2、过滤出这个 pool 所有满足条件的 metaserver。要求 metaserver 状态为 online，磁盘有剩余，内存没有达到 metaserver 限制，上面的 copyset 个数没超过限制。
3、对 pool 内满足条件的 metaserver 按照 zone 进行聚合，如果 zone 的个数不满足最低副本条件，在未选择的pool中重新选择一个新的 pool，重新从步骤2开始。
4、对 metaserver，按照内存使用率/磁盘使用率进行排序。
5、选择这个 pool 不同 zone 的 metaserver，尽量选择内存使用率的更低/磁盘使用率更低的 metaserver。
7、如果不能选择出足够的 metaserver，返回创建失败，打印 ERROR 级别错误日志。这个时候，运维人员需要考虑对系统进行扩容。

```

**整体算法**
```
1、获取 copyset 列表，根据copyset个数选择不同的创建算法。
2、如果 copyset 个数为0，使用 算法1 一次性创建系统设定值 n 个 copyset。
3、如果 copyset 个数大于 n，获取可用的 copyset 个数 m，使用 算法2 创建 n - m 个copyset。
```

## 2.4 系统运行过程中的动态元数据均衡

当前 curvefs 的调度模块，并没有对元数据进行均衡调度。

由于当前 copyset 不支持拆分，所以只能尽量在 copyset 和 partition 创建的时候，为 copyset 和 partition 预留足够的空余。

当 metaserver 或者 metasever 所在的 server 的资源达到一定的限度，可以从资源使用超过限制的 metaserver 选择一个 copyset 迁移到一个相对比较空闲的 metaserver 上。

### 2.4.1 调度框架的均衡调度实现

```
1、向调度框架注册均衡调度器，调度框架会周期性的调用调度算法对整个集群进行均衡调度。
2、调度的范围是一个 pool 内部。pool 之间的资源不能相互调度。
3、调度的粒度为一个 copyset，每次生成调度行为，意味着有一个 copyset 会发生调度。调度时，会把 copyset 的某个副本迁移到另一个 metaserver 上。
4、每次调度算法会计算本轮是否需要调度，调度的依据为 Metaserver 上资源使用情况。考虑资源使用情况时，不同 metaserver 上的 copyset 的个数差异并不作为考虑依据。不会因为某个 metaserver 上 copyset 数目多，另一个 metaserver 上 copyset 数目少就发生调动。因为的每个 copyset 管理的元数据理论上并不均衡，copyset 的个数并不能的反映资源的使用情况。根据 metaserver 的实现方式，选择的系统最紧缺的资源进行调度，如果是全内存缓存的方式，以内存的使用情况进行调度；如果是 DB 的方式，以磁盘的使用情况进行调度。
5、当前可以采取一种简易调度的方式，原则上是从资源紧张的 metaserver 调度到资源相对空缺的 metaserver 上。需要考虑两种情况:
5.1  metaserver 上的资源超过了水位线，需要把部门资源从上面调度走；
5.2 新增的 metaserver 上面没有资源，需要从其他的 metaserver 上调度资源过来。
```

整体算法实现

```
1、mds 的 schedule 调度框架，周期性的执行均衡调度算法。
2、每次调度算法，依次对集群的每个 pool 进行调度。
3、每个 pool 中，遍历所有的 metaserver，检查是否有 metaserver 上的资源超过了水位线，对每一个超过水位线的 metaserver 进行处理。
3.1 如果有 metaserver 超过了水位线，遍历 metaserver 上的每一个 copyset，是否可以找到一个 metaserver 迁移过去。
3.1.1 如果有满足条件的目的地 metaserver，在所有满足条件的 metaserver 中，选择一个资源使用量最小的 metaserver。生成调度任务。
3.1.2 如果没有满足条件的目的地 metaserver ，打印ERROR日志，并继续处理下一个。
3.2 如果 pool 中没有 metaserver 超过水位线，检查是否有资源使用率过低的 metaserver。判断标准为资源使用率最低的 metaserver 的资源使用率是否比 pool 的平均资源使用率低，且使用率的差值低于30%（这个数字可通过配置文件配置）。选择一个资源使用率相对较高的 metaserver，迁移一个 copyset 的副本到的资源使用率较低的 metaserver。
```

### 2.4.2 资源使用率的计算算法

当前主要考虑两种资源，一种是内存，一种是磁盘。metaserver 的心跳会定时上报 metaserver 的资源使用情况，包含下面信息。

* diskThreshold
* diskCopysetMinRequire
* diskUsed
* memoryThreshold
* memoryCopySetMinRequire
* memoryUsed

```
double GetResourceUseRatioPercent() {
    if (memoryCopySetMinRequireByte_ == 0) {
        if (diskThresholdByte_ != 0) {
            return 100.0 * diskUsedByte_ / diskThresholdByte_;
        } else {
            return 0;
        }
    } else {
        if (memoryThresholdByte_ != 0) {
            return 100.0 * memoryUsedByte_ / memoryThresholdByte_;
        } else {
            return 0;
        }
    }
}
```

# 3、下一步工作

## 3.1 copyset内部的元数据的拆分

当前的 curvefs 的实现方式，决定了可能系统中会出现巨无霸 inode，在随机写场景下，文件碎片化非常严重，加上现在的元数据的 compact 并不能做到及时清理垃圾碎片，inode 的大小会长到上百 MB 甚至上 GB。当前的调度算法，无法应对下面这样一种场景。

某 partition 一开始的上面的 inode 都比较小，partition 上创建了比较多 inode，这些 inode 慢慢的成长为巨无霸 inode，导致 partition 也变成了巨无霸 partition。如果 copyset 上的 partition 都是巨无霸，可能导致某个 copyset 即使独占一个的 metaserver，也还是存放不下这么多元数据。这就需要实现 copyset 拆分的功能，比如 copyset 原来管理了10个 partition，把10个 partition 拆成5 + 5个 partition，旧 copyset 和新 copyset 各管理5个 partition。同时 parition，也需要拆分功能，比如原来 partition 管理的是 fs1 上面 inode 范围为[1, 10000]的 inode 和 partition，现在把 partition 拆成两个，旧的 partition 管理 fs1 上范围为[1, 5000]的 inode 和 partition，新的 partition 管理 fs1 上范围为[5001, 10000]的 inode 和 partition。

## 3.2 更加精细的均衡调度

调度时，可以把元数据的热点问题考虑进来，让访问热点尽可能的分散到不同的 metaserver。
