# rename 接口实现优化

## 背景

在目前 CurveFS 的 rename 实现中，我们保证了该操作的原子性，但是仍然存在以下 2 个问题：

* 在多挂载的场景下，我们为了保证 txid 的正确性，在 MDS 中加了一把分布式锁来保证所有的事务都是串行执行，这严重影响了 rename 接口的性能，即事务不能并发执行
* 同样地，为了保证其他操作接口（如 GetDentry、DeleteDentry 等）能拿到正确版本的 dentry，在这些接口执行前都要去 MDS 获取最新的 txid，对于这些接口来说，多了一次 RPC 请求，降低了整体的元数据性能

针对以上 2 个问题，调研了 Google Percolator 事务模型以及现有的开源实现，并发现其能很好解决我们以上 2 个已知问题。

## 方案调研

### Google Percolator

资料：

[Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Peng.pdf)

[Google Percolator 的事务模型](https://github.com/ngaut/builddatabase/blob/master/percolator/README.md)

### TiKV 中 percolator 事务实现优化

资料：

[Optimized Percolator](https://tikv.org/deep-dive/distributed-transaction/optimized-percolator/)

## 方案实现

我们先阐述以原论文、没有任何优化的情况下如何在我们 CurveFS 中实现 pecolator 事务，然后再逐一将每一个优化点加入其中。

### 基础实现

基于我们之前的 rename 实现，我们可以比较好理解 percolator 的实现，percolator 主要对每一个 key 都有一把锁来解决写写冲突。percolator 论文中的 data 就是跟我们之前写入 txid 版本的 dentry 是一样的，而往 write 列写入时间戳则跟我们之前往 MDS 提交 txid 是一样的，也是提交版本号。

#### 整体实现

percalator 也是一个 2PC 的实现方案，分为 Prewrite 和 Commit 阶段：

* 客户端首先会通过 Tso() 接口从 MDS 获取全局递增的时间戳，作为当前数据的版本号（start_ts），然后对所有涉及到的 key 进行 prewrite 操作，prewrite 包括对 key（会从所有的 key 中选出一个主 key） 加锁已经写入对应版本的数据。这一步与我们目前实现中的 prepare 是一样的，准备最终版本的数据
* 当 prewrite 成功后，客户端会再次通过 Tso() 接口从 MDS 获取时间戳，作为提交时间戳 (commit_ts)，然后首先对主键进行提交版本号，并进行解锁。如果主键成功后，再并行地对所有从键进行解锁。

异常处理：

percolator 中的异常处理是由下一个事务完成的：

* 如果发现当前操作的 key 返回  WriteConflict 则表示写冲突了，需要重新 retry
* 而如果发现 key 被锁住，即 KeyIsLocked 状态，客户端需要调用 CheckTxnStatus 接口来获取上一个事务的状态，如果已经提交，则推动从键提交，如果是锁已经操作，则推动所有键 rollback，而如果前一个事务正在进行中，则需要等待。

#### 数据结构

考虑到性能，我们要保证 lock 和 write 这 2 张表永远保存在内存当中，因为这 2 张表中的内容比较小但是读写却很频繁。对于 RocksDB 来说，我们要给这 2 张表单独配置 column failmy，保证其独立性。

| table                 |   key   | value     | 说明 |
|-----------------|---------|--------------|:----------|
| data	| key:timestamp	| |	实际的数据，这里保存多版本数据 |
| lock |	key	| struct lock { primary、timestamp、ttl } | 保护当前 key 的锁 |
| write	| key:timestamp	 | struct write { timestamp、kind } | 记录已提交的版本号 |

**struct lock**

```
struct Lock {
  std::string primary;  // 锁的主键相关信息
  uint64_t timestamp;   // 加锁的时间戳
  uint64_t ttl;         // 锁的 TTL。我们通过这个来判断锁有没有超时
}
```

**struct write**

```
struct Write {
  uint64_t timestamp;  // 最新版本数据的时间戳。我们通过这个时间戳可以在 data 表中找到相应版本的数据
  char kind;           // 写入的类型，详见 WriteKind
}
```

**事务状态码**

```
enum class TxStatus {
  WriteConflict,  // 事务写写冲突。为解决该冲突，我们的处理原则是只保证一个事务成功
  KeyIsLocked,    // 当前事务涉及的键已被锁住。
  OK,             // 成功
};
```

**写入类型**

```
enum class WriteKind {
  Put,
  Delete,
  Rollback,
};
```

以上的数据结构中，各数据结构的主要目的如下：

* timestamp：时间戳主要为了确定事务的顺序，为事务冲突提供判断依据。另一个作用是作为锁 TTL 的开始时间戳
* Lock：主要是为了保证对某一个 key 来说，只能有一个写操作，以此来解决写写冲突
* Lock.ttl：锁的 TTL 主要用来解决当客户端挂掉或 hang 住，锁仍然遗留在相应的 key 上而导致其他事务无法执行，或
* Lock.primary：事务/锁的主键。在我们的实现中，我们可能涉及到多个 dentry，我们会选任意一个
* Write：这个作用跟我们当前事务实现中的 txId 一样，用来保存当前 dentry 的版本号，对外提供统一的视图，通过版本号找到最新的 dentry。而在 Write 表中写入 timeStamp 就跟我们现在实现的往 MDS 提交 txId 一样。

#### 相关函数

**一些约定**

* 对于每一个函数，我们会先阐述正常的处理流程，之后会阐述每个步骤出现异常、不符合预期时的异常处理流程
* 对于 dentry 来说，我们以 dentry_key 作为实际存储 dentry 的键，这个键的编码格式如下：fsId:parentInodeId:name

**Tso**

客户端开启事务后，需要通过 Tso() 接口向 MDS 端获取一个全局递增的时间戳（该时间戳的作用类似于我们目前实现中的 txid）用于 prewrite，而在提交事务时同样需要通过 Tso() 接口获取一个时间戳用于 commit。

时间戳是一个 64 位整数，其由物理时间和逻辑时间两部分组成合成，其编码规则如下：

`TSO = 物理时间 + 序列号`

**Prewrite**

```
struct PrewriteRequest {
  std::string primaryKey;   // 事务/锁的主键
  uint64_t startTimestamp;  // 开始时间戳。该时间戳从调用 Tso 接口从 MDS 端获取
  uint64_t lockTTL;         // 锁的 TTL。用来判断当前 key 的锁是否超时
  ...                       // 涉及修改的 dentry 相关信息
};

struct PrewriteResponse {
  Status status;
};
```

客户端通过 Tso() 接口获取到时间戳后，会调用 Prewrite() 接口依次对事务到涉及到所有 dentry 依次做 prewrite，MetaServer 端在接收到该请求后，会做如下处理：

* 获取当前请求的 dentry 在 write 表中最近一次写入的信息，如果该 dentry 最新一次写入的 Write.timestamp >= PrewriteRequest.startTimestamp，则表明在该事务开始后有其他事务修改了这个 key，返回 TXStatus::WriteConfict;
* 检查当前请求的 dentry 在 lock 表中是否有锁，如果有锁，则返回 status::KeyIsLocked;
* 若前面检查都成功，则调用 RocksDB 的事务接口，执行以下 2 个操作：
    * 在 lock 表中对该 key 进行加锁。key="dentry", value=Lock{}
    * 在 data 表中写入当前版本的 dentry。key="dentry:PrewriteRequest.startTimeStamp", value=mut_dentry
* 若以上事务成功，则返回 status::OK, 否则返回 status::Error

注意：在客户端对所有涉及的 dentry 依次加锁的过程中，只要有一个 dentry 不是返回 status::OK，则表明 Prewrite 阶段失败。而当其中一个 dentry 收到 status::KeyIsLocked 的相应时，客户端需要调用 CheckTxnStatus() 接口来判断当前锁的状态，并根据锁的状态来执行相关的操作。而关于锁的状态，我们可以分

**Commit**

```
struct CommitRequest {
  uint64_t startTimestamp;   // 该事务开始时间戳
  uint64_t commitTimestamp;  // 该事务提交时间戳

};

struct CommitResponse {

};
```

客户端在 prewrite 阶段成功后，就会进入提交事务阶段。客户端首先通过 Tso() 接口获取一个时间戳作为事务提交时间戳 commit_ts，再去该事务主键所在 MetaServer 提交事务，如果主键提交事务成功，则代表整个事务已经成功（这个步骤类似于我们现在的 CommitTxId），之后会并行去所有次键提交事务。MetaServer 在接收到该请求后，会做如下处理：

* 以 dentry_key 为键在 lock 表中查找当前 dentry 是都有锁，如果无锁则代表当前 dentry 已被提交，直接返回 Status::OK
* 如果有锁，则判断上锁的时间戳是否等于事务提交的时间戳，即 Lock.timestamp == CommitRequest.startTimestamp
* 若前面检查都成功，则调用 RocksDB 的事务接口，执行以下 2 个操作：
    * 在 write 表中写入最新版本的 dentry 的时间戳，即更新该 dentry 的版本号。KEY=dentry_key:CommitRequest.commitTimestamp，VALUE=Write{ timestamp=CommitRequest.startTimestamp }。这一步跟我们目前的 CommitTxId 是一样的，主要为了更新版本号。
    * 在 lock 表中删除以 dentry_key 为键的锁
* 失败见以下

Commit 接口用于提交事务，客户端会先对事务中的主键进行提交，成功后并发对所有次键进行提交。如果提交事务失败，客户端会通过 CheckTxnStatus 接口获取锁的状态，再进行下一步动作。

**Rollback**

```
struct RollbackRequest {
  uint64_t StartTimestamp;
};

struct RollbackResponse {
  TXStatus Status;
};
```

* 如果当前 key 的 write 列已经写入 Rollback 的数据，则表明已经 rollback 过了，直接跳过
* 而 write 列中如果不是 rollback，则直接 abort 当前事务
* 若前面检查都成功，则调用 RocksDB 的事务接口，执行以下 3 个操作：
    * 在 lock 表中删除以 dentry.key 为键的锁
    * 在 data 表中删除之前 prewrite 写入以 dentry.key+RollbackRequest.startTimestamp 为键的数据
    * 在 write 表中写入以下键值对
        * KEY: dentry.key + RollbackRequest.startTimestamp
        * VALUE: Write{ timestamp=RollbackRequest.startTimestamp, kind=WriteKind::Rollback }

**CheckTxnStatus**

```
struct CheckTxnStatusRequest {
  std::string primaryKey;
  uint64_t lockTimestamp;
  uint64_t currentTimestamp;
};

struct CheckTxnStatusResponse {
};
```

当客户端在执行某事务时，发现与另一事务（我们称之为前事务）冲突时，即 prewrite 阶段接收到 **TxStatus::LockIsKey** 响应时，客户端需要去前事务的主键所在 MetaServer 获取前事务的状态，并根据前事务的状态来做相应的操作：

* roll-forward：表明前事务已提交，我们也需要推动这个冲突的 key 去提交
* roll-back：表明前事务失败、rollback、超时或 hang 住，我们需要推动这个冲突的 key 去回归
* wait: 表明前事务正在进行中，我们可等待一段时间后再进行 retry。**特别需要注意**的是，重新 retry 开启的事务都需要重新通过 Tso() 获取时间戳并重头开始

我们用一个 { primary, start_ts, commit_ts } 三元组来描述一个事务，以下的表格是 primary key 在事务中所有状态的列举，我们正是根据这个状态来得知事务处于什么状态：

| data@start_ts  | lock? | lock is TTL? | write@start_ts | write@commit_ts | 说明 | 客户端需执行动作 |
|-----------------|---------|------|-------|-------|:----------|-------|
| | | | | timestamp = start_ts | 这个状态说明该事务已被成功提交 | roll-forward |
| | | | kind = rollback | | 这个状态说明事务已被 rollback | roll-back |
| | | | | |这个状态说明在此次事务中，该 primary key 没有被成功 prewrite | roll-back |
| <新版本数据> | Y | Y | | | 这个状态说明事务已超时。有可能是客户端挂了或 hang 住了	 | roll-back |
| <新版本数据> | Y | N | | | 这个状态说明 primary key 已被成功 prewrite，事务正在进行当中 | wait |

 之所以我们能根据主键的各个表中的值就能判断事务的状态，主要是因为我们操作的时候以锁为主要元素，不管是 prewrite、commit、rollback 都是原子操作，而且事务提交的成功是以主键是否成功提交为依据，所以该主键的状态就在由以上 5 个状态组成的状态机中转变。

<补充状态机转换图>

**ResolveLock**

```
struct ResolveLockRequest {
  uint64_t commitTimestamp;
};

struct CheckTxnStatusResponse {
};
```

RevolveLock 接口比较简单，主要是根据当前的请求中是否有 commitTimestamp 字段来决定是 commit 事务，还是 rollback 事务。这个函数是一个接口的聚合。

**Get**

Get 接口主要用来描述 GetDentry/DeleteDentry 等非 rename 事务怎么获取当前 dentry。如果当前键没有锁，则直接获取最近 write 列对应版本号的数据，如果有所，则通过 CheckTxnStatus 获取前一个事务的状态，并推动事务 commit 还是 rollback 后再进行 Get 等操作。

#### FAQ

1、如果事务 A prewrite 成功，但是此时事务 B 发现与事务 A 存在冲突，将 A 的主键解锁了。这时候事务 A 再去提交事务，能否成功？
<这种情况是否会出现？>

### 优化点 1：并发 Prewrite

论文中的 prewrite 是序列进行的，而我们可以推动所有的键并发进行，提高性能

## 优化点  2：去除每次读取操作获取时间戳的流程

论文中的每一个获取也需要获取一个  Tso，这对于我们的实现来说会增加一个 RPC 时间，而其实这个 Tso 是多余的，我们只需要在无锁获取当前 key 的最新数据即可，而有锁的话则需要推动前一个事务。

```
Point Read Without Timestamp
Timestamps are critical to providing isolation for transactions. For every transaction,
we allocate a unique start_ts for it, and ensures transaction T can only see
the data committed before T’s start_ts.
But if transaction T does nothing but reads a single key, is it really necessary to allocate it a start_ts?
The answer is no. We can simply read the newest version directly,
because it’s equivalent to reading with start_ts which is exactly the instant when the key is read.
 It’s even ok to read a locked key, because it’s equivalent to reading with
the start_ts allocated before the lock’s start_ts.
```
