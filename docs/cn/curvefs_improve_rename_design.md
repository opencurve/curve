# Rename 优化方案

## 背景描述

在目前 CurveFS 的 rename 实现中，我们保证了该操作的原子性，但是仍然存在以下 2 个问题：

1. 在多挂载的场景下，我们为了保证 txid 的正确性，在 MDS 中加了一把分布式锁来保证同一 FS 上所有的事务都是串行执行，这严重影响了 rename 接口的性能，即事务不能并发执行。
2. 同样地，为了保证其他操作接口（如 GetDentry、DeleteDentry 等）能拿到正确版本的 dentry，在这些接口执行前都要去 MDS 获取最新的 txid，对于这些接口来说，多了一次 RPC 请求，降低了整体的元数据性能。

针对以上 2 个问题，调研了 [Google Percolator](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Peng.pdf) 事务模型以及现有的开源实现（[TiKV中 Percolator的优化](https://tikv.org/deep-dive/distributed-transaction/optimized-percolator/)），并发现其能很好解决我们以上 2 个已知问题。

## 方案实现

### 整体实现

percalator 也是一个 2PC 的实现方案，分为 Prewrite 和 Commit 阶段。基于我们之前的 rename 实现，可以比较好理解 percolator 的实现，它主要对每一个 key 都有一把锁来解决写写冲突。percolator 论文中的 data 就是跟我们之前写入带 txid 版本的 dentry 是一样的，而往 write 列写入时间戳则类似我们之前往 MDS 提交 txid，也是提交版本号。

#### 数据结构

在介绍整体流程之前，先约定必要的数据结构，percolator 事务模型中涉及到的三个表：data、lock、write，lock 和 write表内容比较小，但访问频繁，在 Rocksdb 中为其单独配置 column family，保证其独立性。

| Table name | Key                         | Value                                               | 说明                             |
| ---------- | --------------------------- | --------------------------------------------------- | -------------------------------- |
| data       | dentryKey                   | struct DentryVec {dentrys}                          | 实际的dentry数据，保存多版本数据 |
| lock       | dentryKey                   | struct TxLock {primaryKey, startTs, timestamp, ttl} | 包含当前key的锁                  |
| write      | dentryKey/commitTs或startTs | struct TxWrite {startTs, writeKind}                 | 记录已提交或已回滚的版本号       |

```protobuf
message DentryVec {
    repeated Dentry dentrys = 1;  // 记录多版本dentry信息，版本信息复用之前的txid字段
}

message TxLock {
    required string primaryKey = 1;  // 事务的主键，rename中涉及两个dentry，默认以src dentry为primaryKey
    required uint64 startTs = 2;     // 事务开始序列号
    required uint64 timestamp = 3;   // 事务开始物理时间
    optional uint32 index = 4;
    optional int32 ttl = 5;
}

enum TxWriteKind {
    Commit = 1;
    Rollback = 2;
}

message TxWrite {
    required uint64 startTs = 1;
    required TxWriteKind kind = 2;
}
```

data 表的清理：复用现有逻辑，在insert、delete 等 dentry 操作时会进行压缩操作，正常情况下最多存在两个版本的 dentry 数据。

lock 表的清理：每个 key 最多只会有一条记录，事务提交或回滚都会清理对应记录。

write 表的清理：key 的每次提交或回滚都会在 write 表中记录一条记录，如果不清理则会越来越多，清理规则是对应 writeKind==TxWriteKind::Commit的记录每个 key 只会保留最新的一条记录，对应 writeKind==TxWriteKind::Rollback 的记录都保留，便于后续判断事务状态，且发生回滚的事务本身极少，不会对容量造成负担。

#### 整体流程

1. 客户端首先会通过 Tso() 接口从 MDS 获取全局递增的事务序列号 startTs 和物理时间戳 timestamp。startTs 作为当前数据的版本号，timestamp用于验证 lock 的超时。
2. 对涉及到的 dentry 进行 prewrite 操作，会任意选择一个 key 作为 primaryKey（默认选择 src dentry），先 prewrite primaryKey 再 prewrite 其他 key，原因见 write 表清理逻辑。
3. 所有 key prewrite 完成后，客户端会再次通过 Tso() 接口从 MDS 获取 commitTs。
4. 对 primaryKey 进行 commit 操作，成功后，对其他 key 并行 commit。

异常处理：percolator 中的异常处理是延迟到下一个事务解决的

1. prewrite 时从 write 表中获取该 key 最新的 ts，如果 ts >= startTs 则说明期间已有新的事务发生，返回 WriteConflict 错误。
2. prewrite 时发现该 key 已经被 lock 住了，则返回 TxkeyLocked，客户端需要调用 CheckTxStatus 接口去主键获取上一个事务的状态，如果已经提交，则推动从键提交，如已回滚或锁已经超时，则推动所有键 rollback，而如果前一个事务正在进行中，则需要等待。

#### 相关函数说明

##### Tso

客户端开启事务后，需要通过 Tso 接口向 MDS 端获取一个全局递增的事务序列号 sn 和 一个物理时间 timestamp，sn 标识一次事务，同时表示该数据的版本，timestamp 记录于 lock 表中的 TxLock 中，便于在 client 异常时导致当前事务挂起，其他事务在 CheckTxStatus 时判断超时。

```protobuf
message TsoRequest {}

message TsoResponse {
    required FSStatusCode statusCode = 1;
    optional uint64 sn = 2;
    optional uint64 timestamp = 3;
}
```

##### PrewriteRenameTx

首先 prewrite primaryKey，成功后再 prewrite 其他 key，如果rename 涉及的 dentry 位于同一个 partition 上，则可以合并成一次请求。metaserver 收到该请求进行如下处理：

1. 在 write 表中获取该 dentryKey 最新的一次写入的 ts，如果 ts >= PrewriteRenameTxRequest.txLock.startts() 则返回错误码 MetaStatusCode::TX_WRITE_CONFLICT。
2. 在 lock 表中检查该 dentryKey 是否存在，如果存在，则返回错误码MetaStatusCode::TX_KEY_LOCKED。
3. 如果以上检查都通过，则通过 Rocksdb 事务接口完成如下两个操作：
   1. 在 lock 表中插入记录： key=dentryKey, value=TxLock
   2. 在 data 表中写入当前版本的 dentry

```protobuf
message PrewriteRenameTxRequest {
    required uint32 poolId = 1;
    required uint32 copysetId = 2;
    required uint32 partitionId = 3;
    repeated Dentry dentrys = 4;
    required TxLock txLock = 5;
}

message PrewriteRenameTxResponse {
    required MetaStatusCode statusCode = 1;
    repeated Dentry dentrys = 2;
    optional TxLock txLock = 3;  // 如果返回 MetaStatusCode::TX_KEY_LOCKED，则该字段为前一个事务的 txlock 信息
    optional uint64 appliedIndex = 4;
}
```

##### CheckTxStatus

在 prewrite 过程如果返回 MetaStatusCode::TX_KEY_LOCKED，则表示事务冲突，客户端需要其检查前一个事务的状态，依据状态再去处理 locked key。metaserver 收到该请求进行如下处理，通过 Rocksdb 事务接口完成：

1. 在 lock 表中获取 primaryKey 对应的 TxLock。如果存在，如果 CheckTxStatusRequest.curTimestamp > txLock.startTs + txLock.ttl，则表明事务已超时，返回错误码 MetaStatusCode::TX_TIMEOUT，否则说明事务正在进行中，返回错误码 MetaStatusCode::TX_TX_INPROGRESS。
2. 如果对应 TxLock 不存在，则在 write 表中用 key=CheckTxStatusRequest.startTs 进行查找，如果找到，并且 TxWrite.kind=TxWriteKind::rollback 则说明该事务已经回滚，返回错误码 MetaStatusCode::TX_ROLLBACKED；如果没找到，则说明事务已提交，返回错误码 MetaStatusCode::TX_COMMITTED。

```protobuf
message CheckTxStatusRequest {
    required uint32 poolId = 1;
    required uint32 copysetId = 2;
    required uint32 partitionId = 3;
    required string primaryKey = 4;
    required uint64 startTs = 5;
    required uint64 curTimestamp = 6;
}

message CheckTxStatusResponse {
    required MetaStatusCode statusCode = 1;
    optional uint64 appliedIndex = 2;
}
```

##### ResolveTxLock

CheckTxStatus 完成后，需要根据返回的事务状态进行处理：

MetaStatusCode::TX_COMMITTED：rollforward

MetaStatusCode::TX_TIMEOUT; MetaStatusCode::TX_ROLLBACKED : rollback

MetaStatusCode::TX_TX_INPROGRESS: retry

metaserver 在收到该请求时进行如下处理，通过 Rocksdb 事务接口完成：

1. 在 lock 表中查找该 key 是否仍然处于 locked 的状态，如果不存在，则直接返回成功；如果存在，判断 TxLock.startTs == ResolveTxLockRequest.startTs，如果不成立，则返回 MetaStatusCode::TX_MISMATCH。
2. 如上对 lock 的检查通过后，如果 ResolveTxLockRequest.commitTs > 0，表示需要 rollforward：删除该 key 的 lock 记录；在 write 表中插入该版本的 dentry。如果 ResolveTxLockRequest.commitTs==0，表示需要 rollback：删除该 key 的 lock 记录；在 data 表中删除对应版本的 dentry； 在 write 表中写入回退的记录，key=dentryKey/startTs， value=TxWrite{startTs, TxWriteKind::Rollback}。

```protobuf
message ResolveTxLockRequest {
    required uint32 poolId = 1;
    required uint32 copysetId = 2;
    required uint32 partitionId = 3;
    required Dentry dentry = 4;
    required uint64 startTs = 5;  // 待处理事务标识
    required uint64 commitTs = 6; // commitTs > 0 表示rollward, 否则 rollback
}

message ResolveTxLockResponse {
    required MetaStatusCode statusCode = 1;
    optional uint64 appliedIndex = 2;
}
```

##### CommitTx

在没有异常情况下，prewrite 完成后会进行 commit 操作，先进行 primaryKey 的 commit，再同时 commit 其他key，只要 promaryKey commit 成功就代表了该事务成功。如果rename 涉及的 dentry 位于同一个 partition 上，则可以合并成一次请求，metaserver 收到该请求会进行如下处理：

1. 在 lock 表中检查该 key 是否存在，如果不存在，则表示该事务已结束（commit 或 rollback），直接返回。
2. 判断 txLock.startTs == CommitTxRequest.startTs ，如果不等则返回错误码 MetaStatusCode::TX_MISMATCH。
3. 在 write 表中更新 dentry 版本，key=dentryKey/commitTs，value=TxWrite{startTs, TxWriteKind::Commit}；删除 lock 表中对应 key 的记录。

```protobuf
message CommitTxRequest {
    required uint32 poolId = 1;
    required uint32 copysetId = 2;
    required uint32 partitionId = 3;
    repeated Dentry dentrys = 4;
    required uint64 startTs = 5;
    required uint64 commitTs = 6;
}

message CommitTxResponse {
    required MetaStatusCode statusCode = 1;
    optional uint64 appliedIndex = 2;
}
```

##### 其他 dentry 操作

CreateDentry、GetDentry、ListDentry、DeleteDentry 等非 rename 事务操作，在操作 dentry 时，如果对应 key 没有被 lock，则直接操作 write 表该 key 最新提交版本在 data 表中的数据即可；如果有锁，则需要通过 CheckTxStatus 来获取事务的状态，并进一步推动事务的完成，之后再处理对应 dentry。所以在这几种操作的 response 中增加 optional TxLock 字段，用于在有锁情况下，返回事务对应的 lock 信息。
