简述
---

目前 CurveFS 全部元数据都缓存在内存中，为降低硬件使用成本，我们将提供 RocksDB 作为我们的持久化存储。
接下来将会阐述为什么我们会选择 RocksDB 作为我们的存储引擎，以及一些实现细节。

为什么选择 RocksDB?
---

目前存储引擎主要分为以下这几类，这些存储引擎都拥有广泛的应用以及非常棒的开源实现，我们要做的是根据我们的应用场景选择最适合的存储引擎：

| 数据结构                  | 存储引擎           | 简介                                                                                                                                                                         | 是否有序 | 写入性能                                                              | 读取性能                  | 适用场景                       |
| :---                      | :---               | :---                                                                                                                                                                         | :---     | :---                                                                  | :---                      | :---                           |
| Log-Structured Hash Table | [Bitcask][bitcask] | 内存中是 Hash 表结构，Hash 结构维护 key 到文件存储的 position，数据仅支持追加操作，所有的操作都是 append 增加而不更改老数据                                                  | N        | 内存写 + 顺序写                                                       | 内存读 + 随机读           | 适合简单的 kv 存储，如缓存     |
| Log-Structured Merge-Tree | [RocksDB][rocksdb] | 内存中维护有序的 Memtable，当 Memtable 到达一定大小后变成只读并保存成有序的 SST 文件。多个有序的 SST 文件会通过多路归并进行合并，保证读取的性能以及删除重复的数据            | Y        | WAL 顺序写 + 内存写                                                   | 内存读 + 可能多次的随机读 | 适合写多于读，高并发写入的场景 |
| B+ Tree                   | [BoltDB][boltdb]   | 内存中是有序的 B+ Tree，这种数据结构又矮又胖，查询和更新的路径都只需要历经极少的节点即可。而一个节点是一个 page 大小，数据库文件通过 mmap 映射到内存中，所以读写性能都有保障 | Y        | 读写比较均衡，除去 value 外，往往只需要写入几个 (如 2-4) 个 page 即可 | 同上                      | 适合读多于写，大量查询的场景   |

最终我们选择 RocksDB，主要基于以下几个考虑点：

* 我们需要数据是有序的，因为我们有一些范围查找的操作，比如 `ListDentry`。
* 我们在 Fuse Client 拥有元数据缓存，所以整体来说到达 MetaServer 的请求是写多于读的。
* 对于 `ListDentry` 这类范围查找的读请求来说，RocksDB 有专门建立索引来优化，而不必去扫描所有 SST 文件。这类读请求性能是和 BoltDB 相当的。
* RocksDB 的社区比较活跃以及使用广泛，有很多基于 RocksDB 的开源 KV，对后续优化改进会比较有帮助。

实现
---

* 保留原有的全内存存储方式

MetaServer 在启动时可指定对应的存储引擎，包括全内存或 RocksDB

* 同一 MetaServer 中的 Copyset 共用一个 RocksDB 实例

考虑到顺序写性能远大于随机写性能，我们将一个 MetaServer 绑定一个 RocksDB 实例（对应一块磁盘），这样可以保证只有顺序写以及拥有写入合并带来的性能提升。

* key-value 分离存储

由于我们的 value 远大于我们的 key，我们将使用 RocksDB 社区提供的 BolbDB 插件。SST 中只保存 key 以及 value 的位置，而 value 则以 append 的形式保存在单独的文件中。这样 SST 文件在合并时只需要合并 key，可以有效减少写放大。

* 关闭 RocksDB 写入 WAL

由于我们 Raft 已经有 WAL 来保证数据的安全，我们将关闭 RocksDB 的 WAL，这样一来，每个写请求就只有一次内存操作：

```
WriteOptions.disableWAL = true
```

* 关于快照

> 打快照

RocksDB 由于有 MVCC 解决读写冲突，每次写入的 key 编码后都拥有全局递增的序列号，打快照时只需获取当前序列号遍历即可（有对应 snapshot 接口）， 无锁也不需要 fork 子进程，并直接写成 SST 文件。

> 加载快照

RocksDB 支持直接增加 SST 文件列表，无需遍历文件中的数据。由于我们多个 Copyset 共用一个 RocksDB 实例，
为保证数据被安全删除，我们需要调用 DeleteRange 接口来删除当前 Copyset 中所有 Partition 范围的数据。

* 关于内存以及磁盘容量限制

RocksDB 提供了一些选项，我们可以比较安全的限制住内存大小及磁盘使用总容量。

* 关于 RocksDB 调优参数

RocksDB 拥有非常多的参数，我们需要根据我们实际的业务场景来选择不同的参数选项。官方 wiki 给出了非常详细的解释以及说明，我们需要在实现以及测试中进行调整。为了方便调整及测试，我们需要实现将 RocksDB 的参数写在我们的 MetaServer 配置中即可生效的方式，无需改动代码。

参考
---

* [《Designing Data-Intensive Application》DDIA中文翻译](https://github.com/Vonng/ddia)
* [浅谈存储引擎](https://zhuanlan.zhihu.com/p/51910237)
* [TiKV 与 RocksDB](https://docs.pingcap.com/zh/tidb/stable/rocksdb-overview)
* [RocksDB wiki][rocksdb-wiki]
* [[FAST 2016] Wisckey-Separating Keys from Values](https://zhuanlan.zhihu.com/p/45792050)
* [Mysql索引结构用B+树-好在哪？](https://zhuanlan.zhihu.com/p/84493668)
* [浅析存储引擎(4)-对比B-tree和LSM-tree](https://blog.csdn.net/daiyudong2020/article/details/104721566)
* [作为一个K-V数据库，levelDB索引为什么要使用LSM树实现，而不采用哈希索引？](https://www.zhihu.com/question/27256968)
* [leveldb-handbook](https://leveldb-handbook.readthedocs.io/zh/latest/basic.html)
* [RocksDB 笔记](http://alexstocks.github.io/html/rocksdb.html)
* [RocksDB 写入流程详解](https://zhuanlan.zhihu.com/p/33389807)
* [Rocksdb 的 BlobDB key-value 分离存储插件](https://blog.csdn.net/Z_Stand/article/details/117621418)
* [RocksDB-06-快照和迭代器](https://zhuanlan.zhihu.com/p/443005768)
* [Rocksdb参数总结](https://www.shangmayuan.com/a/7268e2e83e534101ac79c864.html)
* [TiKV 性能参数调优](https://docs.pingcap.com/zh/tidb/v3.0/tune-tikv-performance)
* [关于Tikv Raft snapshot apply的疑问](https://asktug.com/t/topic/2486)

[bitcask]: https://github.com/basho/bitcask
[rocksdb]: https://github.com/facebook/rocksdb
[boltdb]: https://github.com/etcd-io/bbolt
[rocksdb-wiki]: https://github.com/facebook/rocksdb/wiki
