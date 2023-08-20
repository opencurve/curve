# 背景
当前curvebs空间分配只考虑了集群剩余空间不足的情况，利用 `mds.topology.PoolUsagePercentLimit` 来限制是否继续分配chunk到物理Pool。

# 目标
- 解决单个节点容量不足情况下，不再分配新的chunk。

# 方案
基本思路: 当某一ChunkServer出现容量不足的情况，超过设定阈值后，对节点涉及到的CopySet集合不再分配新的Chunk。
拆解开来就是两件事:
- 需要在分配copySet时考虑容量不足节点涉及的CopySet不再分配。
- 设定什么情况下，节点容量算不足，并收集容量不足的节点信息，配置项命名为 `mds.topology.cs.DiskUsagePercentLimit`。
