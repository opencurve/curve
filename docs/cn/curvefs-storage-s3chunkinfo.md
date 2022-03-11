
# 背景

`Inode` 中 `S3ChunkInfo` 过大，全量更新 `Inode` 比较耗时，
所以我们先前优化采用增量更新的方式，并且在 `InodeStorage` 中保存 `Inode` 的指针以便更新 `Inode`。
由于改用 `RocksDB` 存储元数据后，数据只能序列化后以字符串的形式保存，所以无法保存 `Inode` 的指针。
导致更新 `Inode` 需要先读，修改后，再重新写入，导致超时。

# 增量更新

增量更新模式下，`UpdateInode` 更新 `Inode` 中除 `S3ChunkInfo` 外的其他字段，
更新 `S3ChunkInfo` 是走 `GetOrModifyS3ChunkInfo` 接口：

```
message GetOrModifyS3ChunkInfoRequest {
    ...
    map<uint64, S3ChunkInfoList> s3ChunkInfoAdd = 6;
    ...
}
```

#### 现有 GetOrModifyS3ChunkInfo 接口：
```
std::shared_ptr<Inode> old;
inodeStorage_->Get(..., &old);

for (auto &item : s3ChunkInfoAdd) {
    auto it = old->mutable_s3chunkinfomap()->find(item.first);
    old->mutable_s3chunkinfomap()->insert(
        {item.first, item.second});
}
```

#### 使用 RocksDB 存储后接口：
```
Inode old;
inodeStorage_->Get(..., &old);  // inode 非常大，并有反序列化，操作耗时

for (auto &item : s3ChunkInfoAdd) {
    auto it = old->mutable_s3chunkinfomap()->find(item.first);
    old->mutable_s3chunkinfomap()->insert(
        {item.first, item.second});
}

inodeStorage_->UpdateInode(inode) // inode 非常大，并有序列化，操作耗时
```

# 优化

## 更改 1：存储方式的更改

将 `Inode` 中的 `S3ChunkInfo` 与其他字段分为 2 类 `Key` 来存储：

* Inode
  * Key: `INODE:fsId:inodeId`
  * Value: Inode 序列化的字符串，其中的 S3ChunkInfo 为空
* S3ChunkInfoList:
  * Key: `S3_CHUNK_INFO_LIST:fsId:inodeId:chunkIndex:firstChunkId:lastChunkId`
  * Value: chunkIndex 对应的 S3ChunkInfoList 序列化后的字符串 (一个 chunkIndex 对应 64MB)

所以拆分后，`Inode` 由一个 `INODE` Key 和多个 `S3_CHUNK_INFO_LIST` Key 组成

## 更改 2：修改 S3ChunkInfo 方式的更改

每次更新 `S3ChunkInfo` 只需要插入一个的 Key/Value 即可，无需修改原先的 S3ChunkInfo:


**例如：**

```
// chunkIndex-1 有 3 次修改
S3_CHUNK_INFO_LIST:fsId:inodeId:1:1:3
S3_CHUNK_INFO_LIST:fsId:inodeId:1:4:6
S3_CHUNK_INFO_LIST:fsId:inodeId:1:7:9

// chunkIndex-2 有 2 次修改
S3_CHUNK_INFO_LIST:fsId:inodeId:2:10:20
S3_CHUNK_INFO_LIST:fsId:inodeId:2:30:40
```

## 更改 3: 传输方式的更改

原先 `Client` 获取 `S3ChunkInfo` 是一次性获取，当 `S3ChunkInfo` 过大时，会导致 RPC 超时，
现在采用 Stream 流式传输的方式，可以保证不管 `S3ChunkInfo` 有多大，都不会超时（只有出现网络问题，
2 个数据包接收的间隔超过某一阀值时，才会出现超时）。

备注：当我们需要获取 `Inode` 的全部 `S3ChunkInfo` 时，就需要以 `S3_CHUNK_INFO_LIST:fsId:inodeId:`
作为前缀搜索来遍历所有的 `S3ChunkInfoList`，RocksDB 有前缀索引优化，可以快速定位具有相同前缀的 Key，
所以是不存在性能问题的。

## 更改 4：压缩逻辑的修改

由于我们存储方式和传输方式都做了修改，所以涉及到的压缩逻辑也要做一定的修改

# 总结:

将 S3ChunkInfo 拆开存储，使 `UpdateInode` 和  `GetOrModifyS3ChunkInfo` 接口尽量轻量化
