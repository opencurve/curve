# CurveFS Improve ls -*

## 背景

在对文件系统的使用中，我们经常会使用到 **ls**  相关的命令，例如 **ls -l**，**ls -R** 等。不同命令所需要获取的信息不同，对应到底层文件系统的请求也就不同，下面列出常用的命令 *ls, ls -R , ls --color=auto, ls -l* 在用户态文件系统上请求：

```
不同命令对应到底层文件系统的请求
# ls dir
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> dir
FuseOpOpenDir               -> dir
FuseOpReadDir*n             -> dir
FuseOpReleaseDir            -> dir

# ls file
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> file

# ls -R dir
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> dir
FuseOpOpenDir               -> dir
FuseOpReadDir*n             -> dir
FuseOpGetAttr               -> dir
FuseOpLookup*subFileNum     -> subfiles
FuseOpReleaseDir            -> dir

# ls -R file
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> file

# ls --color=auto dir
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> dir
FuseOpOpenDir               -> dir
FuseOpReadDir*n             -> dir
FuseOpGetAttr               -> dir
FuseOpLookup*subFileNum     -> subfiles
FuseOpReleaseDir            -> dir

# ls --color=auto file
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> file

# ls -l dir
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> dir
FuseOpGetXattr*3            -> dir // security.selinux;system.posix_acl_access;system.posix_acl_default
FuseOpOpenDir               -> dir
FuseOpReadDir*n             -> dir
FuseOpGetAttr               -> dir
FuseOpLookup*subFileNum     -> subfiles
FuseOpGetXattr*subFileNum   -> subfiles // system.posix_acl_access
FuseOpReleaseDir            -> dir

# ls -l file
FuseOpGetAttr               -> parentDir
FuseOpLookup                -> file
FuseOpGetXattr*2            -> file // security.selinux;system.posix_acl_access
```

## 性能测试

假设一个目录下有大量子目录和文件时，我们需要对其进行 *ls -** 的操作时，可能会消耗较大时间，我们以一个目录下有10w个大小为1MB的文件为例，进行测试命令 **ls** 、 **ls -R** 和 **ls -l** 的性能。

| 文件系统类型 | ls | ls -R | ls -l |
| --- | --- | --- | --- |
| ext4 | 0.61s | 0.69s | 2.13s |
| cephfs | 1.2s | 21.8s | 42.0s |
| cephfs-kernel | 1.2s | 1.5s | 7s |
| cubefs | 1.s | 5.2s | 5.6s |
| fastcfs | 0.67s | 0.81s | 1.36s |
| curvefs | 0.72s | 24.8s | 29s |

## 分析与优化

分析得知耗时主要消耗在 **FuseOpLookup*subFileNum** 和 **FuseOpGetXattr*subFileNum** 上，耗时会随着文件数量的增加呈线性增加，特别是对于 CurvrFS 这样的分布式文件系统来说，网络消耗占到了绝大部分。为了优化其性能主要从以下几个方面出发：

- 目前 CurveFS 的 Inode 中保存了除属性信息外还有数据相关的 S3ChunkInfo ，目前的代码实现中在一些只需要 Inode 属性的请求（例如：lookup）中也同样获取了整个 Inode 返回给 client。
- 减少 FuseOpLookup 请求的次数，如果每个子文件都发送一次 RPC 获取属性信息，当文件数量较大时耗时将不可接受。
- 对应 FuseOpGetXattr 的请求，目前 CurveFS 只有一类 summary xattr，可考虑屏蔽不必要的 FuseOpGetXattr 请求。

1. 针对上面的第一点：可以优化所有只需要 Inode 属性的请求仅获取 InodeAttr，现在已有 *BatchGetInodeAttr* 请求接口去 MetaServer 获取 InodeAttr，可以复用该接口。
2. 针对上面的第二点：考虑在 ReadDir 时，在 ListDentry 之后通过批量获取目录下所有 InodeAttr，并进行缓存，当后续对每个文件的 lookup 请求时直接从缓存获取返回。缓存的地方可以选择在 Curve-fuse中自己维护，也可以考虑实现 fuse low-level 接口 **FuseOpReadDirPlus** （FUSE_VERSION >= 3.0），在 ReadDir 时顺便将属性信息返回缓存在 VFS 层，后续lookup 请求直接在 VFS 层返回（attr_timeout & entry_timeout is valid），修改采用后者。
3. 针对上面的第三点：目前 CurveFS 仅有一类 summary xattr，用于加速获取文件系统容量统计相关信息，开关默认打开（enableSumInDir），修改从两方面：
    1. 对于不需要获取文件系统容量统计相关信息的场景可直接屏蔽 FuseOpGetXattr 请求，在接口中直接返回 NOTSUPPORT ，这样后续对该文件系统的 FuseOpGetXattr 请求就不会下发到用户态文件系统上来。
    2. 如果需要获取文件系统容量统计相关信息，先判断获取的扩展属性的 name 如果属于 summary xattr 则处理，否则直接返回空信息。是否直接屏蔽 FuseOpGetXattr 请求，可通过配置项 fuseClient.disableXattr 进行配置（默认为false）。

## 优化后结果

优化代码见[pr1484](https://github.com/opencurve/curve/pull/1484)，测试场景同[性能测试](#性能测试)，结果如下：

| 文件系统类型 | ls | ls -R | ls -l |
| --- | --- | --- | --- |
| ext4 | 0.61s | 0.69s | 2.13s |
| cephfs | 1.2s | 21.8s | 42.0s |
| cephfs-kernel | 1.2s | 1.5s | 7s |
| cubefs | 1.s | 5.2s | 5.6s |
| fastcfs | 0.67s | 0.81s | 1.36s |
| curvefs | 0.72s | 24.8s | 29s |
| curvefs | 1.3s | 1.6s | 5.7s |
| curvefs & disable xattr | 1.3s | 1.5s | 2.6s |