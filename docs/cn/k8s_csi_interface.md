目前，curve可以通过CSI插件的方式对接Kubernetes。本文给出的是CSI插件开发指导说明。curve csi插件源码请见[curve-csi](https://github.com/opencurve/curve-csi)。

## Curve Interface

curve提供命令行管理工具curve，用来创建、删除卷等管理操作。具体接口如下：

- 创建卷：`curve create [-h] --filename FILENAME --length LENGTH --user USER`
- 删除卷：`curve delete [-h] --user USER --filename FILENAME`
- 扩容卷：`curve extend [-h] --user USER --filename FILENAME --length LENGTH`
- 查询卷：`curve stat [-h] --user USER --filename FILENAME`
- rename卷：`curve rename [-h] --user USER --filename FILENAME --newname NEWNAME`
- 创建目录：`curve mkdir [-h] --user USER --dirname DIRNAME`
- 删除目录：`curve rmdir [-h] --user USER --dirname DIRNAME`
- 查询目录下所有文件：`curve list [-h] --user USER --dirname DIRNAME`

提供curve-nbd工具，在node节点上提供map，unmap，list功能:

```bash
Usage: curve-nbd [options] map <image>           (Map an image to nbd device)
            unmap <device|image>                            (Unmap nbd device)
            list-mapped                                              (List mapped nbd devices)
Map options:
--device <device path> Specify nbd device path (/dev/nbd{num})
--read-only Map read-only
--nbds_max <limit> Override for module param nbds_max
--max_part <limit> Override for module param max_part
--timeout <seconds> Set nbd request timeout
--try-netlink Use the nbd netlink interface
```

## Implementing with Kubernetes CSI

CSI spec:

```
   CreateVolume +------------+ DeleteVolume
 +------------->|  CREATED   +--------------+
 |              +---+----^---+              |
 |       Controller |    | Controller       v
+++         Publish |    | Unpublish       +++
|X|          Volume |    | Volume          | |
+-+             +---v----+---+             +-+
                | NODE_READY |
                +---+----^---+
               Node |    | Node
              Stage |    | Unstage
             Volume |    | Volume
                +---v----+---+
                |  VOL_READY |
                +---+----^---+
               Node |    | Node
            Publish |    | Unpublish
             Volume |    | Volume
                +---v----+---+
                | PUBLISHED  |
                +------------+
```

CSI插件的对应：

- CreateVolume:
  - curve mkdir: DIRNAME在`k8s storageClass`定义
  - curve create: FILENAME为`k8s persistentVolume name`
  - curve stat: 等待卷ready
- Controller Publish Volume:
  - Nothing to do
- Node Stage Volume:
  - curve-nbd list-mapped: 查看是否已经被挂载
  - curve-nbd map: 挂载
- Node Publish Volume:
  - mount the stagePath to the publishPath
- Node Unpublish Volume:
  - umount publishPath
- Node Unstage Volume:
  - curve-nbd list-mapped: 查看是否已经被卸载
  - curve-nbd unmap: 卸载
- Controller Unpublish Volume:
  - Nothing to do
- DeleteVolume:
  - curve delete

其他可选支持：

- 扩容：
  - ControllerExpandVolume: curve extend
  - NodeExpandVolume: resize2fs/xfs_growfs

- 快照：暂未支持
