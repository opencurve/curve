[中文版](../cn/k8s_csi_interface.md)

Currently, Curve can be connected to Kubernetes through the CSI plugin. This article gives instructions on the development of CSI plugin. For the source code of the Curve CSI plugin, please see [curve-csi](https://github.com/opencurve/curve-csi).

## Curve Interface

Curve provides a command line management tool curve, which is used to create and delete volumes and other management operations. The specific interface is as follows:

- create volume: `curve create [-h] --filename FILENAME --length LENGTH --user USER`
- delete volume: `curve delete [-h] --user USER --filename FILENAME`
- recover volume: `curve recover [-h] --user USER --filename FILENAME [--id ID]`
- extend volume: `curve extend [-h] --user USER --filename FILENAME --length LENGTH`
- get volume info: `curve stat [-h] --user USER --filename FILENAME`
- rename volume: `curve rename [-h] --user USER --filename FILENAME --newname NEWNAME`
- create directory: `curve mkdir [-h] --user USER --dirname DIRNAME`
- delete directory: `curve rmdir [-h] --user USER --dirname DIRNAME`
- list files in the directory：`curve list [-h] --user USER --dirname DIRNAME`

Provide curve-nbd tool to map, unmap, list on node:

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

In CSI plugin:

- CreateVolume:
  - curve mkdir: DIRNAME defined in `k8s storageClass`
  - curve create: FILENAME is `k8s persistentVolume name`
  - curve stat: wait volume ready
- Controller Publish Volume:
  - Nothing to do
- Node Stage Volume:
  - curve-nbd list-mapped: check if it has been mounted
  - curve-nbd map: mount
- Node Publish Volume:
  - mount the stagePath to the publishPath
- Node Unpublish Volume:
  - umount publishPath
- Node Unstage Volume:
  - curve-nbd list-mapped: check if it has been umounted
  - curve-nbd unmap: umount
- Controller Unpublish Volume:
  - Nothing to do
- DeleteVolume:
  - curve delete

Other optional support:

- Extend：
  - ControllerExpandVolume: curve extend
  - NodeExpandVolume: resize2fs/xfs_growfs

- Snapshot: not yet supported
