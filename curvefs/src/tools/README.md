# curvefs_tool

A tool for CurveFS.

Usage:

```shell
curvefs_tool [Command] [OPTIONS...]
```

When you are not sure how to use a command, --exapmle can give you an example of use:

```shell
curvefs_tool [Command] --example
```

For example:

```shell
$ curvefs_tool status-mds --example
Example :
curvefs_tool status-mds [-confPath=/etc/curvefs/tools.conf] [-rpcTimeoutMs=5000] [-rpcRetryTimes=5] [-mdsAddr=127.0.0.1:16700,127.0.0.1:26700]
```

In addition, this tool reads the configuration from /etc/curvefs/tools.conf,
and can be specified by "-confPath=".

---

## Table of Contents

* [version](#version)
* [status](#status)
* [list](#list)
* [build](#build)
* [umount](#umount)
* [usage](#usage)
* [delete](#delete)
* [check](#check)
* [query](#query)

---

## version

### **version**

show the version of curvefs_tool

Usage:

```shell
curvefs_tool version
```

Output:

```shell
1.0.0
```

[TOC](#table-of-contents)

---

## status

### **status-mds**

show the status of mds who are specified by mdsAddr in the configuration file

Usage:

```shell
curvefs_tool status-mds
```

Output:

```shell
mds version: 1.0.0
leader mds: 10.182.2.45:16700
standy mds: [ ].
offline mds: [ 10.182.2.45:27700 10.182.2.45:36700 ].
```

### **status-metaserver**

show the status of metaserver who are specified by metaserverAddr in the configuration file

Usage:

```shell
curvefs_tool status-metaserver
```

Output:

```shell
metaserver version: 1.0.0
online metaserver: [ 10.182.2.45:16701 10.182.2.45:36701 ].
offline metaserver: [ 10.182.2.45:26701 ].
```

### **status-etcd**

show the status of metaserver who are specified by etcdAddr in the configuration file

Usage:

```shell
curvefs_tool status-etcd
```

Output:

```shell
etcd version: 3.4.0
leader etcd: 10.182.2.45:12379
standy etcd: [ 10.182.2.45:22379 ].
offline etcd: [ 10.182.2.45:32379 ].
```

### **status-copyset**

show the status of copyset

Usage:

```shell
curvefs_tool status-copyset
```

Output:

```shell
all copyset is health.
copyset[4294967297]:
-info:
statusCode: TOPO_OK copysetInfo { poolId: 1 copysetId: 1 peers { id: 1 address: "10.182.2.45:36701:0" } peers { id: 2 address: "10.182.2.45:26701:0" } peers { id: 3 address: "10.182.2.45:16701:0" } epoch: 0 leaderPeer { id: 3 address: "10.182.2.45:16701:0" } }
-status:
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 1 peer { address: "10.182.2.45:16701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 1 lastIndex: 1 diskIndex: 1 epoch: 0 }
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 4 peer { address: "10.182.2.45:26701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 1 lastIndex: 1 diskIndex: 1 epoch: 0 }
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 4 peer { address: "10.182.2.45:36701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 1 lastIndex: 1 diskIndex: 1 epoch: 0 }

...

```

[TOC](#table-of-contents)

---

## list

### **list-fs**

list all fs in cluster

Usage:

```shell
curvefs_tool list-fs
```

Output:

```shell
fsId: 1
fsName: "/test"
status: INITED
rootInodeId: 1
capacity: 18446744073709551615
blockSize: 1
mountNum: 1
mountpoints: "09a03e7f5ece:/usr/local/curvefs/client/mnt"
fsType: TYPE_S3
detail {
  s3Info {
    ak: "********************************"
    sk: "********************************"
    endpoint: "********************************"
    bucketname: "********************************"
    blockSize: 1048576
    chunkSize: 4194304
  }
}

...

```

### **list-coysetInfo**

list all copysetInfo in cluster

Usage:

```shell
curvefs_tool list-copysetInfo
```

Output:

```shell
copyset[4294967297]:
statusCode: TOPO_OK
copysetInfo {
  poolId: 1
  copysetId: 1
  peers {
    id: 1
    address: "10.182.2.45:36701:0"
  }
  peers {
    id: 2
    address: "10.182.2.45:26701:0"
  }
  peers {
    id: 3
    address: "10.182.2.45:16701:0"
  }
  epoch: 0
  leaderPeer {
    id: 3
    address: "10.182.2.45:16701:0"
  }
}

...

```

[TOC](#table-of-contents)

---

## build

### **build-topology**

build cluster topology

Usage:

```shell
curvefs_tool build-topology
```

Output:

```shell

```

[TOC](#table-of-contents)

---

## umount

### **umount-fs**

umount fs from cluster

Usage:

```shell
curvefs_tool umount-fs
```

Output:

```shell
umount fs from cluster success.
```

[TOC](#table-of-contents)

---

## usage

### **usage-metadata**

show the metadata usage of cluster

Usage：

```shell
curvefs_tool usage-metadata
```

Output:

```shell
metaserver[10.182.2.45:26701] usage: total: 1.06 TB used: 755.74 GB left: 327.23 GB
metaserver[10.182.2.45:16701] usage: total: 1.06 TB used: 755.74 GB left: 327.23 GB
metaserver[10.182.2.45:36701] usage: total: 1.06 TB used: 755.74 GB left: 327.23 GB
all cluster usage: total: 3.17 TB used: 2.21 TB left: 981.68 GB
```

[TOC](#table-of-contents)

---

## delete

### **delete-fs**

delete fs by fsName

Usage：

```shell
curvefs_tool delete-fs -fsName=/test -confirm
```

Output:

```shell
1. do you really want to delete fs (/test) :[Ny]y
2. do you really want to delete fs (/test) :[Ny]y
3. do you really want to delete fs (/test) :[Ny]y
delete fs /test success.
```

[TOC](#table-of-contents)

---

## check

### **check-copyset**

checkout copyset status

Usage:

```shell
curvefs_tool checkout-copyset -copysetId=10 -poolId=1
```

Output:

```shell
copyset[4294967306]:
state: 1
peer {
  address: "10.182.2.45:16701:0"
}
leader {
  address: "10.182.2.45:16701:0"
}
readonly: false
term: 2
committedIndex: 1
knownAppliedIndex: 1
pendingIndex: 0
pendingQueueSize: 0
applyingIndex: 0
firstIndex: 2
lastIndex: 1
diskIndex: 1
epoch: 0
```

[TOC](#table-of-contents)

---

## query

### **query-copyset**

query copyset by copysetId

Usage:

```shell
curvefs_tool query-copyset -copysetId=10 -poolId=1
```

Output:

```shell
copyset[4294967306]:
-info:
statusCode: TOPO_OK copysetInfo { poolId: 1 copysetId: 10 peers { id: 1 address: "10.182.2.45:36701:0" } peers { id: 2 address: "10.182.2.45:26701:0" } peers { id: 3 address: "10.182.2.45:16701:0" } epoch: 0 leaderPeer { id: 3 address: "10.182.2.45:16701:0" } }
```

When using the -detail parameter, you can get information about copyset status.

```shell
copyset[4294967306]:
-info:
statusCode: TOPO_OK copysetInfo { poolId: 1 copysetId: 10 peers { id: 1 address: "10.182.2.45:36701:0" } peers { id: 2 address: "10.182.2.45:26701:0" } peers { id: 3 address: "10.182.2.45:16701:0" } epoch: 0 leaderPeer { id: 3 address: "10.182.2.45:16701:0" } }
-status:
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 1 peer { address: "10.182.2.45:16701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 2 lastIndex: 1 diskIndex: 1 epoch: 0 }
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 4 peer { address: "10.182.2.45:26701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 2 lastIndex: 1 diskIndex: 1 epoch: 0 }
status: COPYSET_OP_STATUS_SUCCESS copysetStatus { state: 4 peer { address: "10.182.2.45:36701:0" } leader { address: "10.182.2.45:16701:0" } readonly: false term: 2 committedIndex: 1 knownAppliedIndex: 1 pendingIndex: 0 pendingQueueSize: 0 applyingIndex: 0 firstIndex: 2 lastIndex: 1 diskIndex: 1 epoch: 0 }
```

### **query-partition**

query copyset in partition by partitionId

Usage:

```shell
curvefs_tool query-partition -query-partition -partitionId=1
```

Output:

```shell
statusCode: TOPO_OK
copysetMap {
  key: 1
  value {
    poolId: 1
    copysetId: 8
    peers {
      id: 1
      address: "10.182.2.45:36701:0"
    }
    peers {
      id: 2
      address: "10.182.2.45:26701:0"
    }
    peers {
      id: 3
      address: "10.182.2.45:16701:0"
    }
  }
}
```

### **query-metaserver**

query metaserver by metaserverId or metaserverName

Usage:

```shell
curvefs_tool query-metaserver -metaserverId=1
```

Output:

```shell
statusCode: TOPO_OK
MetaServerInfo {
  metaServerID: 1
  hostname: "******************"
  hostIp: "10.182.2.45"
  port: 36701
  externalIp: "10.182.2.45"
  externalPort: 36701
  onlineState: ONLINE
}
```

### query-fs

query fs by fsId or fsName, fsId first.

Usage:

```shell
curvefs_tool query-fs -fsId=1
```

Output:

```shell
fsId: 1
fsName: "/test"
status: INITED
rootInodeId: 1
capacity: 18446744073709551615
blockSize: 1
mountNum: 1
mountpoints: "09a03e7f5ece:/usr/local/curvefs/client/mnt"
fsType: TYPE_S3
detail {
  s3Info {
    ak: "********************************"
    sk: "********************************"
    endpoint: "********************************"
    bucketname: "********************************"
    blockSize: 1048576
    chunkSize: 4194304
  }
}

```

[TOC](#table-of-contents)

---
