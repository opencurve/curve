# curve 工具

## 1. 背景

目前 curve 工具分为两种：bs 和 fs 两种, 其中 fs 更新较频繁，bs 的更新较稳定。
但是两种工具都存在各种问题:

1. 工具输出不支持 json，解析较为繁琐；
2. curvebs 的工具较多，并且缺少文档；
3. 输出不够友好，一些较为规则的数据不能格式化成表格而是直接输出。
4. 使用 C++ 开发，缺少合适的库对输出进行处理

因此考虑使用 go 重构 curve 工具并整合 bs 和 fs，先 fs 后 bs。优化工具的输出，提高工具的友好性。

## 2. 工具整体设计

工具主要分为三个部分：配置、命令执行、输出。主要使用 cobra（命令） 和 viper（配置） 两个模块来实现。

### 2.1 配置

配置使用 viper 从指定的配置文件中读取配置，对于命令所使用到的参数以参数为优先。

命令执行时，可使用 --config（或-c） 来指定配置文件，默认使用 $HOME/.curve/curve.yaml。如：

```bash
curve list fs --config config.yaml
```

配置文件内分为三个部分：curvefs、 curvebs 和 global 。
curvefs 下保存了 fs 工具的配置，curvebs 下保存了 bs 工具的配置， global 保存两者的通用配置如 rpc 相关的配置。
配置文件内容如下所示：

```yaml
global:
  rpctimeout: "5s"
  rpctimes: "3"
curvefs:
  mds.addr: 127.0.0.1:6701
  s3.ak: ak
curvebs:
  mds.addr: 127.0.0.1:7701
```

### 2.2 命令执行

命令的执行需要使用 cobra 模块，实现命令的解析和执行。
命令的执行分为三个步骤：Init、 Run 和 Show。

#### 2.2.1 Init

命令执行前的初始化工作，包括解析参数、读取配置文件、初始化命令执行所需的数据等。
从命令行参数和配置模块中读取所需的配置，以命令行参数为优先填充命令行执行所需的数据。

#### 2.2.2 Run

正式执行命令，分为三种类型：

* a. rpc
  * 需要使用 rpc 接口向其他的模块（mds、metaserver、chunkserver）发送请求，并获取结果。
* 分为两种：
  * send rpc to all hosts：向所有主机成功发送 rpc 请求
  * send rpc to one host：向一个主机成功发送 rpc 请求成功
* b. metric
  * 获取所有主机的指定的 metric 的值
* c. 其他
  * 一般是多个 rpc 或 metric 的组合。是其他命令的组合命令。

命令执行的结果一般保存为 json 格式，在 Show 中根据需要输出。

### 2.2.3 Show

命令执行后的输出，将命令的执行结果输出以 json 格式或 plain 格式输出。

json 格式的输出更加丰富，主要用于与其他工具的交互或者脚本的解析。
标准的 json 格式输出如下：

```json
{
  "code": 0,
  "message": ["success"],
  "detail": {
    ...
  }
}
```

其中 code 为命令的状态码，状态码可分为：
> 0：成功
> 1xxxx：内部错误，一般是命令引起的，详细信息在 message 中。
> 2xxxx: rpc 请求失败，向 server 发送 rpc 请求失败，后四位为client的错误码，详细信息在 message 中。
> 3xxxx: rpc 请求成功，但是结果不正确，后四位为rpc请求的的状态码，详细信息在 message 中。

message 为对状态码的解释，如果出现错误这里将展示产生错误的原因；
detail 为命令的执行结果（一般为 rpc 返回的除状态码外的内容）。

plain 格式的输出相对简单，主要提供人机交互，显示更加友好。
存在错误时，会在命令的最后显示[error]，在下方输出错误信息。

默认为plain格式。

plain 格式的输出示例：

```bash
curve fs list fs
Name    Id  Status  Type    MountNum
test_1  1   INITED  TYPE_S3 1
test_2  2   INITED  TYPE_S3 1
```

json 格式的输出示例：

```bash
curve fs list fs --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "fsInfo":[
            {
                "fsId":2,
                "fsName":"test_2",
                "status":"INITED",
                "rootInodeId":"1",
                "capacity":"18446744073709551615",
                "blockSize":"1048576",
                "mountNum":1,
                "mountpoints":[
                    "localhost:9004:/curvefs/client/mnt/home/nbs/temp/mnt2"
                ],
                "fsType":"TYPE_S3",
                "detail":{
                    "s3Info":{
                        "ak":"minioadmin",
                        "sk":"minioadmin",
                        "endpoint":"http://172.17.0.2:9000",
                        "bucketname":"test",
                        "blockSize":"4194304",
                        "chunkSize":"67108864"
                    }
                },
                "enableSumInDir":false
            },
            {
                "fsId":1,
                "fsName":"test_1",
                "status":"INITED",
                "rootInodeId":"1",
                "capacity":"18446744073709551615",
                "blockSize":"1048576",
                "mountNum":1,
                "mountpoints":[
                    "localhost:9002:/curvefs/client/mnt/home/nbs/temp/mnt1"
                ],
                "fsType":"TYPE_S3",
                "detail":{
                    "s3Info":{
                        "ak":"minioadmin",
                        "sk":"minioadmin",
                        "endpoint":"http://172.17.0.2:9000",
                        "bucketname":"test",
                        "blockSize":"4194304",
                        "chunkSize":"67108864"
                    }
                },
                "enableSumInDir":false
            }
        ]
    }
}
```

## 3. 工具的执行

curve 工具的执行，首先需要指定集群的类型（bs、fs）。然后是要执行的命令（包括子命令）及其参数。
规定可执行的命令不允许有子命令，当输入的命令不完整时，对用户进行提示：

```bash
curve fs list
list command

Usage:
  curve fs list [command]

Available Commands:
  fs          list fs

Flags:
  -h, --help   help for list

Global Flags:
  -c, --conf string     config file (default is $HOME/.curve/curve.yaml)

Use "curve fs list [command] --help" for more information about a command.
```

对于可执行命令，使用 --help 可以获取命令使用的帮助信息，包括所需的参数，参数的含义等。

```bash
curve fs list fs --help
curvefs list fs command

Usage:
  curve fs list [flag]

Flags:
  -h, --help   help for list
  --mdsAddr  string  mds address

Global Flags:
      --conf string     config file (default is $HOME/.curve/curve.yaml)
```

## 4. 命令分类

大致将命令分为 list、summary、query、create、delete、usage 几种类型

### 4.1 list

list 类的命令可以列出集群中各种资源的信息，如 fs、copyset、topology 等。

plain 格式的输出示例：

```bash
curve fs list fs
Name    Id  Status  Type    MountNum
test_1  1   INITED  TYPE_S3 1
test_2  2   INITED  TYPE_S3 1
```

json 格式的输出示例：

```bash
curve fs list fs --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "fsInfo":[
            {
                "fsId":2,
                "fsName":"test_2",
                "status":"INITED",
                "rootInodeId":"1",
                "capacity":"18446744073709551615",
                "blockSize":"1048576",
                "mountNum":1,
                "mountpoints":[
                    "localhost:9004:/curvefs/client/mnt/home/nbs/temp/mnt2"
                ],
                "fsType":"TYPE_S3",
                "detail":{
                    "s3Info":{
                        "ak":"minioadmin",
                        "sk":"minioadmin",
                        "endpoint":"http://172.17.0.2:9000",
                        "bucketname":"test",
                        "blockSize":"4194304",
                        "chunkSize":"67108864"
                    }
                },
                "enableSumInDir":false
            },
            {
                "fsId":1,
                "fsName":"test_1",
                "status":"INITED",
                "rootInodeId":"1",
                "capacity":"18446744073709551615",
                "blockSize":"1048576",
                "mountNum":1,
                "mountpoints":[
                    "localhost:9002:/curvefs/client/mnt/home/nbs/temp/mnt1"
                ],
                "fsType":"TYPE_S3",
                "detail":{
                    "s3Info":{
                        "ak":"minioadmin",
                        "sk":"minioadmin",
                        "endpoint":"http://172.17.0.2:9000",
                        "bucketname":"test",
                        "blockSize":"4194304",
                        "chunkSize":"67108864"
                    }
                },
                "enableSumInDir":false
            }
        ]
    }
}
```


### 4.2 summary

对集群中的信息进行统计并检查状态，如 status（包括 status cluster、status mds、 status metaserver 等）。

plain 格式的输出示例：

``` bash
curve fs status mds
Host            Status  Role  
127.0.0.1:6700  online  mds-standby
127.0.0.1:6701  online  mds-standby
127.0.0.1:6702  online  mds-leader
```

json 格式的输出示例：

```bash
curve fs status mds --format json 

```

当集群状态存在异常时，会对错误进行提示，如：

plain 格式的输出示例：

``` bash
curve fs status mds
Host            Status  Role  
127.0.0.1:6700  online  mds-standby
127.0.0.1:6701  online  mds-standby
127.0.0.1:6702  offline null
[error]
no leader mds  
```

json 格式的输出示例：

```bash
curve fs status mds --format json 
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "role":"mds",
        "online":[
            "127.0.0.1:6700",
            "127.0.0.1:6701"
        ],
        "offline":[

        ],
        "leader":[
            "127.0.0.1:6702"
        ],
        "error":[

        ]
    }
}
```

### 4.3 query

查询集群中的信息，如 fs、copyset、topology 等，与 list 不同的是需要制定要查询的具体资源的唯一标识（如fsId等等）。

plain 格式的输出示例：

```bash
curve fs query fs --fsId 1
"fsId":  1,
"fsName":  "test_1",
"status":  "INITED",
"rootInodeId":  "1",
"capacity":  "18446744073709551615",
"blockSize":  "1048576",
"mountNum":  1,
"mountpoints":  ["localhost:9002:/curvefs/client/mnt/home/nbs/temp/mnt1"],
"fsType":  "TYPE_S3",
"detail":  {
    "s3Info":  {
        "ak":  "minioadmin",
        "sk":  "minioadmin",
        "endpoint":  "http://172.17.0.2:9000",
        "bucketname":  "test",
        "blockSize":  "4194304",
        "chunkSize":  "67108864"
    }
},
"enableSumInDir":  false
```

json 格式的输出示例：

```bash
curve fs query fs --fsId 1 --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "fsInfo":{
            "fsId":1,
            "fsName":"test_1",
            "status":"INITED",
            "rootInodeId":"1",
            "capacity":"18446744073709551615",
            "blockSize":"1048576",
            "mountNum":1,
            "mountpoints":[
                "localhost:9002:/curvefs/client/mnt/home/nbs/temp/mnt1"
            ],
            "fsType":"TYPE_S3",
            "detail":{
                "s3Info":{
                    "ak":"minioadmin",
                    "sk":"minioadmin",
                    "endpoint":"http://172.17.0.2:9000",
                    "bucketname":"test",
                    "blockSize":"4194304",
                    "chunkSize":"67108864"
                }
            },
            "enableSumInDir":false
        }
    }
}
```

### 4.4 create

使用命令创建资源（如fs、topology）。

plain 格式的输出示例：

```bash
curve fs create fs --fsName test_1 --fsType TYPE_S3
create fs success, fsId: 1.
```

json 格式的输出示例：

```bash
curve fs create fs --fsName test_1 --fsType TYPE_S3  --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "fsInfo":{
            "fsId":1,
            "fsName":"test_1",
            "status":"INITED",
            "rootInodeId":"1",
            "capacity":"18446744073709551615",
            "blockSize":"1048576",
            "mountNum":0,
            "mountpoints":[

            ],
            "fsType":"TYPE_S3",
            "detail":{
                "s3Info":{
                    "ak":"minioadmin",
                    "sk":"minioadmin",
                    "endpoint":"http://172.17.0.2:9000",
                    "bucketname":"test",
                    "blockSize":"4194304",
                    "chunkSize":"67108864"
                }
            },
            "enableSumInDir":false
        }
    }
}
```

在创建资源时，如果资源已经存在并且参数一致（除mountpoints和mountNum），则创建成功。
否则创建失败，并返回错误信息。

对于其他创建失败的原因根据返回的状态码 statusCode，确定创建失败的原因并输出。

### 4.4 delete

从集群中删除资源。

plain 格式的输出示例：

```bash
curve fs delete fs --fsName test_1
mountpoint is in use, please check!
```

json 格式的输出示例：

```bash
curve fs delete fs --fsName test_1 --format json
{
    "code":30010,
    "message":[
        "mountpoint is in use, please check!"
    ],
    "detail":{

    }
}
```

此时表示有人正在使用该fs，无法删除。

### 4.5 usage

集群中某种资源的使用情况。

plain 格式的输出示例：

```bash
curve fs usage metadata
Host            Total           Used            Free
127.0.0.1:6802  2199023255552   198372364288    2000650891264
127.0.0.1:6800  2199023255552   198372364288    2000650891264
127.0.0.1:6801  2199023255552   198372364288    2000650891264
```

json 格式的输出示例：

```bash
curve fs usage metadata --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "metadataUsages":[
            {
                "metaserverAddr":"127.0.0.1:6802",
                "total":"2199023255552",
                "used":"198372364288",
                "left":"2000650891264"
            },
            {
                "metaserverAddr":"127.0.0.1:6800",
                "total":"2199023255552",
                "used":"198372286464",
                "left":"2000650891264"
            },
            {
                "metaserverAddr":"127.0.0.1:6801",
                "total":"2199023255552",
                "used":"198372286464",
                "left":"2000650891264"
            }
        ]
    }
}
```

使用参数 -H，可以使得输出更易于理解：

plain 格式的输出示例：

```bash
curve fs usage metadata -H
Host            Total       Used        Free
127.0.0.1:6802  2.00 TiB    184.74 GiB  1.82 TiB
127.0.0.1:6800  2.00 TiB    184.74 GiB  1.82 TiB
127.0.0.1:6801  2.00 TiB    184.74 GiB  1.82 TiB
```

json 格式的输出示例：

```bash
curve fs usage metadata -H --format json
{
    "code":0,
    "message":[
        "success"
    ],
    "detail":{
        "metadataUsages":[
            {
                "metaserverAddr":"127.0.0.1:6802",
                "total":2,
                "totalUnit":"TiB",
                "used":184.74,
                "usedUnit":"GiB",
                "free":1.82,
                "freeUnit":"TiB"
            },
            {
                "metaserverAddr":"127.0.0.1:6800",
                "total":2,
                "totalUnit":"TiB",
                "used":184.74,
                "usedUnit":"GiB",
                "free":1.82,
                "freeUnit":"TiB"
            },
            {
                "metaserverAddr":"127.0.0.1:6801",
                "total":2,
                "totalUnit":"TiB",
                "used":184.74,
                "usedUnit":"GiB",
                "free":1.82,
                "freeUnit":"TiB"
            }
        ]
    }
}
```
