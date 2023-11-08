# localsnapshotclone restful api

localsnapshotclone restful api 需兼容原有s3快照api: docs/cn/snapshotcloneserver_interface.md, 使用两套API均可以调用本地快照和克隆功能,
此处, 列出本地快照和克隆新api


## 创建卷：

创建一个卷。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=CreateFile&Version=1.5&User=curve&File=/test1&Size=100 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | CreateFile      |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户信息        |
| File    | string | 是       | 卷路径          |
| Size    | uint64    | 是       | 卷大小，单位为GB |
| StripeUnit | uint64 | 否       | 条带的宽度, 默认为0，无条带 |
| StripeCount | uint64 | 否       | 条带数量, 默认为0， 无条带 | 
| PoolSet | string | 否       | poolset name, 默认使用默认存储池|

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=CreateFile&Version=1.5&User=curve&File=/test1&Size=100
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

```

##### 错误码

见最后一节错误码表。

## 删除卷:

删除一个卷。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=DeleteFile&Version=1.5&User=curve&File=/test1 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | DeleteFile      |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户信息        |
| File    | string | 是       | 卷路径          |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteFile&Version=1.5&User=curve&File=/test1
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

```

##### 错误码

见最后一节错误码表。

## 列出卷

列出指定目录下的所有卷信息

若指定卷名，则列出指定卷的信息

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=ListFile&Version=1.5&User=curve&Dir=/&Limit=10&Offset=0 |
| GET    | /SnapshotCloneService?Action=ListFile&Version=1.5&User=curve&File=/test1 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | ListFile        |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户信息        |
| Dir     | string | 否       | 目录路径        |
| File    | string | 否       | 卷路径          |
| Limit   | uint64 | 否       | 返回的卷数量    |
| Offset  | uint64 | 否       | 偏移量          |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |
| TotalCount | int   | 总个数   |
| FileInfos | array  | 卷信息   |

FileInfo类型说明

| 名称       | 类型   | 描述                                                         |
| :--------- | :----- | :----------------------------------------------------------- |
| File       | string | 卷名                                                         |
| User       | string | 租户信息                                                     |
| Type       | enum   | 类型, 0:卷, 1:目录, 2:未知类型    |
| Time       | uint64 | 创建时间                                                     |
| FileLength | uint64 | 卷大小（单位Byte）                                         |
| StripeUnit | uint64 | 条带的宽度, 默认为0，无条带 |
| StripeCount | uint64 | 条带数量, 默认为0， 无条带 | 
| Status     | enum   | 卷的状态, 0:done, 1:flattening, 2:unflattened |
| Readonly   | bool   | 是否只读                                                     |
| Progress   | uint32 | 完成百分比, flattening状态时则表示百分比进度  |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=ListFile&Version=1.5&User=curve&Dir=/&Limit=10&Offset=0
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx

{
        "Code" : "0",
        "FileInfos" :
        [
                {
                        "File" : "/RecycleBin",
                        "FileLength" : 0,
                        "Progress" : 100,
                        "Status" : 0,
                        "StripeCount" : 0,
                        "StripeUnit" : 0,
                        "Time" : 1696761354857556,
                        "Type" : 1,
                        "User" : "root"
                },
                {
                        "File" : "/clone",
                        "FileLength" : 0,
                        "Progress" : 100,
                        "Status" : 0,
                        "StripeCount" : 0,
                        "StripeUnit" : 0,
                        "Time" : 1696761362745606,
                        "Type" : 1,
                        "User" : "root"
                },
                {
                        "File" : "/test1",
                        "FileLength" : 107374182400,
                        "Progress" : 100,
                        "Status" : 0,
                        "StripeCount" : 0,
                        "StripeUnit" : 0,
                        "Time" : 1696761378914477,
                        "Type" : 0,
                        "User" : "curve"
                }
        ],
        "Message" : "Exec success.",
        "RequestId" : "26ffe85b-55e8-46c4-8823-0104fcf9b362",
        "TotalCount" : 3
}
```

##### 错误码

见最后一节错误码表。

## 创建快照：

##### 描述

创建一个快照。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=CreateSnapshot&Version=1.5&User=curve&File=/test1&Name=snap1 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | CreateSnapshot  |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户名称        |
| File    | string | 是       | 快照所属的卷路径    |
| Name    | string | 是       | 快照名称    |

##### 响应

| 名称      | 类型   | 描述       |
| :-------- | :----- | :--------- |
| Code      | string | 错误码     |
| Message   | string | 错误信息   |
| RequestId | string | 请求ID     |
| UUID      | string | 快照唯一ID |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=CreateSnapshot&Version=1.5&User=curve&File=/test1&Name=snap1
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

​    "UUID" : "xxx"

}

```

##### 错误码

见最后一节错误码表。

## 删除快照

##### 描述

删除一个快照。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=DeleteSnapshot&Version=1.5&User=curve&File=/test1&Name=snap1 |

#####  请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | DeleteSnapshot  |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户信息        |
| File    | string | 是       | 快照所属卷路径  |
| Name    | string | 是       | 快照名      |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteSnapshot&Version=1.5&User=curve&File=/test1&Name=snap1
```

response

```
HTTP/1.1 200 OK

Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}
```

##### 错误码

见最后一节错误码表。

## 列出快照

##### 描述

查询指定卷的所有快照信息，

若指定快照名，则查询指定快照信息。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1.5&User=curve&File=/test1&Limit=10&Offset=0 |
| GET    | /SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1.5&User=curve&File=/test1&Name=snap1 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述                   |
| :------ | :----- | :------- | :--------------------- |
| Action  | string | 是       | GetFileSnapinfo        |
| Version | string | 是       | API版本号        |
| User    | string | 是       | 租户信息               |
| File    | string | 是       | 卷路径               |
| Limit   | int    | 否       | 最大显示条数，默认为10 |
| Offset  | int    | 否       | 偏移值，默认为0        |
| Name    | string | 否       | 快照名         |

##### 响应

| 名称       | 类型     | 描述         |
| :--------- | :------- | :----------- |
| Code       | string   | 错误码       |
| Message    | string   | 错误信息     |
| RequestId  | string   | 请求ID       |
| TotalCount | int      | 快照总个数   |
| Snapshots  | Snapshot | 快照信息列表 |

Snapshot类型说明

| 名称       | 类型   | 描述                                                         |
| :--------- | :----- | :----------------------------------------------------------- |
| UUID       | string | 快照唯一ID                                                   |
| User       | string | 租户信息                                                     |
| File       | string | 快照所属卷路径                                                     |
| SeqNum     | uint32 | 快照版本号                                                   |
| Name       | string | 快照名称                                                     |
| Time       | uint64 | 创建时间                                                     |
| FileLength | uint64 | 卷大小（单位Byte）                                         |
| Status     | enum   | 快照处理的状态（0:done, 2:deleteing, 5:error） |
| Progress   | uint32 | 快照完成百分比, 创建快照只有0和100两种情况, 删除快照时则表示删除百分比进度  |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1.5&User=curve&File=/test1&Limit=10&Offset=0
```

response

```
HTTP/1.1 200 OK

Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx",

​    "TotalCount": 1,

​    "Snapshots":

 [

​    {

​        "File" : "/test1",

​        "FileLength" : 10737418240,

​        "Name" : "snap1",

​        "Progress" : 30,

​        "SeqNum" : 1,

​        "Status" : 1,

​        "Time" : 1564391913582677,

​        "UUID" : "de06df66-b9e4-44df-ba3d-ac94ddee0b28",

​        "User" : "curve"

​     }

   ]

}
```

##### 错误码

见最后一节错误码表。

## 克隆：

##### 描述

从快照克隆一个子卷

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=Clone&Version=1.5&User=curve&File=/test1&Name=snap1&Destination=/clone1 |

#####  请求参数

| 名称        | 类型   | 是否必须 | 描述                   |
| :---------- | :----- | :------- | :--------------------- |
| Action      | string | 是       | Clone                  |
| Version     | string | 是       | API版本号              |
| User        | string | 是       | 租户信息               |
| File        | string | 是       | 卷路径 |
| Name        | string | 是       | 快照名                 |
| Destination | string | 是       | 克隆目标路径         |
| Readonly    | bool   | 否       | 是否克隆成只读卷     |

##### 响应

| 名称      | 类型   | 描述           |
| :-------- | :----- | :------------- |
| Code      | string | 错误码         |
| Message   | string | 错误信息       |
| RequestId | string | 请求ID         |
| UUID      | string | 克隆任务唯一ID |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=Clone&Version=1.5&User=curve&File=/test1&Name=snap1&Destination=/clone1
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx",

}
```

##### 错误码

见最后一节错误码表。

## 回滚：

暂不支持

## Flatten

##### 描述

补足子卷的数据，使其与父卷和快照解耦

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=Flatten&Version=1.5&User=curve&File=/clone1 |

#####  请求参数

| 名称    | 类型   | 是否必须 | 描述           |
| :------ | :----- | :------- | :------------- |
| Action  | string | 是       | Flatten        |
| Version | string | 是       | API版本号 |
| User    | string | 是       | 租户信息       |
| File    | string | 是       | 目标卷路径     |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=Flatten&Version=1.5&User=curve&File=/clone1
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx
{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}
```

##### 错误码

见最后一节错误码表。

## 错误码表：

| Code | Message                         | HTTP Status Code | 描述                                      | 补充说明                                                     |
| :--- | :------------------------------ | :--------------- | :---------------------------------------- | :----------------------------------------------------------- |
| 0    | Exec success.                   | 200              | 执行成功                                  |                                                              |
| -1   | Internal error.                 | 500              | 未知内部错误                              | 未知错误，任何请求不应出现此错误，已知的错误都应有明确的错误码。如发现此错误，请联系开发人员定位。 |
| -2   | Server init fail.               | 500              | 服务器初始化失败                          | 任何请求不应出现此错误，初始化阶段错误                       |
| -3   | Server start fail.              | 500              | 服务器启动失败                            | 任何请求不应出现此错误，初始化阶段错误                       |
| -4   | Service is stop                 | 500              | 服务已停止                                | snapshotcloneserver停止服务退出阶段，发送请求会收到此错误    |
| -5   | BadRequest:"Invalid request."   | 400              | 非法的请求                                | 发送http请求格式非法：缺少字段，字段值非法等                 |
| -6   | Task already exist.             | 500              | 任务已存在                                | 目前不会出现                                                 |
| -7   | Invalid user.                   | 500              | 非法的用户                                | 请求中User字段与操作的卷或快照的owner不匹配          |
| -8   | File not exist.                 | 500              | 文件或快照不存在                          | 打快照时目标卷不存在; 从快照回滚时，目标卷不存在; 从快照克隆/回滚时，快照不存在; |
| -9   | File status invalid.            | 500              | 文件状态异常                              | 打快照时，目标卷正在克隆/回滚中或删除中等状态而不是Normal状态，返回文件状态异常; 从快照克隆过程中，快照正在删除中等状态而不是Normal状态，返回文件状态异常 |
| -10  | Chunk size not aligned.         | 500              | chunk大小未按分片对齐                     | 一般是配置文件问题，配置的chunk分配大小与chunksize未对其，正常情况下不会出现 |
| -11  | FileName not match.             | 500              | 文件名不匹配                              | 删除快照接口，快照所属卷与快照不匹配，即该卷没有这个快照 |
| -12  | Cannot delete unfinished.       | 500              | 不能删除未完成的快照                      |                                                              |
| -13  | Cannot create when has error.   | 500              | 不能对存在错误的卷打快照或克隆/回滚       | 不能对存在错误快照的卷再次打快照 |
| -14  | Cannot cancel finished.         | 500              | 待取消的快照已完成或不存在                | 目前不会出现                                                 |
| -15  | Invalid snapshot.               | 500              | 不能对未完成或存在错误的快照进行克隆/回滚 |                                                              |
| -16  | Cannot delete when using.       | 500              | 不能删除正在克隆/回滚的快照               |                                                              |
| -17  | Cannot clean task unfinished.   | 500              | 不能清理未完成的克隆/回滚任务             | 目前不会出现                                                 |
| -18  | Snapshot count reach the limit. | 500              | 快照到达上限                              |                                                              |
| -19  | File exist.                     | 500              | 文件或快照已存在                          | 从快照克隆时，目标卷已存在                                   |
| -20  | Task is full.                   | 500              | 克隆/回滚任务已满                         | 目前不会出现                                                 |
| -21  | not support.                    | 500              | 不支持的操作                              |                                                              |
| -22  | File is under snapshot          | 500              | 删除的卷具有快照                          |                                                              |
| -23  | File is occuiped                | 500              | 删除的卷正在使用(挂载状态或者Flatting中)  |                                                              |
| -24  | Invalid argument.               | 500              | 非法的请求参数                            |                                                              |



