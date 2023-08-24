# snapshotcloneserver interface

## 创建快照：

##### 描述

创建一个快照。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=CreateSnapshot&Version=0.0.6&User=test&File=test&Name=test |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | CreateSnapshot  |
| Version | string | 是       | API版本号 0.0.6 |
| User    | string | 是       | 租户名称        |
| File    | string | 是       | 快照目标文件    |
| Name    | string | 是       | 快照文件名称    |

##### 响应

| 名称      | 类型   | 描述       |
| :-------- | :----- | :--------- |
| Code      | string | 错误码     |
| Message   | string | 错误信息   |
| RequestId | string | 请求ID     |
| UUID      | string | 快照唯一ID |

##### 示例

request

[http://127.0.0.1](http://127.0.0.1/):5555/SnapshotCloneService?Action=CreateSnapshot&Version=0.0.6&User=test&File=test&Name=test

response

```
HTTP/1.1 200 OK
Content-Length: xxx
```

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

​    "UUID" : "xxx"

}

##### 错误码

见最后一节错误码表。

## 删除快照

##### 描述

删除一个快照。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=DeleteSnapshot&Version=0.0.6&User=test&File=test&UUID=uuid1 |

#####  请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | DeleteSnapshot  |
| Version | string | 是       | API版本号 0.0.6 |
| User    | string | 是       | 租户信息        |
| File    | string | 是       | 快照所属文件名  |
| UUID    | string | 是       | 快照唯一ID      |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteSnapshot&Version=0.0.6&User=test&File=test&UUID=uuid1
```

response

HTTP/1.1 200 OK

Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

##### 错误码

见最后一节错误码表。

 

## 取消快照

取消一个正在执行的快照。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=CancelSnapshot&Version=0.0.6&User=test&File=test&UUID=uuid1 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述            |
| :------ | :----- | :------- | :-------------- |
| Action  | string | 是       | CancelSnapshot  |
| Version | string | 是       | API版本号 0.0.6 |
| User    | string | 是       | 租户信息        |
| File    | string | 是       | 快照所属文件名  |
| UUID    | string | 是       | 快照唯一ID      |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=CancelSnapshot&Version=0.0.6&User=test&File=test&UUID=uuid1
response
```

HTTP/1.1 200 OK

Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

##### 错误码

见最后一节错误码表。



## 查询文件的快照信息

##### 描述

查询指定文件的所有快照信息，

若指定UUID，则查询该文件的该UUID指定的快照信息

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=GetFileSnapshotInfo&Version=0.0.6&User=test&File=test&Limit=10&Offset=0 |
| GET    | /SnapshotCloneService?Action=GetFileSnapshotInfo&Version=0.0.6&User=test&File=test&UUID=de06df66-b9e4-44df-ba3d-ac94ddee0b28 |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述                   |
| :------ | :----- | :------- | :--------------------- |
| Action  | string | 是       | GetFileSnapinfo        |
| Version | string | 是       | API版本号 0.0.6        |
| User    | string | 是       | 租户信息               |
| File    | string | 是       | 文件名称               |
| Limit   | int    | 否       | 最大显示条数，默认为10 |
| Offset  | int    | 否       | 偏移值，默认为0        |
| UUID    | string | 否       | 快照的uuid信息         |

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
| File       | string | 文件名称                                                     |
| SeqNum     | uint32 | 快照版本号                                                   |
| Name       | string | 快照名称                                                     |
| Time       | uint64 | 创建时间                                                     |
| FileLength | uint32 | 文件大小（单位Byte）                                         |
| Status     | enum   | 快照处理的状态（0:done, 1:pending, 2:deleteing, 3:errorDeleting, 4:canceling, 5:error） |
| Progress   | uint32 | 快照完成百分比                                               |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=0.0.6&User=zjm&File=/zjm/test1&Limit=10 
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

​        "File" : "/zjm/test1",

​        "FileLength" : 10737418240,

​        "Name" : "snap1",

​        "Progress" : 30,

​        "SeqNum" : 1,

​        "Status" : 1,

​        "Time" : 1564391913582677,

​        "UUID" : "de06df66-b9e4-44df-ba3d-ac94ddee0b28",

​        "User" : "zjm"

​     }

   ]

}

##### 错误码

见最后一节错误码表。

## 克隆：

##### 描述

从快照或镜像克隆一个文件。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=Clone&Version=0.0.6&User=zjm&Source=/zjm/test1&Destination=/zjm/clone1&Lazy=true |

#####  请求参数

| 名称        | 类型   | 是否必须 | 描述                   |
| :---------- | :----- | :------- | :--------------------- |
| Action      | string | 是       | Clone                  |
| Version     | string | 是       | API版本号 0.0.6        |
| User        | string | 是       | 租户信息               |
| Source      | string | 是       | 镜像文件名或者快照UUID |
| Destination | string | 是       | 克隆目标文件名         |
| Lazy        | bool   | 是       | 是否Lazy克隆           |

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
http://127.0.0.1:5555/SnapshotCloneService?Action=Clone&Version=0.0.6&User=zjm&Source=/zjm/test1&Destination=/zjm/clone1&Lazy=true 
response
HTTP/1.1 200 OK
Content-Length: xxx
```

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx",

​    "UUID" : "xxx"

}

##### 错误码

见最后一节错误码表。

## 恢复：

##### 描述

从快照恢复一个文件。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=Recover&Version=0.0.6&User=zjm&Source=de06df66-b9e4-44df-ba3d-ac94ddee0b28&Destination=/zjm/recover1&Lazy=true |

#####  请求参数

| 名称        | 类型   | 是否必须 | 描述            |
| :---------- | :----- | :------- | :-------------- |
| Action      | string | 是       | Recover         |
| Version     | string | 是       | API版本号 0.0.6 |
| User        | string | 是       | 租户信息        |
| Source      | string | 是       | 快照UUID        |
| Destination | string | 是       | 克隆目标文件名  |
| Lazy        | bool   | 是       | 是否Lazy克隆    |

##### 响应

| 名称      | 类型   | 描述             |
| :-------- | :----- | :--------------- |
| Code      | string | 错误码           |
| Message   | string | 错误信息         |
| RequestId | string | 请求ID           |
| UUID      | string | 恢复任务的唯一ID |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=Recover&Version=0.0.6&User=zjm&Source=de06df66-b9e4-44df-ba3d-ac94ddee0b28&Destination=/zjm/recover1&Lazy=true 
response
HTTP/1.1 200 OK
Content-Length: xxx
```

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx",

​    "UUID" : "xxx"

}

##### 错误码

见最后一节错误码表。

## Flatten

##### 描述

对Lazy克隆的文件进行恢复数据操作。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=Flatten&Version=0.0.6&User=zjm&UUID=xxx |

#####  请求参数

| 名称    | 类型   | 是否必须 | 描述           |
| :------ | :----- | :------- | :------------- |
| Action  | string | 是       | Flatten        |
| Version | string | 是       | API版本号0.0.6 |
| User    | string | 是       | 租户信息       |
| UUID    | string | 是       | 任务唯一ID     |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=Flatten&Version=0.0.6&User=zjm&UUID=de06df66-b9e4-44df-ba3d-ac94ddee0b28
response
HTTP/1.1 200 OK
Content-Length: xxx
```

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

##### 错误码

见最后一节错误码表。

 

## 查询指定用户的克隆/恢复任务信息：

##### 描述

查询指定用户的所有克隆/恢复任务信息，

若指定UUID，则查询该用户的该UUID的任务信息

若指定File，则查询该用户的该文件的任务信息

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=GetCloneTasks&Version=0.0.6&User=zjm&Limit=10&Offset=0 |
| GET    | /SnapshotCloneService?Action=GetCloneTasks&Version=0.0.6&User=zjm&UUID=xxx" |
| GET    | /SnapshotCloneService?Action=GetCloneTasks&Version=0.0.6&User=zjm&File=DestFileName" |

##### 请求参数

| 名称    | 类型   | 是否必须 | 描述                      |
| :------ | :----- | :------- | :------------------------ |
| Action  | string | 是       | GetCloneTasks             |
| Version | string | 是       | API版本号 0.0.6           |
| User    | string | 是       | 租户信息                  |
| Limit   | int    | 否       | 最大显示任务数，默认为10  |
| Offset  | int    | 否       | 偏移值，默认为0           |
| UUID    | string | 否       | 克隆/恢复任务唯一ID       |
| File    | string | 否       | 克隆/恢复任务的目标文件名 |

##### 响应

| 名称       | 类型     | 描述         |
| :--------- | :------- | :----------- |
| Code       | string   | 错误码       |
| Message    | string   | 错误信息     |
| RequestId  | string   | 请求ID       |
| TotalCount | int      | 任务总个数   |
| TaskInfos  | TaskInfo | 任务信息列表 |

TaskInfo类型说明

| 名称       | 类型   | 描述                                                         |
| :--------- | :----- | :----------------------------------------------------------- |
| UUID       | string | 任务唯一ID                                                   |
| TaskType   | enum   | 任务类型（0:clone, 1:recover）                               |
| User       | string | 租户信息                                                     |
| File       | string | 文件名称                                                     |
| Time       | uint64 | 创建时间                                                     |
| TaskStatus | enum   | 任务的状态（0:done, 1:cloning, 2:recovering, 3:cleaning, 4:errorCleaning, 5:error，6:retrying, 7:metaInstalled） |

##### 示例

request

```
http://127.0.0.1:5555//SnapshotCloneService?Action=GetCloneTasks&Version=0.0.6&User=zjm&Limit=10"
response
```

HTTP/1.1 200 OK

Content-Length: xxx

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx", 

​    "TotalCount": 1,

​    "TaskInfos" :

​    [

​        {

​            "File" : "/zjm/clone1",

​            "UUID" : "78e83875-2b50-438f-8f25-36715380f4f5",

​            "TaskStatus" : 5,

​            "TaskType" : 0,

​            "Time" : 0,

​            "User" : "zjm"

​        }

​    ]

}



##### 错误码

见最后一节错误码表。

## 清除克隆/恢复任务：

##### 描述

清除克隆/恢复任务。若是失败的任务，还会删除curvefs上的临时克隆文件，否则只删除任务。

##### 语法

| Method | Url                                                          |
| :----- | :----------------------------------------------------------- |
| GET    | /SnapshotCloneService?Action=CleanCloneTask&Version=0.0.6&User=zjm&UUID=78e83875-2b50-438f-8f25-36715380f4f5 |

#####  请求参数

| 名称    | 类型   | 是否必须 | 描述                |
| :------ | :----- | :------- | :------------------ |
| Action  | string | 是       | CleanCloneTask      |
| Version | string | 是       | API版本号 0.0.6     |
| User    | string | 是       | 租户信息            |
| UUID    | string | 是       | 克隆/恢复任务唯一ID |

##### 响应

| 名称      | 类型   | 描述     |
| :-------- | :----- | :------- |
| Code      | string | 错误码   |
| Message   | string | 错误信息 |
| RequestId | string | 请求ID   |

##### 示例

request

```
http://127.0.0.1:5555/SnapshotCloneService?Action=CleanCloneTask&Version=0.0.6&User=zjm&UUID=78e83875-2b50-438f-8f25-36715380f4f5
```

response

```
HTTP/1.1 200 OK
Content-Length: xxx
```

{

​    "Code" : "0",

​    "Message" : "Exec success.",

​    "RequestId" : "xxx"

}

##### 错误码

见最后一节错误码表。

## 错误码表：

| Code | Message                         | HTTP Status Code | 描述                                      | 补充说明                                                     |
| :--- | :------------------------------ | :--------------- | :---------------------------------------- | :----------------------------------------------------------- |
| 0    | Exec success.                   | 200              | 执行成功                                  |                                                              |
| -1   | Internal error.                 | 500              | 未知内部错误                              | 未知错误，任何请求不应出现此错误，已知的错误都应有明确的错误码。如发现此错误，请联系相关人员定位。 |
| -2   | Server init fail.               | 500              | 服务器初始化失败                          | 任何请求不应出现此错误，初始化阶段错误                       |
| -3   | Server start fail.              | 500              | 服务器启动失败                            | 任何请求不应出现此错误，初始化阶段错误                       |
| -4   | Service is stop                 | 500              | 服务已停止                                | snapshotcloneserver停止服务退出阶段，发送请求会收到此错误    |
| -5   | BadRequest:"Invalid request."   | 400              | 非法的请求                                | 发送http请求格式非法：缺少字段，字段值非法等                 |
| -6   | Task already exist.             | 500              | 任务已存在                                | 目前不会出现                                                 |
| -7   | Invalid user.                   | 500              | 非法的用户                                | 请求中User字段与操作的文件、镜像或快照的owner不匹配          |
| -8   | File not exist.                 | 500              | 文件不存在                                | 打快照时目标文件不存在从快照恢复时，目标文件不存在从快照或镜像克隆/恢复时，快照或镜像不存在清除指定clone任务时，clone任务不存在获取指定克隆/恢复任务时，任务不存在获取指定快照时，快照不存在 |
| -9   | File status invalid.            | 500              | 文件状态异常                              | 打快照时，目标文件正在克隆/恢复中或删除中等状态而不是Normal状态，返回文件状态异常从镜像克隆过程中，源镜像文件正在克隆/恢复中或删除中等状态而不是Normal状态，返回文件状态异常 |
| -10  | Chunk size not aligned.         | 500              | chunk大小未按分片对齐                     | 一般是配置文件问题，配置的chunk分配大小与chunksize未对其，正常情况下不会出现 |
| -11  | FileName not match.             | 500              | 文件名不匹配                              | 删除或取消快照接口，快照所属文件与快照不匹配，即该文件没有这个快照 |
| -12  | Cannot delete unfinished.       | 500              | 不能删除未完成的快照                      |                                                              |
| -13  | Cannot create when has error.   | 500              | 不能对存在错误的文件打快照或克隆/恢复     | 不能对存在错误快照的文件再次打快照不能对存在错误克隆/恢复任务的目标文件再次克隆/恢复 |
| -14  | Cannot cancel finished.         | 500              | 待取消的快照已完成或不存在                |                                                              |
| -15  | Invalid snapshot.               | 500              | 不能对未完成或存在错误的快照进行克隆/恢复 |                                                              |
| -16  | Cannot delete when using.       | 500              | 不能删除正在克隆/恢复的快照               |                                                              |
| -17  | Cannot clean task unfinished.   | 500              | 不能清理未完成的克隆/恢复任务             |                                                              |
| -18  | Snapshot count reach the limit. | 500              | 快照到达上限                              |                                                              |
| -19  | File exist.                     | 500              | 文件已存在                                | 从快照或镜像克隆时，目标文件已存在                           |
| -20  | Task is full.                   | 500              | 克隆/恢复任务已满                         |                                                              |
