# DataStore集成测试

## 背景介绍

集成测试的方法有很多种，但是归纳起来主要有两种。一种是一次性将所有单元组装起来进行测试的“大棒”模式，一种是一层一层累加递增式的测试。DataStore层是chunkserver端底层的一个模块，其下面还有LocalFileSystem层；DataStore层的集成测试就是将DataStore和LocalFileSystem层组装起来然后进行测试，属于前面所述的第二种模式中自底向上的测试方法。

##### DataStore层作用

DataStore的作用是将上层对Chunk的操作转换到对本地文件的操作，并维护本地文件的数据和状态。

##### 对DataStore层集成测试目的

- 保证DataStore层和LocalFileSystem层能正常协作
- 保证调用指定的接口能够返回预期的结果
- 保证文件的数据和状态始终保持正确
- 在测试过程中帮助思考和发现之前未考虑到的场景

## 用例设计

### 功能测试

对于DataStore来说，所谓的Given其实就是要操作的Chunk的状态，When就是用不同的参数组合来调用不同的接口；也就是说测试DataStore就是测试不同接口参数同文件状态的所有组合。

#### DataStore文件状态

| 状态项              |
| :------------------ |
| chunk是否存在       |
| chunk的大小         |
| chunk的sn           |
| chunk的correctedSn  |
| chunk快照是否存在   |
| chunk快照page的状态 |
| 是否为clone chunk   |
| chunk的page的状态   |

可以用等价类划分法来将连续的参数取值范围划分为几组离散的条件，划分依据是将输入参数与当前chunk的状态信息进行对比（不同模块可以有不同的依据来进行划分，但本质其实是一样的）。

#### 条件分析

##### WriteChunk

条件

| 参数                                                         | 条件项                                                       | 条件                    |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :---------------------- |
| id                                                           | 指定id的chunk是否存在                                        | chunk存在               |
| chunk不存在                                                  |                                                              |                         |
| chunk的快照是否存在                                          | 快照存在                                                     |                         |
| 快照不存在                                                   |                                                              |                         |
| chunk是否为clonechunk                                        | 是clone chunk                                                |                         |
| 不是clone chunk                                              |                                                              |                         |
| sn                                                           | 与chunk的sn对比                                              | sn>chunkinfo.sn         |
| sn<chunkinfo.sn                                              |                                                              |                         |
| sn==chunkinfo.sn                                             |                                                              |                         |
| 与chunk的correctedSn对比                                     | sn>chunkinfo.correctedSn                                     |                         |
| sn<chunkinfo.correctedSn                                     |                                                              |                         |
| sn==chunkinfo.correctedSn                                    |                                                              |                         |
| offset 和 length（实际上就是请求写入的区域）                 | 与chunk size对比（本质就是判断写入区域是否超过chunk的大小范围） | offset+length>chunksize |
| offset+length<chunksize                                      |                                                              |                         |
| offset+length==chunksize                                     |                                                              |                         |
| 与快照的写入状态对比（快照存在时）                           | 请求写入区域已经cow过                                        |                         |
| 请求写入的区域还未cow                                        |                                                              |                         |
| 请求写入的区域部分cow过，部分未cow                           |                                                              |                         |
| 与chunk的写入状态对比（对于写clone chunk来说，还隐含一个条件：写完以后chunk是否全部被覆盖写了） | 请求写入的区域已经写过                                       |                         |
| 请求写入的区域还未写过                                       |                                                              |                         |
| 请求写入的区域部分写过，部分未写过                           |                                                              |                         |

如果单纯考虑上面所有条件项的排列组合，其组合数是很大的。实际上很多条件项之间是互斥、依赖或其他一些关系，并不是彼此独立的，这样就没法单纯的使用组合测试的方法，需要梳理条件之间的关系，但是这样一来组合数也会大大减少。

##### ReadChunk

条件项

| 参数                        | 条件项                                                       | 条件                    |
| :-------------------------- | :----------------------------------------------------------- | :---------------------- |
| id                          | chunk是否存在                                                | 存在                    |
| 不存在                      |                                                              |                         |
| 快照是否存在（不影响）      | 存在                                                         |                         |
| 不存在                      |                                                              |                         |
| 是否为clone chunk（不影响） | 是                                                           |                         |
| 不是                        |                                                              |                         |
| sn                          | 可取任意值（不影响）                                         | sn==chunkinfo.sn        |
| sn!=chunkinfo.sn            |                                                              |                         |
| offset和length              | 与chunk size对比（本质就是判断写入区域是否超过chunk的大小范围） | offset+length>chunksize |
|                             | offset+length<chunksize                                      |                         |
|                             | offset+length==chunksize                                     |                         |
|                             | 与chunk的写入状态对比                                        | 请求读取的区域已经写过  |
|                             | 请求读取的区域还未写过                                       |                         |
|                             | 请求读取的区域部分写过，部分未写过                           |                         |

##### ReadSnapshotChunk

条件项

| 参数                                                     | 条件项                                                       | 条件                    |
| :------------------------------------------------------- | :----------------------------------------------------------- | :---------------------- |
| id                                                       | chunk是否存在                                                | 存在                    |
| 不存在                                                   |                                                              |                         |
| 快照是否存在                                             | 存在                                                         |                         |
| 不存在                                                   |                                                              |                         |
| sn                                                       | 与chunk的版本和快照的版本对比                                | sn==chunkinfo.sn        |
| sn==snapsn                                               |                                                              |                         |
| sn!=chunkinfo.sn && sn!=snapsn                           |                                                              |                         |
| offset和length                                           | 与chunk size对比（本质就是判断写入区域是否超过chunk的大小范围） | offset+length>chunksize |
| offset+length<chunksize                                  |                                                              |                         |
| offset+length==chunksize                                 |                                                              |                         |
| 与快照的写入状态对比（快照存在时，且请求sn等于快照版本） | 请求读取的区域已经cow                                        |                         |
| 请求读取的区域未cow                                      |                                                              |                         |
| 请求读取的区域部分cow                                    |                                                              |                         |
| 与chunk的写入状态对比                                    | 请求读取的区域已经写过                                       |                         |
| 请求读取的区域未写过                                     |                                                              |                         |
| 请求读取的区域部分写过                                   |                                                              |                         |

##### DeleteChunk

条件项

| 参数                        | 条件项        | 条件 |
| :-------------------------- | :------------ | :--- |
| id                          | chunk是否存在 | 存在 |
| 不存在                      |               |      |
| 快照是否存在                | 存在          |      |
| 不存在                      |               |      |
| 是否为clone chunk（不影响） | 是            |      |
| 不是                        |               |      |
| sn                          | 可以为任意值  |      |

##### DeleteSnapshotChunkOrCorrectSn

条件项

| 参数                        | 条件项                   | 条件            |
| :-------------------------- | :----------------------- | :-------------- |
| id                          | chunk是否存在            | 存在            |
| 不存在                      |                          |                 |
| 快照是否存在                | 存在                     |                 |
| 不存在                      |                          |                 |
| 是否为clone chunk（不影响） | 是                       |                 |
| 不是                        |                          |                 |
| correctedSn                 | 与chunk的sn对比          | sn>chunkinfo.sn |
| sn<chunkinfo.sn             |                          |                 |
| sn==chunkinfo.sn            |                          |                 |
| 与chunk的correctedSn对比    | sn>chunkinfo.correctedSn |                 |
| sn<chunkinfo.correctedSn    |                          |                 |
| sn==chunkinfo.correctedSn   |                          |                 |

##### CreateCloneChunk

条件项

| 参数                         | 条件项                   | 条件                         |
| :--------------------------- | :----------------------- | :--------------------------- |
| id                           | 指定id的chunk是否存在    | chunk存在                    |
| chunk不存在                  |                          |                              |
| chunk的快照是否存在          | 快照存在                 |                              |
| 快照不存在                   |                          |                              |
| chunk是否为clonechunk        | 是clone chunk            |                              |
| 不是clone chunk              |                          |                              |
| sn                           | 与chunk的sn对比          | sn==chunkinfo.sn             |
| sn!=chunkinfo.sn             |                          |                              |
| correctedSn                  | 与chunk的correctedSn对比 | sn==chunkinfo.correctedSn    |
| sn!=chunkinfo.correctedSn    |                          |                              |
| size                         | 与chunk size对比         | size==chunksize              |
| size!=chunksize              |                          |                              |
| location                     | 与chunk中的location对比  | location==chunkinfo.location |
| location!=chunkinfo.location |                          |                              |

##### PasteChunk

条件项

| 参数                                                         | 条件项                                                       | 条件                    |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :---------------------- |
| id                                                           | 指定id的chunk是否存在                                        | chunk存在               |
| chunk不存在                                                  |                                                              |                         |
| chunk的快照是否存在                                          | 快照存在                                                     |                         |
| 快照不存在                                                   |                                                              |                         |
| chunk是否为clonechunk                                        | 是clone chunk                                                |                         |
| 不是clone chunk                                              |                                                              |                         |
| offset 和 length（实际上就是请求写入的区域）                 | 与chunk size对比（本质就是判断写入区域是否超过chunk的大小范围） | offset+length>chunksize |
| offset+length<chunksize                                      |                                                              |                         |
| offset+length==chunksize                                     |                                                              |                         |
| 与chunk的写入状态对比（对于写clone chunk来说，还隐含一个条件：写完以后chunk是否全部被覆盖写了） | 请求写入的区域已经写过                                       |                         |
| 请求写入的区域还未写过                                       |                                                              |                         |
| 请求写入的区域部分写过，部分未写过                           |                                                              |                         |

##### GetChunkInfo

条件项

| 参数                  | 条件项                | 条件      |
| :-------------------- | :-------------------- | :-------- |
| id                    | 指定id的chunk是否存在 | chunk存在 |
| chunk不存在           |                       |           |
| chunk的快照是否存在   | 快照存在              |           |
| 快照不存在            |                       |           |
| chunk是否为clonechunk | 是clone chunk         |           |
| 不是clone chunk       |                       |           |

GetChunkInfo会获取Chunk的各项信息，因此需要在各种状态下调用GetChunkInfo，判断与预期结果是否相同。

#### 单接口功能用例集

根据上面对各接口参数条件项的分析进行单接口功能用例集设计。

| 编号 |         测试接口          |                    Given                    |                             When                             |                             Then                             | 备注                                                         |                           是否执行                           |
| :--: | :-----------------------: | :-----------------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------- | :----------------------------------------------------------: |
|  1   |        WriteChunk         |                 chunk不存在                 |                 sn=0offset+length<chunksize                  |             1.返回InvalidArgError2.不会产生chunk             | 因为chunkinfo.snapSn通过判断值是否为0来判断快照是否存在，所以初始版本必须从1开始 |                       单元测试集成测试                       |
|  2   |        WriteChunk         |                 chunk不存在                 |                 sn=1offset+length>chunksize                  | 1.返回InvalidArgError，数据未写入2.生成chunk3.chunk的sn为1，correctedSn为04.isClone==false |                                                              |                       单元测试集成测试                       |
|  3   |        WriteChunk         |                 chunk不存在                 |             sn=1offset或length未与page size对齐              | 1.返回InvalidArgError，数据未写入2.生成chunk3.chunk的sn为1，correctedSn为04.isClone==false |                                                              |                       单元测试集成测试                       |
|  4   |        WriteChunk         |                 chunk不存在                 |                 sn=1offset+length<chunksize                  | 1.返回Success，数据成功写入2.生成chunk3.chunk的sn为1，correctedSn为04.isClone==false |                                                              |                       单元测试集成测试                       |
|  5   |        WriteChunk         |                 chunk不存在                 |                 sn=2offset+length==chunksize                 | 1.返回Success，数据成功写入2.生成chunk3.chunk的sn为2，correctedSn为04.isClone==false |                                                              |                       单元测试集成测试                       |
|  6   |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域未写过 | 1.返回Success，数据写入成功2.读取写入区域数据和实际写入相同3.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  7   |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已写过 | 1.返回Success，数据写入成功，原数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  8   |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域部分被写过 | 1.返回Success，数据写入成功，写过区域数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  9   |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn>chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize | 1.返回Success，数据写入成功2.产生快照文件，快照版本为chunkinfo.sn3.chunkinfo.sn更新为sn，correctedSn不变4.原数据被cow到快照，然后被写入数据覆盖5.读取快照数据为写入前数据，读取chunk数据为写入数据 |                                                              |                       单元测试集成测试                       |
|  10  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn>chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize | 1.返回Success，数据写入成功，写入区域数据被覆盖2.读取写入区域数据和实际写入相同3.未产生快照文件4.chunkinfo.sn更新为sn，correctedSn不变 |                                                              |                       单元测试集成测试                       |
|  11  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize | 1.返回Success，数据写入成功，写过区域数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  12  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn<chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   |                                                              |                       单元测试集成测试                       |
|  13  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn<chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   |                                                              |                       单元测试集成测试                       |
|  14  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn<chunkinfo.snsn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   |                                                              |                       单元测试集成测试                       |
|  15  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snsn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   |                                                              |                       单元测试集成测试                       |
|  16  |        WriteChunk         |     chunk存在快照不存在不是clone chunk      | sn>chunkinfo.snsn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   |                                                              |                       单元测试集成测试                       |
|  17  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域未被cow过 ①chunkinfo.snapsn<chunkinfo.sn②chunkinfo.snapsn==chunkinfo.sn③chunkinfo.snapsn>chunkinfo.sn | 场景一、二：1.返回Success，数据写入成功2.原数据被cow到快照，然后被写入数据覆盖3.读取快照数据为写入前数据，读取chunk数据为写入数据4.chunk其他状态不变场景三、四：1.返回Success，数据写入成功2.原数据写入chunk但不会cow3.读取快照数据为写入前数据，读取chunk数据为写入数据4.chunk其他状态不变 | 有四种场景可能出现这个情况：1.正常创建产生快照2.历史快照未删除，这种情况下这个快照文件一定最新的快照过程中产生的；这种情况可以当做第1种情况来处理3.chunkserver重启了，重启前有个请求产生了快照，但是还没更新mepage的时候重启了，恢复日志的时候提交了前面的请求会出现这种情况,此时不会产生cow。4.chunkserver做快照的同时，raft在加载快照，follower先下载的chunk文件，下载时有一次或多次快照；follower下载完以后做日志恢复以前的日志。第1、2种场景，快照的版本小于[chunkinfo.sn](http://chunkinfo.sn/)；第3种场景，快照的版本等于[chunkinfo.sn](http://chunkinfo.sn/)；第4种场景，快照的版本等于或者大于chunkinfo.sn | 场景一、二单元测试集成测试场景三：单元测试集成测试异常测试  场景四：单元测试集成测试 |
|  18  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已被cow过 | 1.返回Success，数据写入成功2.chunk数据被覆盖，未发生cow3.读取快照数据为第一次cow的数据，读取chunk数据为写入数据4.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  19  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域部分被cow过 | 1.返回Success，数据写入成功2.chunk数据被覆盖，未被写过部分数据被cow，写过部分数据未cow3.读取快照数据，已被cow过区域为第一次cow的数据，未被cow区域为写入前chunk数据；读取chunk数据为写入数据4.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  20  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn>chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize ①chunkinfo.snapsn<[chunkinfo.sn](http://chunkinfo.sn/)②chunkinfo.snapsn==[chunkinfo.sn](http://chunkinfo.sn/)③chunkinfo.snapsn>chunkinfo.sn | 场景一：1.返回SnapshotConflictError，数据未写入2.chunk其他状态不变场景二：1.返回Success，数据写入成功2.产生快照文件，快照版本为chunkinfo.sn3.chunkinfo.sn更新为sn，correctedSn不变4.原数据被cow到快照，然后被写入数据覆盖5.读取快照数据为写入前数据，读取chunk数据为写入数据场景三：1.返回Success，数据写入成功2.不会产生新的快照，数据也不会cow到快照文件 | 以下两种场景会导致这个问题：场景一：.存在历史快照未删除，此时快照的sn肯定小于chunkinfo.sn场景二有个请求产生了快照后还未更新chunkinfo时重启了，日志回放了的这个写请求，此时快照的sn应该等于chunkinfo.sn场景三：chunkserver做快照的同时，raft在加载快照，follower先下载的chunk文件，下载时有一次或多次快照；follower下载完以后做日志恢复以前的日志。三种场景处理方式不同 | 场景一：单元测试集成测试场景二：单元测试集成测试异常测试场景三：单元测试集成测试 |
|  21  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn==chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize ①chunkinfo.snapsn<[chunkinfo.sn](http://chunkinfo.sn/)②chunkinfo.snapsn==[chunkinfo.sn](http://chunkinfo.sn/)②chunkinfo.snapsn>[chunkinfo.sn](http://chunkinfo.sn/) | 1.返回Success，数据写入成功2.chunk数据被覆盖，未发生cow3.读取快照数据为第一次cow的数据，读取chunk数据为写入数据4.chunk其他状态不变 | 以下两种场景会导致这个问题：场景一：1.存在历史快照未删除，此时快照的sn肯定小于chunkinfo.sn，但是这种场景理论上不会出现。场景二：2.有个请求产生了快照后还未更新chunkinfo时重启了，日志回放了之前的一个写请求，此时快照的sn应该等于chunkinfo.sn 场景三： chunkserver做快照的同时，raft在加载快照，follower先下载的chunk文件，下载时有一次或多次快照；follower下载完以后做日志恢复以前的日志。 三种场景处理方式一样 |                   单元测试集成测试异常测试                   |
|  22  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn>chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize | 1.返回Success，数据写入成功2.chunk数据被覆盖，未发生cow3.读取快照数据为第一次cow的数据，读取chunk数据为写入数据4.chunk.sn更新为sn，其他状态不变 | 有个请求产生了快照后还未更新chunkinfo时重启了，日志回放了的这个写请求，此时快照的sn应该等于chunkinfo.sn |                   单元测试集成测试异常测试                   |
|  23  |        WriteChunk         |      chunk存在快照存在不是clone chunk       | sn<chunkinfo.sn \|\|sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 这里只要测一组即可，这里的判断逻辑在调用开始就做了判断，前面测快照不存在时，已经将所有组合做了测试。 |                       单元测试集成测试                       |
|  24  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域未写过 | 1.返回Success，数据写入成功2.读取写入区域数据和实际写入相同3.chunkinfo.bitmap中对应的bit位被置为1，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  25  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已写过 | 1.返回Success，数据写入成功，原数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 |                                                              |                       单元测试集成测试                       |
|  26  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域部分被写过 | 1.返回Success，数据写入成功，已写过区域数据被覆盖2.读取写入区域数据和实际写入相同3.chunkinfo.bitmap中之前未写过区域对应的bit位被置为1，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  27  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snsn>chunkinfo.correctedSnoffset=0length=chunksize将chunk覆盖写一遍 | 1.返回Success，数据写入成功2.读取写入区域数据和实际写入相同3.clone chunk转换为普通chunk，chunkinfo.isClone变为false，location和bitmap都变为空，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  28  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn>chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize请求写入区域未被写过 | 1.返回Success，数据写入成功2.读取写入区域数据和实际写入相同3.chunkinfo.bitmap中对应的bit位被置为1，chunkinfo.sn被改为sn，其他状态不变 | 从快照恢复文件的时候存在这种情况；通过CreateCloneChunk产生的chunk版本为快照chunk的版本，而文件版本又是新的，为了避免产生cow，需要修改chunk的correctedSn。 |                       单元测试集成测试                       |
|  29  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snsn==chunkinfo.correctedSnoffset+length<=chunksize请求写入区域部分被写过 | 1.返回Success，数据写入成功，已写过区域数据被覆盖2.读取写入区域数据和实际写入相同3.chunkinfo.bitmap中之前未写过区域对应的bit位被置为1，其他状态不变 | 从快照恢复时通过CreateCloneChunk产生快照，然后又有数据写入会出现这种情况。 |                       单元测试集成测试                       |
|  30  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn>chunkinfo.snsn>chunkinfo.correctedSnoffset+length<=chunksize |     1.返回StatusConflictError，数据未写入2.其他状态不变      | 正常不会出现这种情况                                         |                       单元测试集成测试                       |
|  31  |        WriteChunk         |      chunk存在快照不存在是clone chunk       | sn<chunkinfo.sn \|\|sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 这里只要测一组即可                                           |                       单元测试集成测试                       |
|  32  |        PasteChunk         |                 chunk不存在                 |                   offset+length<chunksize                    |      1.返回ChunkNotExistError，数据未写入2.未产生chunk       | PasteChunk是chunkserver内部逻辑产生的操作，要求chunk必须存在。 |                       单元测试集成测试                       |
|  33  |        PasteChunk         |     chunk存在快照不存在不是clone chunk      |                   offset+length>chunksize                    |       1.返回InvalidArgError，数据未写入2.其他状态不变        |                                                              |                       单元测试集成测试                       |
|  34  |        PasteChunk         |     chunk存在快照不存在不是clone chunk      |               offset或length未与page size对齐                |       1.返回InvalidArgError，数据未写入2.其他状态不变        |                                                              |                       单元测试集成测试                       |
|  35  |        PasteChunk         |     chunk存在快照不存在不是clone chunk      |         offset+length<=chunksize请求写入区域未被写过         |         1.返回Success，实际数据未写入2.其他状态不变          | 如果不是clone chunk，PasteChunk数据不会写入                  |                       单元测试集成测试                       |
|  36  |        PasteChunk         |      chunk存在快照存在不是clone chunk       |         offset+length<=chunksize请求写入区域未被写过         |         1.返回Success，实际数据未写入2.其他状态不变          |                                                              |                       单元测试集成测试                       |
|  37  |        PasteChunk         |      chunk存在快照不存在是clone chunk       |         offset+length<=chunksize请求写入区域未被写过         | 1.返回Success，数据写入成功2.读取写入区域数据与实际写入相同3.chunkinfo.bitmap中对应的bit位置1，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  38  |        PasteChunk         |      chunk存在快照不存在是clone chunk       |         offset+length<=chunksize请求写入区域已被写过         | 1.返回Success，实际数据未写入2.读取写入区域数据与上次写入相同3.其他状态不变 |                                                              |                       单元测试集成测试                       |
|  39  |        PasteChunk         |      chunk存在快照不存在是clone chunk       |        offset+length<=chunksize请求写入区域部分被写过        | 1.返回Success，之前未写入区域被写入，写过区域未写入2.读取写入区域数据，之前未写入区域与实际写入相同，之前写过区域与之前写入内容相同3.chunkinfo.bitmap中实际写入区域对应bit位置1，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  40  |        PasteChunk         |      chunk存在快照不存在是clone chunk       |          offset=0length=chunksize将chunk覆盖写一遍           | 1.返回Success，数据写入成功2.clone chunk转为普通chunk，chunkinfo.isClone变为false，chunkinfo.location和chunkinfo.bitmap变为空，其他状态不变 |                                                              |                       单元测试集成测试                       |
|  41  |     CreateCloneChunk      |                 chunk不存在                 |        sn=0correctedsn=0size==chunksizelocation不为空        |              1.返回InvalidArgError2.chunk未产生              | sn不应该为0，如果为0，后面打快照产生的快照版本也为0。但是chunkinfo.snapSn为0会认为快照不存在。程序实现将0定义为无效的版本。 |                       单元测试集成测试                       |
|  42  |     CreateCloneChunk      |                 chunk不存在                 |        sn=1correctedsn=0size!=chunksizelocation不为空        |              1.返回InvalidArgError2.chunk未产生              | datastore中的所有chunk大小是根据配置来统一的，如果请求创建的size不等于chunksize是不允许创建的。 |                       单元测试集成测试                       |
|  43  |     CreateCloneChunk      |                 chunk不存在                 |         sn=0correctedsn=0size==chunksizelocation为空         |              1.返回InvalidArgError2.chunk未产生              | location不能为空，为空在拷贝的时候就会找不到从哪里拷贝。     |                       单元测试集成测试                       |
|  44  |     CreateCloneChunk      |                 chunk不存在                 |        sn=1correctedsn=0size==chunksizelocation不为空        | 1.返回Success，并产生chunk2.chunkinfo.isClone==true;3.chunkinfo.sn==1,chunkinfo.correctedSn==04.chunkinfo.bitmap位全为0 |                                                              |                       单元测试集成测试                       |
|  45  |     CreateCloneChunk      |                 chunk不存在                 |        sn=2correctedsn=3size==chunksizelocation不为空        | 1.返回Success，并产生chunk2.chunkinfo.isClone==true;3.chunkinfo.sn==2，chunkinfo.correctedSn==34.chunkinfo.bitmap位全为0 |                                                              |                       单元测试集成测试                       |
|  46  |     CreateCloneChunk      |           chunk存在是clone chunk            | sn==chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk未被写过 |                1.返回Success2.chunk状态未变化                | 1.日志恢复时可能出现2.cloneserver重启恢复任务时可能出现      |                       单元测试集成测试                       |
|  47  |     CreateCloneChunk      |           chunk存在是clone chunk            | sn==chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk被写过 |                1.返回Success2.chunk状态未变化                | 1.日志恢复时可能出现2.cloneserver重启恢复任务时可能出现      |                       单元测试集成测试                       |
|  48  |     CreateCloneChunk      |           chunk存在是clone chunk            | sn!=chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | 日志恢复时可能出现；CreateCloneChunk时 sn < correctedSn，然后WriteChunk更改了sn，日志恢复回放了CreateCloneChunk，此时sn<chunkinfo.sn |                       单元测试集成测试                       |
|  49  |     CreateCloneChunk      |           chunk存在是clone chunk            | sn==chunkinfo.sncorrectedsn!=chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | 正常不会出现                                                 |                       单元测试集成测试                       |
|  50  |     CreateCloneChunk      |           chunk存在是clone chunk            | sn==chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation!=chunkinfo.locationchunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | 正常不会出现                                                 |                       单元测试集成测试                       |
|  51  |     CreateCloneChunk      |          chunk存在不是clone chunk           | sn==chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation为空chunk被写过 |            1.返回InvalidArgError2.chunk状态无变化            | 正常不会出现                                                 |                       单元测试集成测试                       |
|  52  |     CreateCloneChunk      |          chunk存在不是clone chunk           | sn==chunkinfo.sncorrectedsn==chunkinfo.correctedsnsize==chunksizelocation不为空chunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | chunk被编写过一遍了，日志恢复到之前的操作可能出现            |                       单元测试集成测试                       |
|  53  |        DeleteChunk        |                 chunk不存在                 |                         参数为任意值                         |                        1.返回Success                         |                                                              |                       单元测试集成测试                       |
|  54  |        DeleteChunk        |     chunk存在快照不存在不是clone chunk      |                       sn==chunkinfo.sn                       |                  1.返回Success2.chunk被删除                  |                                                              |                       单元测试集成测试                       |
|  55  |        DeleteChunk        |     chunk存在快照不存在不是clone chunk      |                       sn>chunkinfo.sn                        |                  1.返回Success2.chunk被删除                  |                                                              |                       单元测试集成测试                       |
|  56  |        DeleteChunk        |     chunk存在快照不存在不是clone chunk      |                       sn<chunkinfo.sn                        |          1.返回BackwardRequestError2.chunk未被删除           |                                                              |                       单元测试集成测试                       |
|  57  |        DeleteChunk        |      chunk存在快照存在不是clone chunk       |                       sn==chunkinfo.sn                       |            1.返回Success2.删除快照3.删除chunk文件            |                                                              |                       单元测试集成测试                       |
|  58  |        DeleteChunk        |      chunk存在快照不存在是clone chunk       |                       sn==chunkinfo.sn                       |                  1.返回Success2.chunk被删除                  |                                                              |                       单元测试集成测试                       |
|  59  | DeleteSnapshotOrCorrectSn |                 chunk不存在                 |                     correctedsn为任意值                      |                         返回Success                          |                                                              |                       单元测试集成测试                       |
|  60  | DeleteSnapshotOrCorrectSn |     chunk存在快照不存在不是clone chunk      |       correctedSn>sncorrectedSn>chunkinfo.correctedSn        |    1.返回Success2.chunkinfo.correctedSn被改为correctedSn     | 快照不存在的情况下，是不是clone chunk处理方式是一样的，可以交叉组合 |                       单元测试集成测试                       |
|  61  | DeleteSnapshotOrCorrectSn |     chunk存在快照不存在不是clone chunk      |       correctedSn>sncorrectedSn==chunkinfo.correctedSn       |                 1.返回Success2.chunk状态不变                 |                                                              |                       单元测试集成测试                       |
|  62  | DeleteSnapshotOrCorrectSn |     chunk存在快照不存在不是clone chunk      |       correctedSn>sncorrectedSn<chunkinfo.correctedSn        |          1.返回BackwardRequestError2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  63  | DeleteSnapshotOrCorrectSn |     chunk存在快照不存在不是clone chunk      |       correctedSn<sncorrectedSn==chunkinfo.correctedSn       |          1.返回BackwardRequestError2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  64  | DeleteSnapshotOrCorrectSn |     chunk存在快照不存在不是clone chunk      |       correctedSn<sncorrectedSn<chunkinfo.correctedSn        |          1.返回BackwardRequestError2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  65  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn>sncorrectedSn>chunkinfo.correctedSnchunk.sn>snap.sn | 1.返回Success，快照被删除2.chunkinfo.correctedSn被改为correctedSn |                                                              |                       单元测试集成测试                       |
|  66  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn>sncorrectedSn>chunkinfo.correctedSn[chunk.sn](http://chunk.sn/)<=[snap.sn](http://snap.sn/) | 1.返回Success，快照不删除2.chunkinfo.correctedSn被改为correctedSn |                                                              |                       单元测试集成测试                       |
|  67  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn==sncorrectedSn>chunkinfo.correctedSn[chunk.sn](http://chunk.sn/)>[snap.sn](http://snap.sn/) |         1.返回Success，快照被删除2.chunk其他状态不变         |                                                              |                       单元测试集成测试                       |
|  68  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn==sncorrectedSn>chunkinfo.correctedSn[chunk.sn](http://chunk.sn/)<=[snap.sn](http://snap.sn/) |         1.返回Success，快照不删除2.chunk其他状态不变         |                                                              |                       单元测试集成测试                       |
|  69  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       |       correctedSn<sncorrectedSn>chunkinfo.correctedSn        |          1.返回BackwardRequestError2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  70  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn==sncorrectedSn==chunkinfo.correctedSn[chunk.sn](http://chunk.sn/)>[snap.sn](http://snap.sn/) |           1.返回Success，快照被删除2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  71  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       | correctedSn==sncorrectedSn==chunkinfo.correctedSn[chunk.sn](http://chunk.sn/)<=[snap.sn](http://snap.sn/) |           1.返回Success，快照不删除2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  72  | DeleteSnapshotOrCorrectSn |      chunk存在快照存在不是clone chunk       |       correctedSn==sncorrectedSn<chunkinfo.correctedSn       |          1.返回BackwardRequestError2.chunk状态不变           |                                                              |                       单元测试集成测试                       |
|  73  | DeleteSnapshotOrCorrectSn |      chunk存在快照不存在是clone chunk       |                     correctedSn为任意值                      |                  1.返回StatusConflictError                   | 理论上不应该出现这种调用                                     |                       单元测试集成测试                       |
|  74  |         ReadChunk         |                 chunk不存在                 |                          sn为任意值                          |                    返回ChunkNotExistError                    |                                                              |                       单元测试集成测试                       |
|  75  |         ReadChunk         |     chunk存在快照不存在不是clone chunk      |           sn==chunkinfo.snoffset+length>chunksize            |                     返回InvalidArgError                      |                                                              |                       单元测试集成测试                       |
|  76  |         ReadChunk         |     chunk存在快照不存在不是clone chunk      | sn==[chunkinfo.sn](http://chunkinfo.sn/)offset或length未与page size 对齐 |                     返回InvalidArgError                      |                                                              |                       单元测试集成测试                       |
|  77  |         ReadChunk         |     chunk存在快照不存在不是clone chunk      |  sn>chunkinfo.snoffset+length==chunksize请求读取区域未写过   |       1.返回Success，数据读取成功2.读到的数据无需验证        |                                                              |                       单元测试集成测试                       |
|  78  |         ReadChunk         |     chunk存在快照不存在不是clone chunk      |   sn<chunkinfo.snoffset+length<chunksize请求读取区域已写过   | 1.返回Success，数据读取成功2.读到的数据与上一次写入的数据内容相等 |                                                              |                       单元测试集成测试                       |
|  79  |         ReadChunk         |     chunk存在快照不存在不是clone chunk      |    sn==chunkinfo.snoffset+length<chunksize部分区域已写过     | 1.返回Success，数据读取成功2.已写过区域读出来后与上一次写入的数据内容相等，未写过区域数据读出来无需验证 |                                                              |                       单元测试集成测试                       |
|  80  |         ReadChunk         |      chunk存在快照存在不是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域未写过  |       1.返回Success，数据读取成功2.读到的数据无需验证        |                                                              |                       单元测试集成测试                       |
|  81  |         ReadChunk         |      chunk存在快照存在不是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域已写过  | 1.返回Success，数据读取成功2.读到的数据与上一次写入的数据内容相等 |                                                              |                       单元测试集成测试                       |
|  82  |         ReadChunk         |      chunk存在快照存在不是clone chunk       |    sn==chunkinfo.snoffset+length<=chunksize部分区域已写过    | 1.返回Success，数据读取成功2.已写过区域读出来后与上一次写入的数据内容相等，未写过区域数据读出来无需验证 |                                                              |                       单元测试集成测试                       |
|  83  |         ReadChunk         |      chunk存在快照不存在是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域未写过  |                 1.返回PageNerverWrittenError                 | 不允许读                                                     |                       单元测试集成测试                       |
|  84  |         ReadChunk         |      chunk存在快照不存在是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域已写过  | 1.返回Success，数据读取成功2.读到的数据与上一次写入的数据内容相等 |                                                              |                       单元测试集成测试                       |
|  85  |         ReadChunk         |      chunk存在快照不存在是clone chunk       |    sn==chunkinfo.snoffset+length<=chunksize部分区域已写过    |                   1.PageNerverWrittenError                   | 不允许读                                                     |                       单元测试集成测试                       |
|  86  |     ReadSnapshotChunk     |                 chunk不存在                 |                         参数为任意值                         |                    返回ChunkNotExistError                    |                                                              |                       单元测试集成测试                       |
|  87  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      |                   offset+length>chunksize                    |                     返回InvalidArgError                      |                                                              |                       单元测试集成测试                       |
|  88  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      |               offset或length未与page size对齐                |                     返回InvalidArgError                      |                                                              |                       单元测试集成测试                       |
|  89  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      |           sn!=chunkinfo.snoffset+length<=chunksize           |                    返回ChunkNotExistError                    |                                                              |                       单元测试集成测试                       |
|  90  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域未写过  |       1.返回Success，数据读取成功2.读到的数据无需验证        |                                                              |                       单元测试集成测试                       |
|  91  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域已写过  | 1.返回Success，数据读取成功2.读到的数据与上一次写入的数据内容相等 |                                                              |                       单元测试集成测试                       |
|  92  |     ReadSnapshotChunk     |     chunk存在快照不存在不是clone chunk      | sn==chunkinfo.snoffset+length<=chunksize请求读取区域部分已写过 | 1.返回Success，数据读取成功2.已写过区域读出来后与上一次写入的数据内容相等，未写过区域数据读出来无需验证 |                                                              |                       单元测试集成测试                       |
|  93  |     ReadSnapshotChunk     |      chunk存在快照存在不是clone chunk       | sn!=chunkinfo.snsn!=chunkinfo.snapSnoffset+length<=chunksize |                    返回ChunkNotExistError                    |                                                              |                       单元测试集成测试                       |
|  94  |     ReadSnapshotChunk     |      chunk存在快照存在不是clone chunk       | sn==chunkinfo.snsn!=chunkinfo.snapSnoffset+length<=chunksize请求读取区域部分已写过 | 1.返回Success，数据读取成功2.已写过区域读出来后与上一次写入的数据内容相等，未写过区域数据读出来无需验证 | 正常不会出现；不过这个接口其实本意上可以理解为读取指定版本的chunk |                       单元测试集成测试                       |
|  95  |     ReadSnapshotChunk     |      chunk存在快照存在不是clone chunk       | sn!=chunkinfo.snsn==chunkinfo.snapSnoffset+length<=chunksize请求读取区域未cow过 | 1.返回Success，数据读取成功2.读到的数据等于chunk文件中的数据 |                                                              |                       单元测试集成测试                       |
|  96  |     ReadSnapshotChunk     |      chunk存在快照存在不是clone chunk       | sn!=chunkinfo.snsn==chunkinfo.snapSnoffset+length<=chunksize请求读取区域已cow过 | 1.返回Success，数据读取成功2.读取到的数据等于cow到快照文件的数据 |                                                              |                       单元测试集成测试                       |
|  97  |     ReadSnapshotChunk     |      chunk存在快照存在不是clone chunk       | sn!=chunkInfo.snsn==chunkinfo.snapSnoffset+length<=chunksize请求读取区域部分已cow过 | 1.返回Success，数据读取成功2.已cow过区域读出来后与cow到快照文件的数据内容相等，未写过区域数据读出来验证等于chunk文件中的数据 |                                                              |                       单元测试集成测试                       |
|  98  |     ReadSnapshotChunk     |      chunk存在快照不存在是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域未写过  |                 1.返回PageNerverWrittenError                 | 不允许读                                                     |                       单元测试集成测试                       |
|  99  |     ReadSnapshotChunk     |      chunk存在快照不存在是clone chunk       |  sn==chunkinfo.snoffset+length<=chunksize请求读取区域已写过  | 1.返回Success，数据读取成功2.读到的数据与上一次写入的数据内容相等 |                                                              |                       单元测试集成测试                       |
| 100  |     ReadSnapshotChunk     |      chunk存在快照不存在是clone chunk       | sn==chunkinfo.snoffset+length<=chunksize请求读取区域部分已写过 |                 1.返回PageNerverWrittenError                 | 不允许读                                                     |                       单元测试集成测试                       |
| 101  |       GetChunkInfo        |                 chunk不存在                 |                                                              |                    返回ChunkNotExistError                    |                                                              |                       单元测试集成测试                       |
| 102  |       GetChunkInfo        |     chunk存在快照不存在不是clone chunk      |                                                              | 1.chunk的sn和correctedsn符合预期2.chunkinfo.snapSn=03.chunkinfo.isClone==false4.chunkinfo.bitmap和chunkinfo.location为空 |                                                              |                       单元测试集成测试                       |
| 103  |       GetChunkInfo        | chunk存在快照存在快照版本为a不是clone chunk |                                                              | 1.chunk的sn和correctedsn符合预期2.chunkinfo.snapSn=a3.chunkinfo.isClone==false4.chunkinfo.bitmap和chunkinfo.location为空 |                                                              |                       单元测试集成测试                       |
| 104  |       GetChunkInfo        |      chunk存在快照不存在是clone chunk       |                                                              | 1.chunk的sn和correctedsn符合预期2.chunkinfo.snapSn=03.chunkinfo.isClone==true4.chunkinfo.location为CreateCloneChunk时的location5.chunkinfo.bitmap中，写过的page对应位为1，其余为0 |                                                              |                       单元测试集成测试                       |

#### 场景设计

##### 普通使用

测试普通盘的使用场景，此时没有快照，没有克隆。

新生成的盘一开始没有chunk文件，只有第一次写会触发创建chunk文件。

**场景一：**新建的文件，Chunk文件不存在

- 操作：chunk文件不存在，请求ReadChunk
  预期：返回ChunkNotExistError

- 操作：chunk文件不存在，请求GetChunkInfo
  预期：返回ChunkNotExistError
- 操作：chunk文件不存在，请求DeleteChunk
  预期：返回Success

**场景二：**通过WriteChunk产生chunk文件后操作

- 操作：chunk文件不存在，请求WriteChunk；sn=1,offset=0,length=4kb
  预期：数据写入成功，产生Chunk文件
- 操作：chunk产生后，请求GetChunkInfo
  预期：可以获取到chunk的信息，且信息都符合预期

- 操作：chunk产生后，请求ReadChunk；offset=0,length=4kb
  预期：读取数据成功，读取出来的数据与写入的数据比较相等
- 操作：请求ReadChunk；offset=16mb-4kb,length=4kb
  预期：读取数据成功，读出来的数据无需验证
- 操作：chunk文件存在，请求WriteChunk；sn=1,offset=0,length=4kb
  预期：数据写入成功，产生Chunk文件
- 操作：chunk文件存在，请求ReadChunk；offset=0,length=12kb
  预期：读取数据成功，已写入区域数据与写入的数据比较相等，为写过区域数据无需验证
- 操作：chunk文件存在，请求WriteChunk；sn=1,offset=4kb,length=4kb
  预期：数据写入成功，产生Chunk文件
- 操作：chunk文件存在，请求ReadChunk；offset=0,length=12kb
  预期：读取数据成功，0-4KB数据与第二次写入相等，4kb-8kb与上一次写入相等
- 操作：chunk文件存在，请求WriteChunk；sn=1,offset=4kb,length=8kb
  预期：数据写入成功，产生Chunk文件
- 操作：chunk文件存在，请求ReadChunk；offset=0,length=12kb
  预期：读取数据成功，0-4KB数据与第二次写入相等，4kb-12kb与上一次写入相等

**场景三：**用户删除文件

- 操作：chunk产生后，请求DeleteChunk
  预期：返回Success，文件删除成功；请求GetChunkInfo返回ChunkNotExistError

##### 快照

模拟用户创建快照的过程，假设当前文件初始版本为1，文件有两个chunk，分别为chunk1，chunk2.

**构造初始环境**

写chunk1产生chunk1，chunk1版本为1，chunk2开始不存在。

- 操作：请求WriteChunk写chunk1；sn=1，offset=0，length=12kb
  预期：产生chunk文件，数据写入成功

**场景一：第一次给文件打快照**

- 操作：请求WriteChunk写chunk1；sn=2，offset=4kb，length=4kb
  预期：数据写入成功，产生快照文件
- 操作：重复写入同一区域，用于验证不会重复cow
  预期：ReadSnapshotChunk时读出来的数据等于初始构造写入的数据。
- 操作：请求ReadSnapshotChunk，读取chunk1的快照；sn=1，offset=0，length=12kb
  预期：成功读取数据，且读到的数据和初始构造时写入的数据相等
- 操作：请求WriteChunk写chunk1；sn=2，offset=0kb，length=4kb
  预期：数据写入成功，产生cow，chunk数据被覆盖
- 操作：请求WriteChunk写chunk1；sn=2，offset=4kb，length=12kb
  预期：数据写入成功，未被写过区域产生cow，chunk数据被覆盖
- 操作：请求GetChunkInfo，获取的chunk1信息
  预期：chunk版本为2，快照版本为1
- 操作：请求ReadChunk，读取chunk1的数据；sn=2，offset=0，length=12kb
  预期：成功读取数据，读到的数据符合预期
- 操作：请求ReadSnapshotChunk，读取chunk1的快照；sn=1，offset=0，length=12kb
  预期：成功读取数据，且读到的数据和初始构造时写入的数据相等
- 操作：请求ReadSnapshotChunk，读取chunk2的快照
  预期：返回ChunkNotExistError

**场景二：第一次快照结束，删除快照**

- 操作：请求DeleteSnapshotChunkOrCorrectSn删chunk1快照；correctedSn=2
  预期：快照文件删除成功，通过GetChunkInfo获取chunk1信息，文件版本为2，correctedSn为0，快照版本为0
- 操作：请求DeleteSnapshotChunkOrCorrectSn删chunk2快照，correctedSn为2
  预期：接口调用返回成功
- 操作：请求WriteChunk写chunk2；sn=2，offset=0，length=8kb
  预期：数据写入成功，通过GetChunkInfo获取chunk2信息，文件版本为2，correctedSn为0，快照版本为0

**场景三：第二次打快照**

- 操作：请求WriteChunk写chunk1；sn=3，offset=0，length=8kb
  预期：数据写入成功，产生快照文件2，通过GetChunkInfo获取chunk1信息，文件版本为3，correctedSn为0，快照版本为2
- 操作：请求ReadSnapshotChunk，读取chunk1的快照；sn=2，offset=0，length=12kb
  预期：成功读取数据，且读到的数据与版本3写入之前写入的数据相等
- 操作：请求ReadSnapshotChunk，读取chunk2的快照；sn=2，offset=0，length=8kb
  预期：成功读取数据，且读到的数据与版本2的数据相同
- 操作：请求DeleteChunk，删除chunk1文件
  预期：返回SnapshotExistError，本地快照存在的情况下不允许删除文件

**场景四：第二次快照结束，删除快照**

- 操作：请求DeleteSnapshotChunkOrCorrectSn删chunk1快照，correctedSn为3
  预期：快照文件删除成功，通过GetChunkInfo获取chunk1信息，文件版本为3，correctedSn为0，快照版本为0
- 操作：请求DeleteSnapshotChunkOrCorrectSn删chunk2快照，correctedSn为3
  预期：接口调用返回成功，通过GetChunkInfo获取chunk2信息，文件版本为2，correctedSn为3，快照版本为0
- 操作：请求WriteChunk写chunk2；sn=3，offset=0，length=4kb
  预期：数据写入成功，通过GetChunkInfo获取chunk2信息，文件版本为3，correctedSn为3，快照版本为0
- 操作：请求WriteChunk写chunk2；sn=3，offset=0，length=4kb
  预期：数据写入成功，通过GetChunkInfo获取chunk2信息，chunk信息不变

**场景五：用户删除文件**

- 操作：请求DeleteChunk删除chunk1和chunk2
  预期：返回Success，文件删除成功，请求GetChunkInfo返回ChunkNotExistError

##### 克隆

模拟克隆过程

**场景一：创建克隆文件**

- 操作：通过CreateCloneChunk创建chunk1；sn=1，correctedsn=0，size=chunksize，
  预期：创建克隆文件成功，通过GetChunkInfo获取chunk信息，isClone标志为true，bitmap不为空，初始所有bit状态都为0
- 操作：再次通过CreateCloneChunk创建clone文件
  预期：返回成功，通过GetChunkInfo获取chunk信息，信息同上
- 操作：通过CreateCloneChunk创建chunk2；sn=1，correctedsn=0，size=chunksize，
  预期：创建克隆文件成功，通过GetChunkInfo获取chunk信息，isClone标志为true，bitmap不为空，初始所有bit状态都为0

**场景二：恢复克隆文件**

- 操作：通过WriteChunk写数据到clone文件；sn=1，offset=0，length=8kb
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，bitmap中被写过的page对应的bit状态为1，其余状态为0
- 操作：通过ReadChunk读取数据；offset=0，length=8kb
  预期：等于前面写入的数据
- 操作：通过PasteChunk写新数据到clone文件；sn=1，offset=0，length=8kb
  预期：返回成功，实际数据未写入
- 操作：通过ReadChunk读取数据；offset=0，length=8kb
  预期：等于第一次写入数据
- 操作：通过WriteChunk写数据到clone文件；sn=1，offset=4kb，length=8kb
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，bitmap中被写过的page对应的bit状态为1，其余状态为0
- 操作：通过ReadChunk读取数据；offset=0，length=12kb
  预期：0-4KB等于第二次写入的数据，4kb-12kb等于前面写入的数据
- 操作：通过PasteChunk写数据到clone文件，写入区域有部分与WriteChunk写入区域重叠，部分不重叠；offset=4kb，length=8kb
  预期：数据paste成功，通过GetChunkInfo获取chunk信息，bitmap中被写过的page对应的bit状态为1，其余状态为0
- 操作：通过ReadChunk读取前面WriteChunk和PasteChunk写入的区域；offset=0，length=12kb
  预期：重叠部分的数据等于WriteChunk写入的区域，不重叠的区域等于WriteChunk和PasteChunk各自写入的数据

**场景三：clone文件遍写后转换为普通chunk文件**

- 操作：通过PasteChunk遍写clone文件的所有pages
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，isClone标志为false，bitmap为空

**场景四：删除文件**

- 操作：通过DeleteChunk删除chunk1
  预期：删除成功

- 操作：通过DeleteChunk删除chunk2
  预期：删除成功

##### 恢复

模拟恢复过程，恢复原理基本和克隆是一样的，区别在于克隆的初始sn为1，correctedsn为0；

恢复的话correctedsn可以不为0且correctedsn>sn。

**场景一：创建克隆文件**

- 操作：通过CreateCloneChunk创建chunk1；sn=2，correctedsn=3，size=chunksize，
  预期：创建克隆文件成功，通过GetChunkInfo获取chunk信息，isClone标志为true，bitmap不为空，初始所有bit状态都为0
- 操作：再次通过CreateCloneChunk创建clone文件
  预期：返回成功，通过GetChunkInfo获取chunk信息，信息同上

**场景二：恢复克隆文件**

- 操作：通过PasteChunk写数据到clone文件；sn=3，offset=0，length=8kb
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，bitmap中被写过的page对应的bit状态为1，其余状态为0
- 操作：通过ReadChunk读取数据；offset=0，length=8kb
  预期：等于前面写入的数据
- 操作：通过WriteChunk写数据到clone文件；sn=3，offset=0，length=8kb
  预期：返回成功，覆盖前面写入数据。
- 操作：通过ReadChunk读取数据；offset=0，length=8kb
  预期：等于前面写入数据
- 操作：通过PasteChunk写数据到clone文件；sn=3，offset=4kb，length=8kb
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，bitmap中被写过的page对应的bit状态为1，其余状态为0
- 操作：通过ReadChunk读取数据；offset=0，length=12kb
  预期：0-8KB等于第二次写入的数据，8kb-12kb等于前面写入的数据

**场景三：clone文件遍写后转换为普通chunk文件**

- 操作：通过WriteChunk遍写clone文件的所有pages
  预期：数据写入成功，通过GetChunkInfo获取chunk信息，isClone标志为false，bitmap为空

##### 重启恢复

copyset在重启的时候会恢复raft 日志中的操作，这些操作重启前可能已经执行了，也可能还没执行；日志中记录的操作主要是更改操作（可能存在读操作，但是读操作日志恢复的时候可以不用考虑）：

包括WriteChunk、PasteChunk、CreateCloneChunk、DeleteChunk、DeleteSnapshotChunk。

日志恢复必须保证操作的幂等性，对于同一个Chunk来说，日志中的操作是顺序执行的，如果有操作未执行过，其后面的操作也一定未执行过，因此只要保证已经执行过的操作恢复后是幂等的，那么未执行过的操作恢复就一定是幂等的。

这里的幂等并不是指某一操作已经执行过就不会再执行了，而是指在日志全部恢复完以后，所有chunk的状态与重启前仍然一致。

因此这里要分析每一种操作的处理方式在恢复场景下是否能保证幂等性。

这里分析首先考虑排除几种场景，减少分析复杂度：

1.重启前chunk被delete了，这种情况下中间无论怎么操作，最终都能被delete（只有快照存在情况下不会被delete，所以delete之前chunk一定不存在快照）；我们下面的分析都认为chunk在重启前没有被delete。

2.前面提到过只要执行过的操作再次恢复可以保证幂等性，未执行过的操作恢复一定是幂等的；所以我们考虑的操作重启前都已经执行过。

3.这里只考虑正常重启的情况，异常重启的情况在异常测试中考虑；也就是说操作都是完整执行的，不会执行到一半重启。

4.基于上面三点，可以过滤掉一些之前一定没执行过的条件：①请求写入的区域未写过或部分未写过；②快照中数据未被cow过或部分被cow过。③sn>chunkinfo.sn

5.此外还能过滤掉一些条件，例如参数不符合要求，无论是否恢复都会被拒绝的操作。

依据上面过滤条件，我们对上面表格中的测试用例进行筛选。

 主要要分析的列出的这些用例在恢复场景下是如何产生的，以及是否如用例中所示产生预期的结果，并且这种处理方式不会对幂等性造成影响。

| 编号 |         测试接口          |               Given                |                             When                             |                             Then                             | 备注                                                         | 是否执行 |
| :--: | :-----------------------: | :--------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------- | :------: |
|  6   |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已写过 | 1.返回Success，数据写入成功，原数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 | sn==chunkinfo.sn的情况下，说明重启前一定有相同版本的写操作写过这个chunk；快照不存在可能是已经删除了也可能是没有快照，此时可以不需要关心快照；对于同一版本时期内的写入是保证幂等的。 |          |
|  10  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn==chunkinfo.correctedSnoffset+length<=chunksize | 1.返回Success，数据写入成功，写过区域数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 | sn==chunkinfo.correctedSn说明之前一定掉过DeleteSnapshot接口，DeleteSnapshot时chunk是不存在快照的；DeleteSnapshot之后有版本为sn的写操作写过该chunk；此时数据写入可保证幂等性。 |          |
|  11  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn<[chunkinfo.sn](http://chunkinfo.sn/)sn>chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 请求可能是个慢请求，这种场景比较少；一般来说是chunk打过快照并且打完快照后写过数据，然后恢复了快照前的写请求，那么此时这个请求之前一定已经执行过了，可以不执行。 |          |
|  12  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn<[chunkinfo.sn](http://chunkinfo.sn/)sn==chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 可能是慢请求，都不做考虑；正常情况下，chunk可能经过DeleteSnapshot修改了correctednSn，然后写入数据，再然后用户打了快照并写数据；此时重启恢复了DeleteSnapshot之后，打快照之前的写入操作。 |          |
|  13  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn<[chunkinfo.sn](http://chunkinfo.sn/)sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 这个请求要么是慢请求，要么中间经过快照，重启恢复了快照前的请求，此时该操作一定已经执行过，可以不执行。 |          |
|  14  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 可能是慢请求；正常情况下，重启前用户调了DeleteSnapshot更改了correctedSn，调用前有写请求写入数据；重启后恢复了DeleteSnapshot前的写操作，那么这个操作之前一定已经执行成功过，可以不用在执行。 |          |
|  15  |        WriteChunk         | chunk存在快照不存在不是clone chunk | sn>[chunkinfo.sn](http://chunkinfo.sn/)sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 这种一定是慢请求；假如这个请求成功执行过，sn应该等于chunkinfo.sn。 |          |
|  17  |        WriteChunk         |  chunk存在快照存在不是clone chunk  | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已被cow过 | 1.返回Success，数据写入成功2.chunk数据被覆盖，未发生cow3.读取快照数据为第一次cow的数据，读取chunk数据为写入数据4.chunk其他状态不变 | 重启前，用户打了快照，打完快照后有新的数据写入；重启的时候快照还未删除；重启后恢复了快照后的请求。 |          |
|  22  |        WriteChunk         |  chunk存在快照存在不是clone chunk  | sn<[chunkinfo.sn](http://chunkinfo.sn/) \|\|sn<chunkinfo.correctedSnoffset+length<=chunksize |  1.返回BackwardRequestError，数据未写入2.chunk其他状态不变   | 可能是慢请求；正常情况下，重启前用户打了快照并有新数据写入；重启时快照未删除；重启后恢复了快照前的写请求。 |          |
|  24  |        WriteChunk         |  chunk存在快照不存在是clone chunk  | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn>chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已写过 | 1.返回Success，数据写入成功，原数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 | 重启前调用了CreateCloneChunk然后写入数据；重启后恢复了写请求。 |          |
|      |        WriteChunk         |  chunk存在快照不存在是clone chunk  | sn==[chunkinfo.sn](http://chunkinfo.sn/)sn==chunkinfo.correctedSnoffset+length<=chunksize请求写入区域已写过 | 1.返回Success，数据写入成功，原数据被覆盖2.读取写入区域数据和实际写入相同3.chunk其他状态不变 | 重启前调用了CreateCloneChunk然后写入数据,创建时correctedSn>sn；重启后恢复了写请求。 |          |
|  33  |        PasteChunk         | chunk存在快照不存在不是clone chunk |         offset+length<=chunksize请求写入区域未被写过         |         1.返回Success，实际数据未写入2.其他状态不变          | 可能是用户的错误调用；但是正常来说应该是重启前clone chunk被覆盖写过一遍，重启后恢复了被覆盖写之前的Paste操作；还一种正常情况在用户从远端拷贝数据时，clone chunk被覆盖写了（拷贝时不会阻塞正常写入）。这种情况数据不会被覆盖，不影响幂等性。 |          |
|  34  |        PasteChunk         |  chunk存在快照存在不是clone chunk  |         offset+length<=chunksize请求写入区域未被写过         |         1.返回Success，实际数据未写入2.其他状态不变          | 可能是用户错误调用；正常来说可能是重启前clone chunk被覆盖写过一遍，并且之后打了快照并写入新的数据产生了快照；或者从远端拷贝数据时，clone chunk被覆盖写并产生了快照。 |          |
|  36  |        PasteChunk         |  chunk存在快照不存在是clone chunk  |         offset+length<=chunksize请求写入区域已被写过         | 1.返回Success，实际数据未写入2.读取写入区域数据与上次写入相同3.其他状态不变 | 重启前调用了CreateCloneChunk，然后paste了数据；重启后恢复了paste操作。不影响幂等性 |          |
|  44  |     CreateCloneChunk      |       chunk存在是clone chunk       | sn==[chunkinfo.sn](http://chunkinfo.sn/)correctedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk未被写过 |                1.返回Success2.chunk状态未变化                | 重启前调了CreateCloneChunk，后面没有其他操作，然后发生了重启。不影响幂等性。 |          |
|  45  |     CreateCloneChunk      |       chunk存在是clone chunk       | sn==[chunkinfo.sn](http://chunkinfo.sn/)correctedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk被写过 |                1.返回Success2.chunk状态未变化                | 重启前调用了CreateCloneChunk，并且后续有数据写入；重启后恢复了CreateCloneChunk操作。不影响幂等性。 |          |
|  46  |     CreateCloneChunk      |       chunk存在是clone chunk       | sn！=[chunkinfo.sn](http://chunkinfo.sn/)correctedsn==chunkinfo.correctedsnsize==chunksizelocation==chunkinfo.locationchunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | 日志恢复时可能出现；CreateCloneChunk时 sn < correctedSn，然后WriteChunk更改了sn，日志恢复回放了CreateCloneChunk，此时sn<[chunkinfo.sn](http://chunkinfo.sn/);不影响幂等性 |          |
|  50  |     CreateCloneChunk      |      chunk存在不是clone chunk      | sn==[chunkinfo.sn](http://chunkinfo.sn/)correctedsn==chunkinfo.correctedsnsize==chunksizelocation不为空chunk被写过 |          1.返回ChunkConflictError2.chunk状态无变化           | 重启前调用了CreateCloneChunk，然后将数据覆盖写了一遍；重启后恢复了CreateCloneChunk操作。不影响幂等性。 |          |
|  52  |        DeleteChunk        | chunk存在快照不存在不是clone chunk |           sn==[chunkinfo.sn](http://chunkinfo.sn/)           |                  1.返回Success2.chunk被删除                  | 重启前这一步要么没调用；要么调用了，日志恢复了前面的操作又产生了chunk文件。不影响幂等性 |          |
|  53  |        DeleteChunk        | chunk存在快照不存在不是clone chunk |           sn>[chunkinfo.sn](http://chunkinfo.sn/)            |                  1.返回Success2.chunk被删除                  | 同上。不影响幂等性。                                         |          |
|  54  |        DeleteChunk        | chunk存在快照不存在不是clone chunk |           sn<[chunkinfo.sn](http://chunkinfo.sn/)            |          1.返回BackwardRequestError2.chunk未被删除           | 正常不会出现这种情况。除非删除以后又有新的版本的数据写入，正常不可能发生。 |          |
|  55  |        DeleteChunk        |  chunk存在快照存在不是clone chunk  |           sn==[chunkinfo.sn](http://chunkinfo.sn/)           |            1.返回success2.快照被删除3.chunk被删除            | 重启前这一步要么没调用；要么调用了，日志恢复了前面的操作又产生了chunk文件。不影响幂等性 |          |
|  56  |        DeleteChunk        |  chunk存在快照不存在是clone chunk  |           sn==[chunkinfo.sn](http://chunkinfo.sn/)           |                  1.返回Success2.chunk被删除                  | 重启前这一步要么没调用；要么调用了，日志恢复了前面的操作又产生了chunk文件；该chunk通过CreateCloneChunk产生。不影响幂等性 |          |
|  58  | DeleteSnapshotOrCorrectSn | chunk存在快照不存在不是clone chunk |       correctedSn>sncorrectedSn>chunkinfo.correctedSn        |    1.返回Success2.chunkinfo.correctedSn被改为correctedSn     | 重启前一定未执行过，不然correctedSn==chunkinfo.correctedSn。可以不考虑 |          |
|  59  | DeleteSnapshotOrCorrectSn | chunk存在快照不存在不是clone chunk |       correctedSn>sncorrectedSn==chunkinfo.correctedSn       |                 1.返回Success2.chunk状态不变                 | 重启前调了DeleteSnapshot，调用时chunk不存在快照，调用后没有新的数据写入；重启后恢复了DeleteSnapshot操作。不影响幂等性。 |          |
|  60  | DeleteSnapshotOrCorrectSn | chunk存在快照不存在不是clone chunk |       correctedSn>sncorrectedSn<chunkinfo.correctedSn        |                 1.返回Success2.chunk状态不变                 | 重启前连续调了多次的DeleteSnapshot，每次调用时chunk都不存在快照，且多次调用之间没有新的数据写入；重启后恢复了前面一点的DeleteSnapshot请求。不影响幂等性。 |          |
|  61  | DeleteSnapshotOrCorrectSn | chunk存在快照不存在不是clone chunk |       correctedSn<sncorrectedSn==chunkinfo.correctedSn       |                 1.返回Success2.chunk状态不变                 | 重启前调了DeleteSnapshot，调用时chunk不存在快照，调用后有新的快照，且快照后有新的数据写入，该写请求应该产生了快照，然后删除了快照。重启后恢复了一开始的DeleteSnapshot请求。不影响幂等性。 |          |
|  62  | DeleteSnapshotOrCorrectSn | chunk存在快照不存在不是clone chunk |       correctedSn<sncorrectedSn<chunkinfo.correctedSn        |                 1.返回Success2.chunk状态不变                 | 重启前调了DeleteSnapshot，调用后有新的快照，且快照后有新的数据写入，该写请求应该产生了快照，然后删除了快照。重启后恢复了一开始的DeleteSnapshot请求。不影响幂等性。 |          |
|  63  | DeleteSnapshotOrCorrectSn |  chunk存在快照存在不是clone chunk  |       correctedSn>sncorrectedSn>chunkinfo.correctedSn        | 1.返回Success，快照被删除2.chunkinfo.correctedSn被改为correctedSn | 正常不会出现这种场景。                                       |          |
|  64  | DeleteSnapshotOrCorrectSn |  chunk存在快照存在不是clone chunk  |       correctedSn==sncorrectedSn>chunkinfo.correctedSn       |         1.返回Success，快照被删除2.chunk其他状态不变         | 重启前该操作一定未执行过，否则快照会被删除。                 |          |
|  65  | DeleteSnapshotOrCorrectSn |  chunk存在快照存在不是clone chunk  |       correctedSn<sncorrectedSn>chunkinfo.correctedSn        |                 1.返回Success2.chunk状态不变                 | 重启前调了DeleteSnapshot，调用时存在快照，然后用户又打了快照，并且有新数据写入；重启后恢复了之前的DeleteSnapshot |          |
|  66  | DeleteSnapshotOrCorrectSn |  chunk存在快照存在不是clone chunk  |      correctedSn==sncorrectedSn==chunkinfo.correctedSn       |           1.返回Success，快照被删除2.chunk状态不变           | 异常场景重启前调了DeleteSnapshot，调的时候快照不存在，修改了correctedSn；用户写入新的数据修改了sn；用户打了快照并且写入新的数据，但是刚创建完快照发生了重启，重启恢复了DeleteSnapshot。此时将快照删除，恢复到后面的操作仍然会产生快照，可以保证幂等性。 |          |
|  67  | DeleteSnapshotOrCorrectSn |  chunk存在快照存在不是clone chunk  |       correctedSn==sncorrectedSn<chunkinfo.correctedSn       |           1.返回Success，快照被删除2.chunk状态不变           | 异常场景重启前调了DeleteSnapshot，然后用户又打了快照，快照期间数据未写入，再次调DeleteSnapshot，然后又打了快照，并且写入新数据，但是刚创建完快照发生了重启，重启恢复了一开始的DeleteSnapshot。此时将快照删除，恢复到后面的操作仍然会产生快照，可以保证幂等性。 |          |



在做恢复测试时，要想保证所有场景都是正确的，可能需要对每一组用例都单独测试，而且每一组用例还需要测从不同步骤开始恢复的情况。

举个例子，假如在重启前，用户执行了1、2、3三个操作使chunk处于某个用例的given状态。但是步骤1可能已经做了checkpoint被删掉了，也可能1、2都被删掉了。

那么实际测试的话需要在同样状态下测恢复1-2-3、2-3、3三种序列的情况。有的用例状态可能需要n步操作产生，那么就有n种恢复序列。然后有这么多组用例，需要比较巧妙的方式来实现测试代码。

**构造重启前的数据**

为了能验证所有接口，这里需要模拟普通文件、快照文件、克隆文件的一般使用。

普通读写重启恢复：

1.WriteChunk，产生chunk1；写入[0,8kb];

2.WriteChunk写chunk1，写入[4kb,12kb];

3.DeleteChunk，删除chunk1

模拟文件快照操作：

快照场景：

1.WriteChunk，产生chunk1；写入[0,8k];版本为1.

2.WriteChunk，写[4kb, 12kb],版本为2

3.DeleteSnapshotOrCorrectSn，correctedSn为2

4.再次DeleteSnapshotOrCorrectSn，correctedSn为3

5.WriteChunk，写[8kb, 16kb]，版本为3

6.WriteChunk，写[4kb, 12kb]，版本为4

7.DeleteSnapshotOrCorrectSn，correctedSn为4

8.再次DeleteSnapshotOrCorrectSn，correctedSn为5

9.DeleteChunk，删除chunk1

克隆场景：

1.CreateCloneChunk，产生chunk，sn=1，correctedSn=0

2.WriteChunk，写[0, 8kb]，版本为1

3.PasteChunk,写[4kb, 12kb]

4.通过PasteChunk遍写一遍chunk

5.WriteChunk，写[4kb, 12kb]，版本为2

6.DeleteSnapshotOrCorrectSn，correctedSn为2

7.DeleteChunk

恢复场景：

1.CreateCloneChunk，产生chunk，sn=3，correctedSn=5

2.PasteChunk，写[0, 8kb]

3.WriteChunk,写[4kb, 12kb]，版本为5

4.通过WriteChunk遍写一遍chunk

5.DeleteChunk

**针对上面场景，测试以任意步骤作为重启前的最后执行步骤，然后检验从该步骤之前的任意步骤开始恢复是否能够满足幂等性。**

### 异常测试

| 编号 | 测试内容                                               | Given                       | When                                                         | Then                                                         | 备注 | 是否执行         |
| :--- | :----------------------------------------------------- | :-------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :--- | :--------------- |
| 1    | chunk的metapage数据损坏，然后启动DataStore             | chunk存在                   | 1.通过LocalFileSystem修改metapage中的某个字节2.重新启动DataStore | 重启失败                                                     |      | 单元测试集成测试 |
| 2    | 运行过程中，chunk的metapage损坏                        | chunk存在程序运行中         | 1.通过LocalFileSystem修改metapage中的某个字节                | 1.WriteChunk写入成功2.DataStore重启成功                      |      | 单元测试集成测试 |
| 3    | chunk快照的metapage数据损坏，然后启动DataStore         | chunk存在快照存在           | 1.通过LocalFileSystem修改chunk快照metapage中的某个字节2.重新启动DataStore | 重启失败                                                     |      | 单元测试集成测试 |
| 4    | 运行过程中，chunk快照的metapage损坏                    | chunk存在快照存在程序运行中 | 1.通过LocalFileSystem修改快照metapage中的某个字节2.通过WriteChunk触发快照metapage的更新3.重新启动DataStore | 1.WriteChunk写入成功2.DataStore重启成功                      |      | 单元测试集成测试 |
| 5    | WriteChunk，数据写了一半时，重启/崩溃                  | chunk存在快照不存在         | 1.构造要写入的数据，将部分数据通过外围写入chunk2.构造WriteChunk请求，请求的sn==chunk.sn，sn>chunk.correctedsn，3.提交WriteChunk操作 | 1.WriteChunk成功2.读取写入区域，与实际写入数据相等           |      | 单元测试集成测试 |
| 6    | WriteChunk，更新metapage后，写数据前崩溃，重启/崩溃    | chunk存在快照不存在         | 1.构造WriteChunk请求，请求的sn>chunk.sn，sn==chunk.correctedsn2.通过外围更新chunk的metapage，将chunk.sn改为sn2.提交WriteChunk请求 | 1.WriteChunk成功2.读取写入区域，与实际写入数据相同3.chunkinfo.sn==sn，其他状态不变 |      | 单元测试集成测试 |
| 7    | WriteChunk，创建快照后，更新metapage前重启/崩溃        | chunk存在快照不存在         | 1.构造WriteChunk请求，请求的sn>chunk.snsn>chunk.correctedsn2.通过外围生成chunk的快照文件，快照版本为chunk.sn3.提交WriteChunk请求 | 1.WriteChunk成功2.写入区域cow到快照文件3.读取chunk和快照文件与预期一致4.chunk.sn更新为sn，其他状态不变 |      | 单元测试集成测试 |
| 8    | WriteChunk，创建快照并更新metapage后，cow前重启/崩溃   | chunk存在快照不存在         | 1.构造WriteChunk请求，请求的sn>chunk.snsn>chunk.correctedsn2.通过外围生成chunk的快照文件，快照版本为chunk.sn；通过外围更新chunk的metapage，将chunk.sn改为sn3.提交WriteChunk请求 | 1.WriteChunk成功2.写入区域cow到快照文件3.读取chunk和快照文件与预期一致4.chunk.sn更新为sn，其他状态不变 |      | 单元测试集成测试 |
| 9    | WriteChunk，cow拷贝数据时，更新快照metapage前重启/崩溃 | chunk存在快照存在           | 1.构造WriteChunk请求，请求的sn==chunk.snsn>chunk.correctedsn2.通过外围写数据到快照文件；3.提交WriteChunk请求 | 1.WriteChunk成功2.数据再次cow到快照文件3.读取chunk和快照文件数据与预期一致4.chunk其他状态不变 |      | 单元测试集成测试 |
| 10   | WriteChunk，cow成功，写数据到chunk时重启/崩溃          | chunk存在快照存在           | 1.构造WriteChunk请求，请求的sn==chunk.snsn>chunk.correctedsn2.通过外围写数据到快照文件及快照的metapage，并将部分数据写入chunk文件3.提交WriteChunk请求 | 1.WriteChunk成功2.数据不再cow3.读取chunk和快照文件数据与预期一致4.chunk其他状态不变 |      | 单元测试集成测试 |
| 11   | PasteChunk，数据写入一半时，还未更新metapage重启/崩溃  | chunk存在是clone chunk      | 1.构造PasteChunk请求，请求的2.通过外围将部分数据写入chunk文件3.提交PasteChunk请求 | 1.PasteChunk成功2.读取chunk数据和实际写入相同3.chunkinfo.bitmap中对应的bit置为1 |      | 单元测试集成测试 |

### 规模测试

| 编号 | 测试内容          | Given                        | When                            | Then                                               | 备注                                                | 是否执行         |
| :--- | :---------------- | :--------------------------- | :------------------------------ | :------------------------------------------------- | :-------------------------------------------------- | :--------------- |
| 1    | 测试打开大量chunk | 一个大容量的盘（1T、4T这样） | 1.通过WriteChunk创建大量的chunk | 程序运行正常，在磁盘容量范围内，WriteChunk不会失败 | TODO 这个用例不适合在CI上跑，因为运行时间可能比较长 | 单元测试集成测试 |

### 并发测试

| 编号 | 测试内容                        | Given | When                                                         | Then                                           | 备注 | 是否执行         |
| :--- | :------------------------------ | :---- | :----------------------------------------------------------- | :--------------------------------------------- | :--- | :--------------- |
| 1    | 测试并发对同一chunk进行随机操作 |       | 1.启动10个线程2.每个线程对同一个chunk进行随机操作3.每个线程循环产生10个操作 | 程序运行正常                                   |      | 单元测试集成测试 |
| 2    | 测试并发对不同chunk进行随机操作 |       | 1.启动10个线程2.每个线程对一个随机chunk进行随机操作3.每个线程循环产生10个操作 | 程序运行正常                                   |      | 单元测试集成测试 |
| 3    | 测试大压力下的4KB写操作         |       | 分别启动1、10、50个线程测试4kb写操作，分别测试10000/50000/100000个IO | 输出IOPS和带宽                                 |      | 单元测试集成测试 |
| 4    | 测试大压力下的1MB写操作         |       | 分别启动1、10、50个线程测试1MB写操作，分别测试100/1000/3000个IO，输出带宽 | 输出IOPS和带宽（TODO目前CI性能较差，后续补充） |      | 单元测试集成测试 |
| 5    | 测试大压力下的4KB读操作         |       | 分别启动1、10、50个线程测试4kb读操作，分别测试10000/50000/100000个IO | 输出IOPS和带宽                                 |      | 单元测试集成测试 |
| 6    | 测试大压力下的1MB读操作         |       | 分别启动1、10、50个线程测试1MB写操作，分别测试100/1000/3000个IO | 输出IOPS和带宽（TODO目前CI性能较差，后续补充） |      | 单元测试集成测试 |
| 7    | 测试大压力下的4KB读写操作       |       | 分别启动1、10、50个线程测试4kb读写操作，分别测试10000/50000/100000个IO | 输出IOPS和带宽                                 |      | 单元测试集成测试 |
| 8    | 测试大压力下的1MB读写操作       |       | 分别启动1、10、50个线程测试1MB读写操作，分别测试100/1000/3000个IO | 输出IOPS和带宽（TODO目前CI性能较差，后续补充） |      | 单元测试集成测试 |