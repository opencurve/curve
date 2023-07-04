## 1 背景
在curvefs当前版本（release2.0）中已经实现了内存缓存，本地盘，s3的3层存储模式。写的缓存策略采用的是writeback，读的淘汰策略采用的是lru。本文主要设计在curveBS场景下的多级缓存实现，对于内存缓存进行统一管理。  
由于本地盘受到单节点访问问题，磁盘故障数据丢失问题，如果将curveBS作为s3的缓存，则可以有效的规避这些问题。
## 2 产品形态
公有云场景：支持后端只使用S3存储（低成本），支持后端只使用共享云盘（高性能），支持后端使用共享云盘和s3存储（有一定性能需求，和成本要求）。
私有云场景：支持后端只使用S3存储（低成本），支持后端只使用curveBS（高性能），支持后端使用curveBS和s3存储（有一定性能需求，和成本要求）。
## 3 架构
![curvefs_client_cache_structure.png](./../images/curvefs_client_cache_structure.png)

- fuseClient负责posix接口实现，并操作数据和元数据。
- S3Adaptor负责s3相关的数据面操作，包括读，写，truncate，flush。
- VolumeAdaptor负责volume相关的数据面操作，包括读，写，truncate，flush。
- FsCacheManager负责数据cache缓存，并管理一个后台flush线程进行数据下刷到S3或者Volume。
- S3Storage负责对接s3，并更新inode中s3相关的元数据ChunkInfo。
- VolumeStorage负责对接volume，并更新inode中volume相关的元数据volemeExtent。
- diskcache负责本地缓存。
## 4 整体思路
### 4.1 复用原有的cache模块和流程。
       原有cache模块的设计本身和底层存储无关，代码可能会有少量变动
### 4.2 重构DataCache的Flush流程。
#### 4.2.1 方案一 不同的storage采用不同的格式存储diskcache
- 在s3场景，当前DataCache的flush，是先申请chunkid，然后根据chunkId，blockIndex，fsId，inodeId来生成唯一的key，作为写到diskcache的filename，同时更新s3info。在读流程里找到对应的s3info则可以重新生成对应的key来读取diskcache。（已实现）
- volume的写流程：每次调用DataCache的flush的时候，先走PrepareWriteRequest流程，会生成一组volume上要写的<offset，len>，每个对应diskcache上一个文件，如果<offset，len>没有4k对齐，则在diskcache这一层去volume上读取数据进行补齐（原本就有该逻辑，不会造成性能问题，后续落到该4k的请求，如果diskcache上有缓存，则无需再次去volume上读取）。diskcache上filename命名为：fsid_offset。并且在缓存中构建一个map<offset，pair<filename, fileLen>>（volume的偏移，diskcache的文件名及文件长度）的diskcacheData表。后续更新元数据的流程不变。
- volume的读流程：先走原有DivideForRead流程获取要读取的ReadPart集合，每个ReadPart先通过上述diskcacheData查看对应的数据是否缓存，如果缓存则找到对应的file和offset，len，从缓存中读取，否则从volume上进行读。
#### 4.2.2 方案二 不同的storage采用同一种格式存储diskcache
- 写流程：每次调用DataCache的flush的时候，会根据fsid，inodeid，文件的offset，生成diskCache上的filename：fsid_inode_offset（注意这里的offset和方案一的不同，方案一的offset是volume的offset，这里是文件的offset。）同样在缓存中构建一个map<inodeId，map<offset，pair<filename, fileLen>>> diskcacheDataMap。数据写到diskcache，并更新diskcacheDataMap则返回。这里由于DataCache的flush有可能是会有和diskcacheDataMap重叠的问题，需要进行合并。
- diskcache后台流程：diskcache在后台回刷到s3/volueme的时候在向mds申请chunkId/extent。数据写到S3/volume后，进行inode元数据更新。
- 读流程：根据文件的inode，offset，len，去diskcacheDataMap表查看是否缓存命中，如果命中则直接从diskcache上读取，否则查找s3/volueme的元数据，从s3/volume上进行读取。

#### 4.2.3 方案一和方案二对比
- 实现难度上看方案一比方案二简单。方案一先经过PrepareWriteRequest，已经能够确保要写入diskcache的<offset，len> 要么是已分配的一部分，要么是全新的写，场景较少。方案二写入diskcache的<inode，offset，len>完全随机，则考虑的场景会更多。
- 方案一flush到diskcache就更新了元数据，在多client场景下会出现另外个client查找到最新的元数据，但是数据读取错误的问题（这个和writeback策略有关，如果是writethough则没问题）。方案二由于后端存储的数据元数据是同步更新，读取不会出错，但是会出现flush成功，读取到仍然是老数据的问题。
- 目前倾向于方案一，实现简单，同时可先不实现DataCache写diskcache的流程，直接写volume。

### 4.3 实现curveBS数据和s3数据转换
#### 4.3.1 VolumeExtent和S3info共存
要实现文件curve盘数据向s3转换，则需要这2种元数据在inode种共存。同时要确保VolumeExtent上的数据是最新的，read流程先查找VolumeExtent，如果没有再查找S3info。
#### 4.3.2 curve盘上数据转s3场景:
- curve盘上数据已无剩余空间分配，数据直接透写到s3。
- 文件分配的某个VolumeExtentList大于15个（默认值，可配置），文件逻辑数据连续。如何定义文件逻辑数据连续？当前一个VolumeExtent默认大小1G，而ChunkInfoList的默认大小是64M，也就是说一个VolumeExtent对应的若干个ChunkInfoList的数据范围内，如果转换为s3info不超过
- 超过xx天没被访问的文件。这里需要在inode中额外加一个访问时间，最好不要复用atime，某些场景下，atime可能不设置。
- 支持可扩展其他配置规则。
- 当前被打开的文件不做数据转换（目前metaserver已支持），做到文件后台数据转换和用户文件访问互斥。
#### 4.3.3 S3上数据转curve盘场景
- 支持配置不转换（默认）
- 支持配置第一次访问的时候转换（可先不实现）
#### 4.3.4 curve盘上数据转换s3流程
- 对于上述场景1，必须要在client的写流程进行处理。当分配失败返回没有空间，则直接写s3，并更新s3info。由于这块空间是需要新分配的，说明VolumeExtent上不存在，则满足VolumeExtent上数据是最新的语义。
- 对于其他场景则通过metaserver后台服务进行数据转换。
## 5 数据结构
### 5.1 client相关数据结构
```C++
/* 底层存储基类 */
class AdaptorStorage {
 public:
    virtual ~AdaptorStorage() = default;
    virtual ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
    virtual ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
    virtual CURVEFS_ERROR Truncate(uint64_t ino, size_t size) = 0;
    virtual CURVEFS_ERROR Flush(uint64_t ino) = 0;
    ssize_t WriteNoCache(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
};

class VolumeAdaptor : public AdaptorStorage{
 public:
    VolumeAdaptor(std::shared_ptr<VolumeStorage> volumeStorage,
                  std::shared_ptr<FsCacheManager> fsCacheManager);
    virtual ~VolumeAdaptor() {}
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
    CURVEFS_ERROR Flush(uint64_t ino);
    ssize_t WriteNoCache(uint64_t ino, off_t offset, size_t len, const char* data);
 private:
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<VolumeStorage> volumeStorage_;
};

class S3Adaptor : public AdaptorStorage {
 public:
    S3Adaptor(std::shared_ptr<S3Storage> S3Storage,
              std::shared_ptr<FsCacheManager> fsCacheManager) {}
    virtual ~S3Adaptor() {}
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
    CURVEFS_ERROR Flush(uint64_t ino);
    ssize_t WriteNoCache(uint64_t ino, off_t offset, size_t len, const char* data);
 private:
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<S3Storage> s3Storage_;
};

class MixAdaptor : public AdaptorStorage {
 public:
    MixAdaptor(std::shared_ptr<S3Client> s3Client,
                     std::shared_ptr<InodeCacheManager> inodeManager,
                     std::shared_ptr<MdsClient> mdsClient
                     std::shared_ptr<FsCacheManager> fsCacheManager_,
                     std::shared_ptr<VolumeStorage> volumeStorage) {}
    virtual ~MixAdaptor() {}
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
    CURVEFS_ERROR Flush(uint64_t ino);
 private:
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<S3Storage> s3Storage_;
    std::shared_ptr<VolumeStorage> volumeStorage_;
};

class UnderStorage {
 public:
    virtual ~UnderStorage() = default;
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size) = 0;
};

class S3Storage : public UnderStorage {
 public:
    S3Storage(std::shared_ptr<S3Client> s3Client,
    std::shared_ptr<InodeCacheManager> inodeManager,
    std::shared_ptr<MdsClient> mdsClient) {}
    virtual ~S3Storage() {}
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
 private:
    std::shared_ptr<S3Client> s3Client_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<MdsClient> mdsClient_;
};

class VolumeStorage : public UnderStorage {
public:
    VolumeStorage(SpaceManager* spaceManager,
    BlockDeviceClient* blockDeviceClient,
    InodeCacheManager* inodeCacheManager) {}
    virtual ~VolumeStorage() {}
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
 private:
    SpaceManager* spaceManager_;
    BlockDeviceClient* blockDeviceClient_;
    InodeCacheManager* inodeCacheManager_;
    VolumeStorageMetric metric_;
};

class FuseClientImpl : public FuseClient {
...
 private:
    ......
    std::shared_ptr<AdaptorStorage> adaptor_;
};

class FsCacheManager {
 public:
   FsCacheManager(AdaptorStorage *adaptorStorage,
uint64_t readCacheMaxByte, uint64_t writeCacheMaxByte)
 private:
    Thread bgFlushThread_;
    std::atomic<bool> toStop_;
    std::unordered_map<uint64_t, FileCacheManagerPtr>
    fileCacheManagerMap_; // first is inodeid
    RWLock rwLock_;
    std::mutex lruMtx_;
    std::list<DataCachePtr> lruReadDataCacheList_;
    uint64_t lruByte_;
    std::atomic<uint64_t> wDataCacheNum_;
    std::atomic<uint64_t> wDataCacheByte_;
    uint64_t readCacheMaxByte_;
    uint64_t writeCacheMaxByte_;
    bool isWaiting_;
    std::mutex mutex_;
    std::condition_variable cond_;
    ReadCacheReleaseExecutor releaseReadCache_;
    AdaptorStorage *adaptorStorage;
};

// 原有的diskcachemanager保持不变，将LRU相关函数和成员变量移动到子类中实现
class S3DiskCacheManager : public DiskCacheManager {
 public:
    S3DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
    std::shared_ptr<DiskCacheWrite> cacheWrite,
    std::shared_ptr<DiskCacheRead> cacheRead);
 private:
    std::shared_ptr<SglLRUCache<std::string>> cachedObjName_;
};

class VolumeDiskCacheManager : public DiskCacheManager {
 public:
    VolumeDiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
    std::shared_ptr<DiskCacheWrite> cacheWrite,
    std::shared_ptr<DiskCacheRead> cacheRead);
   ......
 private:
    // first is volume offset,second first filename,second fileLen
    std::map<uint64_t, std::pair<string, uint64_t>> cachedObj_;
    // first is fileLen,second is volume offset
    std::multimap<uint64_t, uint64_t> trimeObj_;
};

```
### 5.2 metaserver相关数据结构
```C++
enum FSType {
    TYPE_VOLUME = 1;
    TYPE_S3 = 2;
    TYPE_MIX = 3;
}
message FsDetail {

    optional common.Volume volume = 1;
    optional common.S3Info s3Info = 2;

}

message Inode {
......
   required uint64 lastaccesstime = 23; // 文件最后一次访问时间
};
enum InodeLCStatus{
    Init = 0,
    InProcess = 1
};
class InodeManager {
......
 private:
    std::map<Key4Inode, InodeLCStatus> inodeLcStatusMap_;
}

struct LifeCycleOption {
    uint32_t threadNum;
};
class LifeCycleManager {
 public:
    static LifeCycleManager *GetInstance() {
        if (lcManager_ == nullptr) {
            lcManager_ = new LifeCycleManager();
        }
        return lcManager_;
    }
    void Init(LifeCycleOption conf);
 private:
    std::thread entry_;
    curve::common::TaskThreadPool<> taskpool_;
    static LifeCycleManager* lcManager_;
    std::vector<std::shared_ptr<Partition>>> partition_;
    std::shared_ptr<LCRuleManager> lcRuleManager_;
};

class LCRule {
 public:
    LCRule(std::string id, std::string prefixDir, std::string expiration);
    ~LCRule() {}
 private:
    std::string id_;
    std::string prefixDir_;
    std::string expiration_;
};

struct LCFileInfo {
    std::string fileFullPath;
    uint64_t lastAccessTime;
};

class LCAction {
 public:
    bool Check(LCFileInfo lcFileInfo) = 0;
    int Process(LCFileInfo lcFileInf) = 0;
};

class LCMigrateCurveBS : LCAction {
 public:
    bool Check(LCFileInfo lcFileInfo) = 0;
    int Process(LCFileInfo lcFileInf) = 0;
 private:
    // 这里有个问题这2个数据结构为client的，和client的inodemanager强耦合，
    // 如果需要做到metaserver复用，需要对这一块重构，
    // 而且metaerver已由的删除文件和s3compact都有独立的实现，未统一
    std::shared_ptr<S3Storage> s3Storage_;
    std::shared_ptr<VolumeStorage> volumeStorage_;
};

class LCRuleManager {
 public:
    LCRuleManager() {}
    ~LCRuleManager() {}
    void Init() {}
 private:
    //first is fsid，second's first is id
    std::map<uint64_t, std::map<string, LCAtion>> lcAction_;
};
```
### 5.3 mds相关数据结构
```C++
message LCRuleInfo {
    require string id = 1;
    require uint32 expirationsec = 2;
    require string prefix = 3;
};
message SetLCRuleRequest {
    required LCRuleInfo lcRuleInfo;
};

message SetLCRuleRequest {
    required FSStatusCode statusCode = 1;
};

message GetLCRulesRequest {
};

message GetLCRulesResponse {
    repeated LCRuleInfo lcRuleInfo = 1;
};

class LCRuleStorage {
 public:
   LCRuleStorage(std::shared_ptr<KVStorageClient> client = nullptr,
        std::string storeKey = CHUNKID_NAME_KEY_PREFIX) :
        client_(client),
        storeKey_(storeKey) {}
 private:
    std::shared_ptr<KVStorageClient> client_;
    std::string storeKey_;
};

class MdsServiceImpl : public MdsService {
......
 private:
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
    std::shared_ptr<LCRuleStorage> lcRuleStorage_;
};
```
## 6 详细设计
### 6.1 创建文件系统和初始化流程
- tools工具支持创建mix混合文件系统，需要将common.Volume和common.S3Info信息都传递到mds。
- mds端MdsServiceImpl::CreateFs支持创建mix文件系统
- client端初始化，根据文件系统类型，new S3Adaptor，VolumeAdaptor或者MixAdaptor。
### 6.2 文件写流程
- S3Adaptor，VolumeAdaptor或者MixAdaptor的Write流程都会调用FileCacheManager::Write，3者在写流程上一致。
### 6.3 文件读流程
- 先调用FileCacheManager::Read函数读取缓存数据
- 缓存中不存在则调用UnderStorage的Read接口，对于MixAdaptor，先调用VolumeStorage接口进行读取，如果不存在，则调用S3Storage::read接口进行读取。
- 对于不存在的部分进行补0
### 6.4 文件Flush流程
文件flush前面的流程可以和当前保持一致，主要变动在DataCache::Flush流程：
- 判断当前数据是否满足Flush条件（已实现）。
- 调用AdaptorStorage::WriteNoCache接口。
- S3Adaptor场景：
    调用s3Storage::Write接口，该接口实现为：
    - 申请chunkid，构建s3对象的objname。
    - 异步写s3对象存储
    - 更新inode元数据的s3info部分。
- VolumeAdaptor场景：
    - 调用VolumeStorage::Write接口，该接口已实现（DefaultVolumeStorage::Write()，对于写diskcache单独进行说明)。
- MixAdaptor场景：
    - 调用VolumeStorage::Write接口，该接口已实现（DefaultVolumeStorage::Write())。
    - 如果成功，则直接返回，如果返回分配空间不足，则调用s3Storage::Write接口。
### 6.5 volume场景写diskCache流程--todo
需要对现有VolumeStorage::Write进行修改。
- 调用PrepareWriteRequest获取到std::vector<WritePart>。
- 遍历std::vector<WritePart>，每个WritePart去VolumeDiskCacheManager::cachedObj_匹配，如果命中则根据second找到对应的文件和offset进行write操作。这里注意的本次写的WritePart和cachedObj_的文件可能存在3种场景：
    - WritePart的范围在某个cachedObj的file范围内部，直接write数据到second对应的文件偏移上
    - WritePart的范围和某个cachedObj_部分重叠，这个时候需要将未重叠的部分继续和下一个cachedObj_进行匹配，如果没有匹配到则追加写上一个file，同时更新cachedObj_上对应的元数据，如果匹配到，则考虑2个file的合并。
    - WritePart和cachedObj_没有重叠，则生成一个新的cachedObj，first为WritePart的offset，filename为inode+offset生成一个文件名，插入到cachedObj_中。
### 6.6 volume场景读diskCache流程--todo
需要现有的VolumeStorage::Read接口进行修改。
- 调用DivideForRead获取到std::vector<ReadPart>。
- 遍历std::vector<ReadPart>，每个ReadPart去VolumeDiskCacheManager::cachedObj_匹配，如果匹配到，则读对应的second中对应的file的offset数据。否则直接调用blockDeviceClient_::Readv。
### 6.7 LifeCycle初始化流程
- 整个LifeCycleManager设计为单例模式，在metaserver初始化过程进行初始化，启动后台转换的线程池。
- 在Partition初始化的过程中将各个inodeManager注册到LifeCycle管理起来。
### 6.8 LIfeRule的插入和删除

方案一：
- curvetools发送插入和删除请求到mds，并进行持久化。
- metaserver的LifeCycleManager定期（12小时）发送请求给mds，拉取全量的rule规则。

方案二：
- curvetools发送插入和删除请求到mds，并进行持久化。
- 通过heatbeat把changeRule返回给metaserver，并修改LifeCycleManager对应的规则

倾向于方案一
### 6.9 后台从volume转换s3流程
整个LC后台由一个查找线程entry_和一个转换线程池组成taskpool_组成。
查找线程主要流程：
- 定期发送请求到mds，获取全量的ruleInfo，并保留在LCRuleManager中的lcAction。
- 遍历LifeCycleManager中的Partition，获取inodeId列表，这里要注意，考虑到单个partition可能inodeId过大的情况，采用分段遍历的方式。
- 根据inodeId获取到inode，通过inode获取到LCFileInfo。然后每个inode遍历LCRuleManager的lcAction_。这里分2个步骤：
  - 对于不同lcAction调用check函数，确认LCFileInfo的expirationsec是否达到过期时间，同时确认当前无client open该文件，修改InodeManager的inodeLcStatusMap_状态。
  - 对于check返回true的情况，直接enqueue到线程池队列中。
- 线程池会执行LCAction::process函数，以chunk为粒度读取curveBS，然后写入到s3上，更新s3info，删除volume元数据（目前没提供回收和删除接口）。
- 后台转换和前台修改inode需要进行互斥：
  - 后台转换前会在check函数确认该文件没有被打开过，并标记该inode正在进行后台转换。
  - 前台open请求需要确认当前有无进行后台转换，如果有则设置一个stop标识，后台发现当前转换被设置未stop，则进行回退。