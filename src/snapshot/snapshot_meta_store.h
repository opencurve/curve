/*************************************************************************
> File Name: snapshot_meta_store.h
> Author:
> Created Time: Fri Dec 14 18:25:30 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef _SNAPSHOT_META_STORE_H
#define _SNAPSHOT_META_STORE_H

#include <vector>
#include <string>
#include <map>
#include <mutex> //NOLINT

#include "src/snapshot/dao/snapshotRepo.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

const uint64_t kUnInitializeSeqNum = 0;

//快照处理状态
enum class Status{
    done = 0,
    pending,
    deleting,
    errorDeleting,
    canceling,
    error
};

//快照信息
class SnapshotInfo {
 public:
    SnapshotInfo()
        :uuid_(),
        seqNum_(kUnInitializeSeqNum),
        chunkSize_(0),
        segmentSize_(0),
        fileLength_(0),
        time_(0),
        status_(Status::pending) {}

    SnapshotInfo(UUID uuid,
            const std::string &user,
            const std::string &fileName,
            const std::string &snapshotName)
        :uuid_(uuid),
        user_(user),
        fileName_(fileName),
        snapshotName_(snapshotName),
        seqNum_(kUnInitializeSeqNum),
        chunkSize_(0),
        segmentSize_(0),
        fileLength_(0),
        time_(0),
        status_(Status::pending) {}
    SnapshotInfo(UUID uuid,
            const std::string &user,
            const std::string &fileName,
            const std::string &desc,
            uint64_t seqnum,
            uint32_t chunksize,
            uint64_t segmentsize,
            uint64_t filelength,
            uint64_t time,
            Status status)
        :uuid_(uuid),
        user_(user),
        fileName_(fileName),
        snapshotName_(desc),
        seqNum_(seqnum),
        chunkSize_(chunksize),
        segmentSize_(segmentsize),
        fileLength_(filelength),
        time_(time),
        status_(status) {}

    UUID GetUuid() const {
        return uuid_;
    }

    std::string GetUser() const {
        return user_;
    }

    std::string GetFileName() const {
        return fileName_;
    }

    std::string GetSnapshotName() const {
        return snapshotName_;
    }

    void SetSeqNum(uint64_t seqNum) {
        seqNum_ = seqNum;
    }

    uint64_t GetSeqNum() const {
        return seqNum_;
    }

    void SetChunkSize(uint32_t chunkSize) {
        chunkSize_ = chunkSize;
    }

    uint32_t GetChunkSize() const {
        return chunkSize_;
    }

    void SetSegmentSize(uint64_t segmentSize) {
        segmentSize_ = segmentSize;
    }

    uint64_t GetSegmentSize() const {
        return segmentSize_;
    }

    void SetFileLength(uint64_t fileLength) {
        fileLength_ = fileLength;
    }

    uint64_t GetFileLength() const {
        return fileLength_;
    }

    void SetCreateTime(uint64_t createTime) {
        time_ = createTime;
    }

    uint64_t GetCreateTime() const {
        return time_;
    }

    void SetStatus(Status status) {
        status_ = status;
    }

    Status GetStatus() const {
        return status_;
    }

 private:
    // 快照uuid
    UUID uuid_;
    // 租户信息
    std::string user_;
    // 快照目标文件名
    std::string fileName_;
    // 快照名
    std::string snapshotName_;
    // 快照版本号
    uint64_t seqNum_;
    // 文件的chunk大小
    uint32_t chunkSize_;
    // 文件的segment大小
    uint64_t segmentSize_;
    //文件大小
    uint64_t fileLength_;
    // 快照创建时间
    uint64_t time_;
    // 快照处理的状态
    Status status_;
};

class SnapshotMetaStore {
 public:
    SnapshotMetaStore() {}
    virtual ~SnapshotMetaStore() {}
    /**
     * 初始化metastore，不同的metastore可以有不同的实现，可以是初始化目录文件或者初始化数据库
     * @return: 0 初始化成功/ 初始化失败 -1
     */
    virtual int Init() = 0;
    // 添加一条快照信息记录
    /**
     * 添加一条快照记录到metastore中
     * @param 快照信息结构体
     * @return: 0 插入成功/ -1 插入失败
     */
    virtual int AddSnapshot(const SnapshotInfo &snapinfo) = 0;
    /**
     * 从metastore删除一条快照记录
     * @param 快照任务的uuid，全局唯一
     * @return 0 删除成功/ -1 删除失败
     */
    virtual int DeleteSnapshot(UUID uuid) = 0;
    /**
     * 更新快照记录
     * @param 快照信息结构体
     * @return: 0 更新成功/ -1 更新失败
     */
    virtual int UpdateSnapshot(const SnapshotInfo &snapinfo) = 0;
    /**
     * 获取指定快照的快照信息
     * @param 快照的uuid
     * @param 保存快照信息的指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotInfo(UUID uuid, SnapshotInfo *info) = 0;
    /**
     * 获取指定文件的快照信息列表
     * @param 文件名
     * @param 保存快照信息的vector指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(const std::string &filename,
                                std::vector<SnapshotInfo> *v) = 0;
    /**
     * 获取全部的快照信息列表
     * @param 保存快照信息的vector指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo> *list) = 0;
};

class DBSnapshotMetaStore : public SnapshotMetaStore{
 public:
    explicit DBSnapshotMetaStore(
        std::shared_ptr<curve::snapshotserver::SnapshotRepo> repo)
        :repo_(repo) {}
    ~DBSnapshotMetaStore() {}
    int Init() override;
    int AddSnapshot(const SnapshotInfo &snapinfo) override;
    int DeleteSnapshot(UUID uuid) override;
    int UpdateSnapshot(const SnapshotInfo &snapinfo) override;
    int GetSnapshotInfo(UUID uuid, SnapshotInfo *info) override;
    int GetSnapshotList(const std::string &filename,
                        std::vector<SnapshotInfo> *v) override;
    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;

 private:
    /**
     * 启动时从metastore加载快照任务信息到内存中
     * @return: 0 加载成功/ -1 加载失败
     */
    int LoadSnapshotInfos();
    // db metastore的repo实现
    std::shared_ptr<curve::snapshotserver::SnapshotRepo> repo_;
    // key is UUID, map 需要考虑并发保护
    std::map<UUID, SnapshotInfo> snapInfos_;
    // 考虑使用rwlock
    std::mutex snapInfos_mutex;
};
}  // namespace snapshotserver
}  // namespace curve
#endif
