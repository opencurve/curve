/*
 * Project: nebd
 * Created Date: Tuesday March 3rd 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_FILE_ENTITY_H_
#define SRC_PART2_FILE_ENTITY_H_

#include <brpc/closure_guard.h>
#include <limits.h>
#include <memory>
#include <string>
#include <atomic>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "src/common/rw_lock.h"
#include "src/common/timeutility.h"
#include "src/part2/define.h"
#include "src/part2/util.h"
#include "src/part2/request_executor.h"
#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {

using nebd::common::BthreadRWLock;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;
using nebd::common::TimeUtility;

class NebdFileInstance;
class NebdRequestExecutor;
using NebdFileInstancePtr = std::shared_ptr<NebdFileInstance>;

// 处理用户请求时需要加读写锁，避免close时仍有用户IO未处理完成
// 对于异步IO来说，只有返回时才能释放读锁，所以封装成Closure
// 在发送异步请求前，将closure赋值给NebdServerAioContext
class NebdRequestReadLockClosure : public Closure {
 public:
    explicit NebdRequestReadLockClosure(BthreadRWLock& rwLock)  // NOLINT
        : rwLock_(rwLock)
        , done_(nullptr) {
        rwLock_.RDLock();
    }
    ~NebdRequestReadLockClosure() {}

    void Run() {
        std::unique_ptr<NebdRequestReadLockClosure> selfGuard(this);
        brpc::ClosureGuard doneGuard(done_);
        rwLock_.Unlock();
    }

    void SetClosure(Closure* done) {
        done_ = done;
    }

    Closure* GetClosure() {
        return done_;
    }

 private:
    BthreadRWLock& rwLock_;
    Closure* done_;
};

struct NebdFileEntityOption {
    int fd;
    std::string fileName;
    MetaFileManagerPtr metaFileManager_;
};

class NebdFileEntity : public std::enable_shared_from_this<NebdFileEntity> {
 public:
    NebdFileEntity();
    virtual ~NebdFileEntity();

    /**
     * 初始化文件实体
     * @param option: 初始化参数
     * @return 成功返回0， 失败返回-1
     */
    virtual int Init(const NebdFileEntityOption& option);
    /**
     * 打开文件
     * @return 成功返回fd，失败返回-1
     */
    virtual int Open();
    /**
     * 重新open文件，如果之前的后端存储的连接还存在则复用之前的连接
     * 否则与后端存储建立新的连接
     * @param xattr: 文件reopen需要的信息
     * @return 成功返回fd，失败返回-1
     */
    virtual int Reopen(const ExtendAttribute& xattr);
    /**
     * 关闭文件
     * @param removeMeta: 是否要移除文件元数据记录，true表示移除，false表示不移除
     * 如果是part1传过来的close请求，此参数为true
     * 如果是heartbeat manager发起的close请求，此参数为false
     * @return 成功返回0，失败返回-1
     */
    virtual int Close(bool removeMeta);
    /**
     * 给文件扩容
     * @param newsize: 新的文件大小
     * @return 成功返回0，失败返回-1
     */
    virtual int Extend(int64_t newsize);
    /**
     * 获取文件信息
     * @param fileInfo[out]: 文件信息
     * @return 成功返回0，失败返回-1
     */
    virtual int GetInfo(NebdFileInfo* fileInfo);
    /**
     * 异步请求，回收指定区域空间
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Discard(NebdServerAioContext* aioctx);
    /**
     * 异步请求，读取指定区域内容
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioRead(NebdServerAioContext* aioctx);
    /**
     * 异步请求，写数据到指定区域
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioWrite(NebdServerAioContext* aioctx);
    /**
     * 异步请求，flush文件缓存
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Flush(NebdServerAioContext* aioctx);
    /**
     * 使指定文件缓存失效
     * @return 成功返回0，失败返回-1
     */
    virtual int InvalidCache();

    virtual const std::string GetFileName() const {
        return fileName_;
    }

    virtual const int GetFd() const {
        return fd_;
    }

    virtual void UpdateFileTimeStamp(uint64_t timestamp) {
        timeStamp_.store(timestamp);
    }

    virtual const uint64_t GetFileTimeStamp() const {
        return timeStamp_.load();
    }

    virtual const NebdFileStatus GetFileStatus() const {
        return status_.load();
    }

 private:
    /**
     * 更新文件状态，包括元信息文件和内存状态
     * @param fileInstancea: open或reopen返回的文件上下文信息
     * @return: 成功返回0，失败返回-1
     */
    int UpdateFileStatus(NebdFileInstancePtr fileInstance);
    /**
     * 请求统一处理函数
     * @param task: 实际请求执行的函数体
     * @return: 成功返回0，失败返回-1
     */
    using ProcessTask = std::function<int(void)>;
    int ProcessSyncRequest(ProcessTask task);
    int ProcessAsyncRequest(ProcessTask task, NebdServerAioContext* aioctx);

    // 确保文件处于opened状态，如果不是则尝试进行open
    // 无法open或者open失败，则返回false，
    // 如果文件处于open状态，则返回true
    bool GuaranteeFileOpened();

 private:
    // 文件读写锁，处理请求前加读锁，close文件的时候加写锁
    // 避免close时还有请求未处理完
    BthreadRWLock rwLock_;
    // 互斥锁，用于open、close之间的互斥
    bthread::Mutex fileStatusMtx_;
    // nebd server为该文件分配的唯一标识符
    int fd_;
    // 文件名称
    std::string fileName_;
    // 文件当前状态，opened表示文件已打开，closed表示文件已关闭
    std::atomic<NebdFileStatus> status_;
    // 该文件上一次收到心跳时的时间戳
    std::atomic<uint64_t> timeStamp_;
    // 文件在executor open时返回上下文信息，用于后续文件的请求处理
    NebdFileInstancePtr fileInstance_;
    // 文件对应的executor的指针
    NebdRequestExecutor* executor_;
    // 元数据持久化管理
    MetaFileManagerPtr metaFileManager_;
};
using NebdFileEntityPtr = std::shared_ptr<NebdFileEntity>;
std::ostream& operator<<(std::ostream& os, const NebdFileEntity& entity);

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_ENTITY_H_
