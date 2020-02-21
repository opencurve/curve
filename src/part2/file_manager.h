/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_FILE_MANAGER_H_
#define SRC_PART2_FILE_MANAGER_H_

#include <brpc/closure_guard.h>
#include <limits.h>
#include <memory>
#include <string>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "src/common/rw_lock.h"
#include "src/common/name_lock.h"
#include "src/common/timeutility.h"
#include "src/part2/define.h"
#include "src/part2/util.h"
#include "src/part2/filerecord_manager.h"
#include "src/part2/request_executor.h"
#include "proto/client.pb.h"

namespace nebd {
namespace server {

using nebd::common::NameLock;
using nebd::common::NameLockGuard;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;
using nebd::common::TimeUtility;

// 处理用户请求时需要加读写锁，避免close时仍有用户IO未处理完成
// 对于异步IO来说，只有返回时才能释放读锁，所以封装成Closure
// 在发送异步请求前，将closure赋值给NebdServerAioContext
class NebdProcessClosure : public Closure {
 public:
    explicit NebdProcessClosure(const NebdFileRecord& fileRecord)
        : fileRecord_(fileRecord)
        , done_(nullptr) {
        fileRecord_.rwLock->RDLock();
    }
    NebdProcessClosure() {}

    void Run() {
        std::unique_ptr<NebdProcessClosure> selfGuard(this);
        brpc::ClosureGuard doneGuard(done_);
        fileRecord_.rwLock->Unlock();
    }

    void SetClosure(Closure* done) {
        done_ = done;
    }

    Closure* GetClosure() {
        return done_;
    }

    NebdFileRecord GetFileRecord() {
        return fileRecord_;
    }

    void SetFileRecord(const NebdFileRecord& fileRecord) {
        fileRecord_ = fileRecord;
    }

 private:
    NebdFileRecord fileRecord_;
    Closure* done_;
};

class NebdFileManager {
 public:
    explicit NebdFileManager(FileRecordManagerPtr recordManager);
    virtual ~NebdFileManager();
    /**
     * 停止FileManager并释放FileManager资源
     * @return 成功返回0，失败返回-1
     */
    virtual int Fini();
    /**
     * 启动FileManager
     * @return 成功返回0，失败返回-1
     */
    virtual int Run();
    /**
     * 打开文件
     * @param filename: 文件的filename
     * @return 成功返回fd，失败返回-1
     */
    virtual int Open(const std::string& filename);
    /**
     * 关闭文件
     * @param fd: 文件的fd
     * @param removeRecord: 是否要移除文件记录，true表示移除，false表示不移除
     * 如果是part1传过来的close请求，此参数为true
     * 如果是heartbeat manager发起的close请求，此参数为false
     * @return 成功返回0，失败返回-1
     */
    virtual int Close(int fd, bool removeRecord);
    /**
     * 给文件扩容
     * @param fd: 文件的fd
     * @param newsize: 新的文件大小
     * @return 成功返回0，失败返回-1
     */
    virtual int Extend(int fd, int64_t newsize);
    /**
     * 获取文件信息
     * @param fd: 文件的fd
     * @param fileInfo[out]: 文件信息
     * @return 成功返回0，失败返回-1
     */
    virtual int GetInfo(int fd, NebdFileInfo* fileInfo);
    /**
     * 异步请求，回收指定区域空间
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Discard(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，读取指定区域内容
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioRead(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，写数据到指定区域
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioWrite(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，flush文件缓存
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Flush(int fd, NebdServerAioContext* aioctx);
    /**
     * 使指定文件缓存失效
     * @param fd: 文件的fd
     * @return 成功返回0，失败返回-1
     */
    virtual int InvalidCache(int fd);


    // set public for test
    // 启动时从metafile加载文件记录，并reopen文件
    int Load();
    // 返回recordmanager
    virtual FileRecordManagerPtr GetRecordManager();

 private:
    /**
     * 打开指定文件并更新元数据信息
     * @param filename: 文件名
     * @param create: 为true将产生新的filerecord
     * @return: 成功返回0，失败返回-1
     */
    int OpenInternal(const std::string& fileName, bool create = false);
    /**
     * 关闭指定文件，并将文件状态变更为CLOSED
     * @param fd: 文件的内存记录
     * @return: 成功返回0，失败返回-1
     */
    int CloseInternal(int fd);
    /**
     * 根据文件内存记录信息重新open文件
     * @param fileRecord: 文件的内存记录
     * @return: 成功返回0，失败返回-1
     */
    int Reopen(NebdFileRecord fileRecord);
    /**
     * 分配新的可用的fd
     * @return: 成功返回有效的fd，失败返回-1
     */
    int GetValidFd();
    /**
     * 请求统一处理函数
     * @param fd: 请求对应文件的fd
     * @param task: 实际请求执行的函数体
     * @return: 成功返回0，失败返回-1
     */
    using ProcessTask = std::function<int(NebdProcessClosure*)>;
    int ProcessRequest(int fd, ProcessTask task);

 private:
    // 当前filemanager的运行状态，true表示正在运行，false标为未运行
    std::atomic<bool> isRunning_;
    // 文件名锁，对同名文件加锁
    NameLock nameLock_;
    // fd分配器
    FdAllocator fdAlloc_;
    // nebd server 文件记录管理
    FileRecordManagerPtr fileRecordManager_;
};
using NebdFileManagerPtr = std::shared_ptr<NebdFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_MANAGER_H_
