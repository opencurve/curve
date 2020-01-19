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
#include <thread>  // NOLINT
#include <string>
#include <mutex>  // NOLINT

#include "src/common/rw_lock.h"
#include "src/common/name_lock.h"
#include "src/common/timeutility.h"
#include "src/common/interrupt_sleep.h"
#include "src/part2/define.h"
#include "src/part2/util.h"
#include "src/part2/metafile_manager.h"
#include "src/part2/file_record_map.h"
#include "src/part2/request_executor_ceph.h"
#include "src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

using nebd::common::NameLock;
using nebd::common::NameLockGuard;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;
using nebd::common::TimeUtility;
using nebd::common::InterruptibleSleeper;
using MetaFileManagerPtr = std::shared_ptr<NebdMetaFileManager>;

struct NebdFileManagerOption {
    // 文件心跳超时时间（单位：秒）
    uint32_t heartbeatTimeoutS;
    // 心跳超时检测线程的检测间隔（时长：毫秒）
    uint32_t checkTimeoutIntervalMs;
    // metafilemanager 对象指针
    MetaFileManagerPtr metaFileManager;
};

// 处理用户请求时需要加读写锁，避免close时仍有用户IO未处理完成
// 对于异步IO来说，只有返回时才能释放读锁，所以封装成Closure
// 在发送异步请求前，将closure赋值给NebdServerAioContext
class NebdProcessClosure : public Closure {
 public:
    explicit NebdProcessClosure(NebdFileRecordPtr record)
        : record_(record)
        , done_(nullptr) {
        record_->rwLock.RDLock();
    }
    NebdProcessClosure() {}

    void Run() {
        std::unique_ptr<NebdProcessClosure> selfGuard(this);
        brpc::ClosureGuard doneGuard(done_);
        record_->rwLock.Unlock();
    }

    void SetClosure(Closure* done) {
        done_ = done;
    }

    Closure* GetClosure() {
        return done_;
    }

    NebdFileRecordPtr GetFileRecord() {
        return record_;
    }

 private:
    NebdFileRecordPtr record_;
    Closure* done_;
};

class NebdFileManager {
 public:
    NebdFileManager();
    virtual ~NebdFileManager();
    /**
     * 初始化FileManager各成员
     * @param option: 初始化参数
     * @return 成功返回0，失败返回-1
     */
    virtual int Init(NebdFileManagerOption option);
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
     * part2收到心跳后，会通过该接口更新心跳中包含的文件在内存中记录的时间戳
     * 心跳检测线程会根据该时间戳判断是否需要关闭文件
     * @param fd: 指定文件的fd
     * @return 成功返回0，失败返回-1
     */
    virtual int UpdateFileTimestamp(int fd);
    /**
     * 打开文件
     * @param filename: 文件的filename
     * @return 成功返回fd，失败返回-1
     */
    virtual int Open(const std::string& filename);
    /**
     * 关闭文件
     * @param fd: 文件的fd
     * @return 成功返回0，失败返回-1
     */
    virtual int Close(int fd);
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
    int Load();
    void CheckTimeoutFunc();
    std::unordered_map<int, NebdFileRecordPtr> GetRecordMap();

 private:
    /**
     * 打开指定文件并更新元数据信息
     * @param filename: 文件名
     * @param create: 为true将产生新的filerecord
     * @return: 成功返回0，失败返回-1
     */
    int OpenInternal(const std::string& fileName, bool create = false);
    /**
     * 根据文件内存记录信息关闭指定文件
     * @param fileRecord: 文件的内存记录
     * @return: 成功返回0，失败返回-1
     */
    int CloseInternal(NebdFileRecordPtr fileRecord);
    /**
     * 根据文件内存记录信息重新open文件
     * @param fileRecord: 文件的内存记录
     * @return: 成功返回0，失败返回-1
     */
    int Reopen(NebdFileRecordPtr fileRecord);
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
    // TODO(YYK) 后续封装HeartbeatManager
    // 文件心跳超时时长
    uint32_t heartbeatTimeoutS_;
    // 心跳超时检测线程的检测时间间隔
    uint32_t checkTimeoutIntervalMs_;
    // 心跳检测线程
    std::thread checkTimeoutThread_;
    // 心跳检测线程的sleeper
    InterruptibleSleeper sleeper_;
    // 当前filemanager的运行状态，true表示正在运行，false标为未运行
    std::atomic<bool> isRunning_;
    // 文件名锁，对同名文件加锁
    NameLock nameLock_;
    // fd分配器
    FdAllocator fdAlloc_;
    // TODO(YYK) 与metafilemanager整合成FileRecordManager
    // 文件信息内存记录映射表
    FileRecordMap fileRecordMap_;
    // 元数据文件持久化管理
    MetaFileManagerPtr metaFileManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_MANAGER_H_
