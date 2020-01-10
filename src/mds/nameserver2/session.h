/*
 * Project: curve
 * Created Date: 2018-12-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_SESSION_H_
#define SRC_MDS_NAMESERVER2_SESSION_H_

#include <bvar/bvar.h>
#include <map>
#include <list>
#include <string>
#include <unordered_map>
#include <memory>

#include "proto/nameserver2.pb.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/uuid.h"
#include "src/common/interruptible_sleeper.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/dao/mdsRepo.h"

using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {

struct SessionOptions {
    // session过期时间，单位us
    uint32_t leaseTimeUs;
    // 能够容忍的client和mds之间的时钟不同步的时间，单位us
    uint32_t toleranceTimeUs;
    // session后台扫描线程扫描间隔时间，单位us
    uint32_t intevalTimeUs;
};

/**
  * @brief session在内存中的体现形式
  * @detail
  *    -  sesssion在内存中的体现，以<filename, Session>的map方式组织起来
  */
class Session {
 public:
    bool IsLeaseTimeOut();
    inline std::string GenSessionId() {
         return common::UUIDGenerator().GenerateUUID();
    }

    Session(const Session &);

    Session(const std::string& sessionid,
                  const uint32_t leasetime,
                  const uint64_t createtime,
                  const uint32_t toleranceTime,
                  const SessionStatus status,
                  const std::string& clientIP);

    Session(uint32_t leaseTime, const uint32_t toleranceTime,
                     const std::string& clientIP);

    void UpdateLeaseTime();
    void SetStatus(SessionStatus status);

    std::string GetClientIp();
    std::string GetSessionId();
    uint64_t GetCreateTime();
    uint32_t GetLeaseTime();
    SessionStatus GetSessionStatus();
    ProtoSession GetProtoSession();

    void Lock();
    void Unlock();

    Session& operator = (const Session &a);

 private:
    // session最新更新时间，单位us
    uint64_t updateTime_;
    // 能够容忍的client和mds之间的时钟不同步的时间，单位us
    uint32_t toleranceTime_;
    common::Mutex sessionLock_;
    // proto中定义的session结构，可用来返回给client
    ProtoSession protoSession_;
    // client的ip
    std::string clientIP_;
};

class SessionManager {
 public:
    explicit SessionManager(std::shared_ptr<MdsRepo> repo);

    /**
     *  @brief SessionManager初始化，参数初始化，repo初始化，从持久化加载session
     *  @param sessionOptions: 初始化所需参数
     *  @return 初始化是否成功
     */
    bool Init(const struct SessionOptions &sessionOptions);

    /**
     *  @brief SessionManager Start，启动session后台扫描线程
     *  @param
     *  @return
     */
    void Start();

    /**
     *  @brief SessionManager停止，join等待后台扫描线程停止
     *  @param
     *  @return
     */
    void Stop();

    /**
     *  @brief 插入session，session不存在时，直接插入；
     *         session存在且在有效期，返回session被占用kFileOccupied
     *         session存在且不在有效期，删除旧session，插入新的session
     *         如果持久化失败，返回StatusCode::KInternalError
     *  @param fileName: 文件名
     *         clientIP：clientIP
     *         protoSession：返回生成的session
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode InsertSession(const std::string &fileName,
                           const std::string &clientIP,
                           ProtoSession *protoSession);

    /**
     *  @brief 删除session，
     *         如果session不存在，返回kSessionNotExist
     *         如果从数据库删除失败，StatusCode::KInternalError
     *  @param fileName: 文件名
     *         sessionID：sessionid
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode DeleteSession(const std::string &fileName,
                           const std::string &sessionID);

    /**
     *  @brief 检查session是否有效，更新有效的session的有效期
     *         如果session不存在，或者sessionid不匹配，返回kSessionNotExist
     *  @param filename: 文件名
     *         sessionid：sessionid
     *         signature：用来验证client的身份
     *         clientIP：clientIP
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode UpdateSession(const std::string &filename,
                           const std::string &sessionid,
                           const std::string &signature,
                           const std::string &clientIP);

    /**
     *  @brief 文件是否有在有效期内的session
     *  @param filename: 文件名
     *  @return 文件存在有效的session返回true，否则返回false
     */
    bool isFileHasValidSession(const std::string &fileName);

    /**
     *  @brief 获取已经open的文件个数
     *  @param
     *  @return 已经open的文件个数
     */
    uint64_t GetOpenFileNum();

 private:
    // 启动mds时调用，从数据库中加载session信息到内存，返回调用是否成功
    bool LoadSession();

    /**
     *  @brief 后台扫描线程周期扫描任务，更新过期session状态，删除要删除的session
     *  @param
     *  @return
     */
    void SessionScanFunc();

    /**
     *  @brief 扫描sessionMap_，如果有过期的session，把状态标记为kSessionStaled
     *  @param
     *  @return
     */
    void ScanSessionMap();

    /**
     *  @brief 把待删除队列里的session从数据库中删除
     *  @param
     *  @return
     */
    void HandleDeleteSessionList();

    /**
     *  @brief 更新session在内存的状态到数据库
     *  @param
     *  @return
     */
    void UpdateRepoSesssions();

    /**
     *  @brief 向内存和数据库插入一条新的session记录，数据库操作失败，返回
     *         StatusCode::KInternalError
     *  @param fileName：文件名
     *         clientIP: client的IP
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode InsertNewSessionUnlocked(const std::string &fileName,
                                 const std::string &clientIP);

    /**
     *  @brief 从内存和数据库删除一条旧的session记录，数据库操作失败，返回
     *         StatusCode::KInternalError
     *  @param fileName：文件名
     *         sessionId:
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode DeleteOldSessionUnlocked(const std::string &fileName,
                        const std::string &sessionId);

    // 控制后台扫描线程是否需要停止扫描
    curve::common::Atomic<bool> sessionScanStop_;

    // session的后台扫描线程，扫描回收过期的session
    common::Thread *scanThread;

    // 对sessionMap_进行操作时，需要进行加锁
    curve::common::RWLock rwLock_;

    // session的内存组织，初始化时从持久化介质进行加载
    std::unordered_map<std::string, Session> sessionMap_;

    // session的待删除列表，过期session先添加到这个列表中，后续再从持久化介质中删除
    std::list<Session> deleteSessionList_;

    // session进行持久化的repo
    std::shared_ptr<MdsRepo> repo_;

    // session的租约的有效时间，session manager初始化的时候设置
    uint32_t leaseTime_;

    // 可以容忍的client和mds的因时钟不同步造成的session过期时间差异
    uint32_t toleranceTime_;

    // session的后台线程扫描频率，session manager初始化时设置
    uint32_t intevalTime_;

    InterruptibleSleeper sleeper_;

    curve::common::Atomic<uint64_t> openFileNum_;

    // 定时lock住sessiontable，扫描的latency
    ::bvar::LatencyRecorder   scanLatency_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_SESSION_H_

