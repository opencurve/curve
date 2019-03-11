/*
 * Project: curve
 * Created Date: 2018-12-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_SESSION_H_
#define SRC_MDS_NAMESERVER2_SESSION_H_

#include <map>
#include <list>
#include <string>
#include <mutex>  //NOLINT
#include <atomic>
#include <thread> //NOLINT
#include "proto/nameserver2.pb.h"
#include "src/common/rw_lock.h"
#include "src/common/uuid.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/repo/repo.h"


namespace curve {
namespace mds {

struct SessionOptions {
    std::string sessionDbName;
    std::string sessionUser;
    std::string sessionUrl;
    std::string sessionPassword;
    uint32_t leaseTime;
    uint32_t toleranceTime;
    uint32_t intevalTime;
};

class Session {
 public:
    bool IsLeaseTimeOut();
    inline std::string GenSessionId() {
         return common::UUIDGenerator().GenerateUUID();
    }
    std::string GenToken();

    Session(const Session &);

    Session(const std::string& sessionid,
                  const std::string& token,
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
    std::string GetToken();
    uint64_t GetCreateTime();
    uint32_t GetLeaseTime();
    SessionStatus GetSessionStatus();
    ProtoSession GetProtoSession();

    void Lock();
    void Unlock();

    Session& operator = (const Session &a);

 private:
    uint64_t updateTime_;
    uint32_t toleranceTime_;
    std::mutex sessionLock_;
    ProtoSession protoSession_;
    std::string clientIP_;
};

class SessionManager {
 public:
    explicit SessionManager(std::shared_ptr<repo::RepoInterface> repo);

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
     *         session存在且在有效期，更新session有效期
     *         session存在其不在有效期，删除旧session，插入新的session
     *  @param fileName: 文件名
     *         clientIP：clientIP
     *         protoSession：返回生成的session
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode InsertSession(const std::string &fileName,
                           const std::string &clientIP,
                           ProtoSession *protoSession);

    /**
     *  @brief 删除session，如果session过期或者不存在，也返回成功
     *  @param fileName: 文件名
     *         sessionID：sessionid
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode DeleteSession(const std::string &fileName,
                           const std::string &sessionID);

    /**
     *  @brief 检查session是否有效，更新有效的session的有效期
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

 private:
    bool LoadSession();

    bool InitRepo(const std::string &dbName,
                                  const std::string &user,
                                  const std::string &url,
                                  const std::string &password);

    void SessionScanFunc();

    StatusCode ScanSessionMap();

    StatusCode HandleDeleteSessionList();

    void UpdateRepoSesssions();

    StatusCode InsertNewSession(const std::string &fileName,
                                 const std::string &clientIP);

    StatusCode DeleteOldSession(const std::string &fileName,
                        const std::string &sessionId);

    // 控制后台扫描线程是否需要停止扫描
    std::atomic_bool sessionScanStop_;

    // session的后台扫描线程，扫描回收过期的session
    std::thread *scanThread;

    // 对sessionMap_进行操作时，需要进行加锁
    curve::common::RWLock rwLock_;

    // session的内存组织，初始化时从持久化介质进行加载
    std::unordered_map<std::string, Session> sessionMap_;

    // session的待删除列表，过期session先添加到这个列表中，后续再从持久化介质中删除
    std::list<Session> deleteSessionList_;

    // session进行持久化的repo
    std::shared_ptr<repo::RepoInterface> repo_;

    // session的租约的有效时间，session manager初始化的时候设置
    uint32_t leaseTime_;

    // 可以容忍的client和mds的因时钟不同步造成的session过期时间差异
    uint32_t toleranceTime_;

    // session的后台线程扫描频率，session manager初始化时设置
    uint32_t intevalTime_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_SESSION_H_

