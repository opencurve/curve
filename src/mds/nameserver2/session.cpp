/*
 * Project: curve
 * Created Date: 2018-12-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include "src/mds/nameserver2/session.h"
#include <uuid/uuid.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <vector>
#include <utility>
#include "src/common/timeutility.h"



namespace curve {
namespace mds {
bool Session::IsLeaseTimeOut() {
    if (protoSession_.sessionstatus() == SessionStatus::kSessionStaled) {
        return true;
    }

    uint64_t currentTime = ::curve::common::TimeUtility::GetTimeofDayUs();
    return currentTime > updateTime_ + protoSession_.leasetime()
                         + toleranceTime_;
}

std::string Session::GenSessionId() {
    char buf[40] = {0};
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse(uuid, buf);
    return buf;
}

std::string Session::GenToken() {
    // TODO(hzchenwei7): 待实现，生成token
    return "token";
}

Session::Session(const Session &a) {
    this->protoSession_ = a.protoSession_;
    this->updateTime_ = a.updateTime_;
    this->toleranceTime_ = a.toleranceTime_;
    this->clientIP_ = a.clientIP_;
}

Session& Session::operator = (const Session& a) {
    this->protoSession_ = a.protoSession_;
    this->updateTime_ =  a.updateTime_;
    this->toleranceTime_ = a.toleranceTime_;
    this->clientIP_ = a.clientIP_;
    return *this;
}

Session::Session(const std::string& sessionid,
                                const std::string& token,
                                const uint32_t leasetime,
                                const uint64_t createtime,
                                const uint32_t toleranceTime,
                                const SessionStatus status,
                                const std::string& clientIP) {
    protoSession_.set_sessionid(sessionid);
    protoSession_.set_token(token);
    protoSession_.set_leasetime(leasetime);
    protoSession_.set_createtime(createtime);
    protoSession_.set_sessionstatus(status);
    updateTime_ = ::curve::common::TimeUtility::GetTimeofDayUs();
    toleranceTime_ = toleranceTime;
    clientIP_ = clientIP;
}

Session::Session(uint32_t leaseTime,
                            const uint32_t toleranceTime,
                            const std::string& clientIP) {
    uint64_t createTime = ::curve::common::TimeUtility::GetTimeofDayUs();
    protoSession_.set_sessionid(GenSessionId());
    protoSession_.set_token(GenToken());
    protoSession_.set_leasetime(leaseTime);
    protoSession_.set_createtime(createTime);
    protoSession_.set_sessionstatus(SessionStatus::kSessionOK);
    updateTime_ = createTime;
    toleranceTime_ = toleranceTime;
    clientIP_ = clientIP;
    return;
}

void Session::UpdateLeaseTime() {
    updateTime_ = common::TimeUtility::GetTimeofDayUs();
}

void Session::SetStatus(SessionStatus status) {
    protoSession_.set_sessionstatus(status);
}

std::string Session::GetClientIp() {
    return clientIP_;
}

std::string Session::GetSessionId() {
    return protoSession_.sessionid();
}

std::string Session::GetToken() {
    return protoSession_.token();
}

uint64_t Session::GetCreateTime() {
    return protoSession_.createtime();
}

uint32_t Session::GetLeaseTime() {
    return protoSession_.leasetime();
}

SessionStatus Session::GetSessionStatus() {
    return protoSession_.sessionstatus();
}

ProtoSession Session::GetProtoSession() {
    return protoSession_;
}

void Session::Lock() {
    sessionLock_.lock();
}

void Session::Unlock() {
    sessionLock_.unlock();
}

StatusCode SessionManager::InsertSession(const std::string &fileName,
                            const std::string &clientIP,
                            ProtoSession *protoSession) {
    // 对sessionmap上写锁
    common::WriteLockGuard wl(rwLock_);

    // 检查session是否存在
    auto iter = sessionMap_.find(fileName);
    if (iter == sessionMap_.end()) {
        // session不存在，生成并插入新的session
        auto ret = InsertNewSession(fileName, clientIP);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "insert new sessioni error, errCode = " << ret;
            return ret;
        }

        auto iter = sessionMap_.find(fileName);
        *protoSession = (&(iter->second))->GetProtoSession();

        return StatusCode::kOK;
    }

    Session *session = &(iter->second);

    // session存在，检查session是否过期
    if (session->IsLeaseTimeOut()) {
        // lease过期，删除过期lease，然后生成并插入新的session
        std::string oldSessionId = session->GetSessionId();
        auto ret = DeleteOldSession(fileName, oldSessionId);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete session fail, errCode = " << ret;
            return ret;
        }

        auto ret1 = InsertNewSession(fileName, clientIP);
        if (ret1 != StatusCode::kOK) {
            LOG(ERROR) << "insert new sessioni error, errCode = "
                        << ret1;
            return ret1;
        }

        auto iter = sessionMap_.find(fileName);
        *protoSession = (&(iter->second))->GetProtoSession();

        LOG(INFO) << "old session lease timeout,create new session fileName = "
                  << fileName
                  << ", old sessionId = " << oldSessionId
                  << ", new sessionId = " << iter->second.GetSessionId();
        return StatusCode::kOK;
    } else {
        LOG(ERROR) << "file is occupied by other client, fileName = "
                   << fileName;
        return StatusCode::kFileOccupied;
    }

    return StatusCode::kOK;
}

StatusCode SessionManager::DeleteSession(const std::string &fileName,
                            const std::string &sessionID) {
    // 对sessionmap上写锁
    common::WriteLockGuard wl(rwLock_);

    // 检查session是否存在
    auto iter = sessionMap_.find(fileName);
    if (iter == sessionMap_.end()) {
        // session不存在，直接返回OK
        LOG(ERROR) << "session not exist, delete session success.";
        return StatusCode::kSessionNotExist;
    }

    // 检查sessionMap的sessionID是否和传入sessionID匹配
    if (iter->second.GetSessionId().compare(sessionID)) {
        LOG(ERROR) << "sessionID not same in sessionMap, " <<
                     "delete session success，" << "rpc session id = " <<
                     sessionID << ", map sessionid = " <<
                     iter->second.GetSessionId();
        return StatusCode::kSessionNotExist;
    }

    // session存在，删除session
    auto ret = DeleteOldSession(fileName, sessionID);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete session fail, errCode = " << ret;
        return ret;
    }

    return StatusCode::kOK;
}

StatusCode SessionManager::UpdateSession(const std::string &fileName,
                        const std::string &sessionid,
                        const std::string &signature,
                        const std::string &clientIP) {
    // 对sessionmap上读锁，本操作不会对session map进行插入或者删除操作
    common::ReadLockGuard rl(rwLock_);

    // 检查session是否存在
    auto iter = sessionMap_.find(fileName);
    if (iter == sessionMap_.end()) {
        // session不存在，返回session不存在
        LOG(INFO) << "session not exist, " << fileName;
        return StatusCode::kSessionNotExist;
    }

    Session *session = &(iter->second);
    StatusCode ret = StatusCode::kOK;
    session->Lock();
    do {
        // 检查sessionMap的sessionID是否和传入sessionid匹配
        if (session->GetSessionId().compare(sessionid)) {
            LOG(ERROR) << "sessionID not same in sessionMap";
            ret = StatusCode::kSessionNotExist;
            break;
        }

        // TODO(hzchenwei7): 待实现，校验signature和token

        // 无论lease没有过期，只要身份校验通过，更新lease时间
        if (session->IsLeaseTimeOut()) {
            session->SetStatus(SessionStatus::kSessionOK);
        }
        session->UpdateLeaseTime();
    } while (0);

    session->Unlock();
    return ret;
}


// 扫描sessionMap_，把过期的session放入deleteSessionList_
StatusCode SessionManager::ScanSessionMap() {
    common::WriteLockGuard wl(rwLock_);

    auto iterator = sessionMap_.begin();
    while (iterator != sessionMap_.end()) {
        if (iterator->second.IsLeaseTimeOut()
        && iterator->second.GetSessionStatus() == SessionStatus::kSessionOK) {
            LOG(INFO) << "session timeout, fileName = " << iterator->first
            << ", sessionId = " << iterator->second.GetSessionId();
            iterator->second.SetStatus(SessionStatus::kSessionStaled);
        }

        iterator++;
    }

    return StatusCode::kOK;
}

// 扫描deleteSessionList_，把对应session从数据库中删除
StatusCode SessionManager::HandleDeleteSessionList() {
    auto iter = deleteSessionList_.begin();
    while (iter != deleteSessionList_.end()) {
        std::string sessionId = iter->GetSessionId();
        // 从数据库中删除
        auto ret = repo_->DeleteSessionRepo(iter->GetSessionId());
        if (ret != repo::OperationOK) {
            LOG(ERROR) << "delete session from repo fail, sessionId = "
                       << iter->GetSessionId()
                       << ", errCode = " << ret;
            continue;
        }

        iter = deleteSessionList_.erase(iter);
        LOG(INFO) << "delete session from repo and delete list"
                  << ", sessionId = "
                  << sessionId;
    }

    return StatusCode::kOK;
}

void SessionManager::UpdateRepoSesssions() {
    common::WriteLockGuard wl(rwLock_);

    auto iterator = sessionMap_.begin();
    while (iterator != sessionMap_.end()) {
        std::string sessionId = iterator->second.GetSessionId();
        repo::SessionRepo sessionRepo;
        auto ret = repo_->QuerySessionRepo(sessionId,
                                    &sessionRepo);
        if (ret != repo::OperationOK) {
            LOG(ERROR) << "query session from repo fail, sessionId = "
                       << sessionId << ", errCode = " << ret;
            iterator++;
            continue;
        }

        if (iterator->second.GetSessionStatus()
                                    != sessionRepo.GetSessionStatus()) {
            LOG(INFO) << "update session in repo, sessionId = " << sessionId
                      << ", memory status = "
                      << iterator->second.GetSessionStatus()
                      << ", repo status = "
                      << sessionRepo.GetSessionStatus();
            sessionRepo.SetSessionStatus(iterator->second.GetSessionStatus());
            ret = repo_->UpdateSessionRepo(sessionRepo);
            if (ret != repo::OperationOK) {
                LOG(ERROR) << "update session from repo fail, sessionId = "
                           << sessionId << ", errCode = " << ret;
                iterator++;
                continue;
            }
        }

        iterator++;
    }
}

StatusCode SessionManager::InsertNewSession(const std::string &fileName,
                                            const std::string &clientIP) {
    Session session(leaseTime_, toleranceTime_, clientIP);

    std::string sessionId = session.GetSessionId();
    std::string token = session.GetToken();
    uint64_t createTime = session.GetCreateTime();
    uint32_t leaseTime = session.GetLeaseTime();

    // 持久化
    repo::SessionRepo sessionRepo(fileName, sessionId, token, leaseTime,
                            SessionStatus::kSessionOK,
                            createTime, clientIP);
    auto ret = repo_->InsertSessionRepo(sessionRepo);
    if (ret != repo::OperationOK) {
        LOG(ERROR) << "insert session to repo fail, sessionId = "
                   << sessionId << ", errCode = " << ret;
        return StatusCode::KInternalError;
    }

    // 插入内存组织
    auto ret2 = sessionMap_.emplace(std::pair<const std::string, Session>
                                (fileName, session));
    if (!ret2.second) {
        LOG(ERROR) << "session map add session fail, sessionId = " << sessionId;
        return StatusCode::KInternalError;
    }

    return StatusCode::kOK;
}

StatusCode SessionManager::DeleteOldSession(const std::string &fileName,
                                         const std::string &sessionId) {
    // 删除session持久化信息，
    auto ret = repo_->DeleteSessionRepo(sessionId);
    if (ret != repo::OperationOK) {
        LOG(ERROR) << "delete session from repo fail, sessionId = "
                   << sessionId << ", errCode = " << ret;
        return StatusCode::KInternalError;
    }

    // 从内存组织中删除
    sessionMap_.erase(fileName);

    return StatusCode::kOK;
}

SessionManager::SessionManager(std::shared_ptr<repo::RepoInterface> repo) {
    repo_ = repo;
}

bool SessionManager::InitRepo(const std::string &dbName,
                                  const std::string &user,
                                  const std::string &url,
                                  const std::string &password) {
    if (repo_->connectDB(dbName, user, url, password) != repo::OperationOK) {
        LOG(ERROR) << "connectDB fail.";
        return false;
    } else if (repo_->createDatabase() != repo::OperationOK) {
        LOG(ERROR) << "createDatabase fail.";
        return false;
    } else if (repo_->useDataBase() != repo::OperationOK) {
        LOG(ERROR) << "useDataBase fail.";
        return false;
    } else if (repo_->createAllTables() != repo::OperationOK) {
        LOG(ERROR) << "createAllTables fail.";
        return false;
    }
    return true;
}

bool SessionManager::Init(const struct SessionOptions &sessionOptions) {
    LOG(INFO) << "init SessionManager start.";

    std::string sessionDbName = sessionOptions.sessionDbName;
    std::string sessionUser = sessionOptions.sessionUser;
    std::string sessionUrl = sessionOptions.sessionUrl;
    std::string sessionPassword = sessionOptions.sessionPassword;
    if (!InitRepo(sessionDbName, sessionUser, sessionUrl, sessionPassword)) {
        LOG(ERROR) << "init repo fail.";
        return false;
    }
    LOG(INFO) << "init repo sucess.";

    if (!LoadSession()) {
        LOG(ERROR) << "load session from repo fail.";
        return false;
    }
    LOG(INFO) << "load session from repo sucess.";

    leaseTime_ = sessionOptions.leaseTime;
    toleranceTime_ = sessionOptions.toleranceTime;
    intevalTime_ = sessionOptions.intevalTime;
    LOG(INFO) << "load configuration, leaseTime_ = " << leaseTime_
              << ", toleranceTime_ = " << toleranceTime_
              << ", intevalTime_ = " << intevalTime_;

    sessionScanStop_ = false;

    return true;
}

void SessionManager::Start() {
    scanThread = new std::thread(&SessionManager::SessionScanFunc, this);
    return;
}

bool SessionManager::LoadSession() {
    common::WriteLockGuard wl(rwLock_);

    // 启动mds时从数据库中加载session信息
    std::vector<repo::SessionRepo> sessionList;
    if (repo_->LoadSessionRepo(&sessionList) != repo::OperationOK) {
        LOG(ERROR) << "load session repo fail.";
        return false;
    }

    // SessionRepo 转化为 session
    auto repoIter = sessionList.begin();
    while (repoIter != sessionList.end()) {
        // 如果fileName不在sessionMap_，直接添加；否则，比较session的创建时间，
        // 加载创建时间更晚的session
        auto sessionMapIter = sessionMap_.find(repoIter->fileName);
        if (sessionMapIter == sessionMap_.end()) {
            Session session(repoIter->sessionID,
                                    repoIter->token,
                                    repoIter->leaseTime,
                                    repoIter->createTime,
                                    toleranceTime_,
                                    SessionStatus(repoIter->sessionStatus),
                                    repoIter->clientIP);
            sessionMap_.emplace(std::pair<const std::string, Session>
                                    (repoIter->fileName, session));
        } else if (sessionMapIter->second.GetCreateTime() >
                                                        repoIter->createTime) {
            Session session(repoIter->sessionID,
                                    repoIter->token,
                                    repoIter->leaseTime,
                                    repoIter->createTime,
                                    toleranceTime_,
                                    SessionStatus(repoIter->sessionStatus),
                                    repoIter->clientIP);
            deleteSessionList_.push_back(session);
        } else if (sessionMapIter->second.GetCreateTime()
                    < repoIter->createTime) {
            deleteSessionList_.push_back(sessionMapIter->second);
            sessionMap_.erase(sessionMapIter);
            Session session(repoIter->sessionID,
                                    repoIter->token,
                                    repoIter->leaseTime,
                                    repoIter->createTime,
                                    toleranceTime_,
                                    SessionStatus(repoIter->sessionStatus),
                                    repoIter->clientIP);
            sessionMap_.emplace(std::pair<const std::string, Session>
                                    (repoIter->fileName, session));
        } else {
            sessionMap_.clear();
            deleteSessionList_.clear();
            LOG(ERROR) << "load session fail" <<
                          ",a file have 2 session with same createTime.";
            return false;
        }
        repoIter++;
    }

    auto sesssionIter = sessionMap_.begin();
    while (sesssionIter != sessionMap_.end()) {
        LOG(INFO) << "load session from repo, session id = "
                  << sesssionIter->second.GetSessionId()
                  << ", filename = " << sesssionIter->first;
        sesssionIter++;
    }
    return true;
}

void SessionManager::SessionScanFunc() {
    LOG(INFO) << "start session scan thread.";

    // 每秒扫描一次过期的session
    while (!sessionScanStop_) {
        // 1、先扫描过期session，放到待删除队列
        ScanSessionMap();

        // 2、扫描deleteSessionList_，把session从数据库中删除
        HandleDeleteSessionList();

        // 3、睡眠一段时间
        usleep(intevalTime_);
    }

    // 退出之前，确保过期session都从数据库中删除
    ScanSessionMap();
    HandleDeleteSessionList();

    LOG(INFO) << "stop session scan thread.";
    return;
}

void SessionManager::Stop() {
    sessionScanStop_ = true;
    LOG(INFO) << "stop SessionManager, join thread.";

    // TODO(hzchenwei7): 后续通过信号量快速唤醒处在睡眠过程中的线程，
    // 以实现快速退出
    scanThread->join();

    // 更新session在数据库的状态
    UpdateRepoSesssions();
    return;
}
}  // namespace mds
}  // namespace curve
