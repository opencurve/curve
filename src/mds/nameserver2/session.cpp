/*
 * Project: curve
 * Created Date: 2018-12-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include "src/mds/nameserver2/session.h"
#include <glog/logging.h>
#include <brpc/controller.h>
#include <vector>
#include <memory>
#include <utility>
#include <chrono>   // NOLINT
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
                                const uint32_t leasetime,
                                const uint64_t createtime,
                                const uint32_t toleranceTime,
                                const SessionStatus status,
                                const std::string& clientIP) {
    protoSession_.set_sessionid(sessionid);
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
        StatusCode ret = InsertNewSessionUnlocked(fileName, clientIP);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "insert new session error, fileName = " << fileName
                       << ", clientIP = " << clientIP
                       << ", errCode = " << ret
                       << ", errName = " << StatusCode_Name(ret);
            return ret;
        }

        auto iter = sessionMap_.find(fileName);
        *protoSession = (&(iter->second))->GetProtoSession();

        LOG(INFO) << "not found old session, create new session"
                  << ", fileName = " << fileName
                  << ", new sessionId = " << iter->second.GetSessionId()
                  << ", clientIP = " << clientIP;
        return StatusCode::kOK;
    }

    Session *session = &(iter->second);

    // session存在，检查session是否过期
    if (session->IsLeaseTimeOut()) {
        // lease过期，删除过期lease，然后生成并插入新的session
        std::string oldSessionId = session->GetSessionId();
        StatusCode ret = DeleteOldSessionUnlocked(fileName, oldSessionId);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete session fail, fileName = " << fileName
                       << ", clientIP = " << clientIP
                       << ", errCode = " << ret
                       << ", errName = " << StatusCode_Name(ret);
            return ret;
        }

        ret = InsertNewSessionUnlocked(fileName, clientIP);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "insert new sessioni error, fileName = " << fileName
                       << ", clientIP = " << clientIP
                       << ", errCode = " << ret
                       << ", errName = " << StatusCode_Name(ret);
            return ret;
        }

        auto iter = sessionMap_.find(fileName);
        *protoSession = (&(iter->second))->GetProtoSession();

        LOG(INFO) << "old session lease timeout, create new session"
                  << " , fileName = " << fileName
                  << ", old sessionId = " << oldSessionId
                  << ", new sessionId = " << iter->second.GetSessionId()
                  << ", clientIP = " << clientIP;
        return StatusCode::kOK;
    } else {
        LOG(ERROR) << "file is occupied by other client, fileName = "
                   << fileName << ", clientIP = " << clientIP;
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
        LOG(ERROR) << "session not exist, fileName = " << fileName
                   << ", sessionID = " << sessionID;
        return StatusCode::kSessionNotExist;
    }

    // 检查sessionMap的sessionID是否和传入sessionID匹配
    if (iter->second.GetSessionId().compare(sessionID)) {
        LOG(ERROR) << "sessionID not same in sessionMap"
                   << ", retCode = kSessionNotExist"
                   << ", fileName = " << fileName
                   << ", rpc session id = " << sessionID
                   << ", map sessionid = " << iter->second.GetSessionId();
        return StatusCode::kSessionNotExist;
    }

    // session存在，删除session
    auto ret = DeleteOldSessionUnlocked(fileName, sessionID);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete session fail, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret;
        return ret;
    }

    LOG(INFO) << "delete session success, fileName = " << fileName
              << ", sessionID = " << sessionID;

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
        LOG(ERROR) << "session not exist, fileName = " << fileName
                   << ", sessionid = " << sessionid
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP;
        return StatusCode::kSessionNotExist;
    }

    Session *session = &(iter->second);
    StatusCode ret = StatusCode::kOK;
    session->Lock();
    do {
        // 检查sessionMap的sessionID是否和传入sessionid匹配
        if (session->GetSessionId().compare(sessionid)) {
            LOG(ERROR) << "sessionID not same in sessionMap"
                       << ", fileName = " << fileName
                       << ", rpc sessionid = " << sessionid
                       << ", map sessionid = " << session->GetSessionId()
                       << ", signature = " << signature
                       << ", clientIP = " << clientIP;
            ret = StatusCode::kSessionNotExist;
            break;
        }

        // 无论lease没有过期，只要身份校验通过，更新lease时间
        if (session->GetSessionStatus() == SessionStatus::kSessionStaled) {
            session->SetStatus(SessionStatus::kSessionOK);
            openFileNum_++;
        }
        session->UpdateLeaseTime();
    } while (0);

    session->Unlock();
    return ret;
}

bool SessionManager::isFileHasValidSession(const std::string &fileName) {
    common::ReadLockGuard rl(rwLock_);

    // 检查session是否存在
    auto iter = sessionMap_.find(fileName);
    if (iter == sessionMap_.end()) {
        return false;
    }

    // 如果file有session存在，session是否在有效期内
    Session *session = &(iter->second);
    if (session->IsLeaseTimeOut()) {
        return false;
    }

    return true;
}

uint64_t SessionManager::GetOpenFileNum() {
    return openFileNum_.load(std::memory_order_acquire);
}

// 扫描sessionMap_，如果有过期的session，把状态标记为kSessionStaled
void SessionManager::ScanSessionMap() {
    common::WriteLockGuard wl(rwLock_);

    auto iterator = sessionMap_.begin();
    while (iterator != sessionMap_.end()) {
        if (iterator->second.IsLeaseTimeOut()
        && iterator->second.GetSessionStatus() == SessionStatus::kSessionOK) {
            LOG(INFO) << "session timeout, fileName = " << iterator->first
                      << ", sessionId = " << iterator->second.GetSessionId();
            iterator->second.SetStatus(SessionStatus::kSessionStaled);
            openFileNum_--;
        }

        iterator++;
    }
}

// 扫描deleteSessionList_，把对应session从数据库中删除
void SessionManager::HandleDeleteSessionList() {
    auto iter = deleteSessionList_.begin();
    while (iter != deleteSessionList_.end()) {
        std::string sessionId = iter->GetSessionId();
        // 从数据库中删除
        auto ret = repo_->DeleteSessionRepoItem(iter->GetSessionId());
        if (ret != repo::OperationOK) {
            LOG(ERROR) << "delete session from repo fail, sessionId = "
                       << iter->GetSessionId()
                       << ", errCode = " << ret;
            continue;
        }

        iter = deleteSessionList_.erase(iter);
        LOG(INFO) << "delete session from repo and delete list"
                  << ", sessionId = " << sessionId;
    }
}

void SessionManager::UpdateRepoSesssions() {
    common::WriteLockGuard wl(rwLock_);

    auto iterator = sessionMap_.begin();
    while (iterator != sessionMap_.end()) {
        std::string sessionId = iterator->second.GetSessionId();
        SessionRepoItem sessionRepo;
        auto ret = repo_->QuerySessionRepoItem(sessionId,
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
            ret = repo_->UpdateSessionRepoItem(sessionRepo);
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

StatusCode SessionManager::InsertNewSessionUnlocked(const std::string &fileName,
                                            const std::string &clientIP) {
    Session session(leaseTime_, toleranceTime_, clientIP);

    std::string sessionId = session.GetSessionId();
    uint64_t createTime = session.GetCreateTime();
    uint32_t leaseTime = session.GetLeaseTime();

    // 持久化
    SessionRepoItem sessionRepo(fileName, sessionId, leaseTime,
                            SessionStatus::kSessionOK,
                            createTime, clientIP);
    auto ret = repo_->InsertSessionRepoItem(sessionRepo);
    if (ret != repo::OperationOK) {
        LOG(ERROR) << "insert session to repo fail, sessionId = " << sessionId
                   << ", fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret;
        return StatusCode::KInternalError;
    }

    // 插入内存组织
    auto ret2 = sessionMap_.emplace(std::pair<const std::string, Session>
                                (fileName, session));
    if (!ret2.second) {
        LOG(ERROR) << "session map add session fail, sessionId = " << sessionId
                   << ", fileName = " << fileName
                   << ", clientIP = " << clientIP;
        return StatusCode::KInternalError;
    }

    openFileNum_++;

    return StatusCode::kOK;
}

StatusCode SessionManager::DeleteOldSessionUnlocked(const std::string &fileName,
                                         const std::string &sessionId) {
    // 删除session持久化信息，
    auto ret = repo_->DeleteSessionRepoItem(sessionId);
    if (ret != repo::OperationOK) {
        LOG(ERROR) << "delete session from repo fail, sessionId = "
                   << sessionId
                   << ", fileName = " << fileName
                   << ", errCode = " << ret;
        return StatusCode::KInternalError;
    }

    // 从内存组织中删除
    auto iter = sessionMap_.find(fileName);
    if (iter != sessionMap_.end()) {
        if (iter->second.GetSessionStatus() == SessionStatus::kSessionOK) {
            openFileNum_--;
        }
    }
    sessionMap_.erase(fileName);

    return StatusCode::kOK;
}

SessionManager::SessionManager(std::shared_ptr<MdsRepo> repo) {
    repo_ = repo;
}

bool SessionManager::Init(const struct SessionOptions &sessionOptions) {
    LOG(INFO) << "init SessionManager start.";

    if (!LoadSession()) {
        LOG(ERROR) << "load session from repo fail.";
        return false;
    }
    LOG(INFO) << "load session from repo sucess.";

    leaseTime_ = sessionOptions.leaseTimeUs;
    toleranceTime_ = sessionOptions.toleranceTimeUs;
    intevalTime_ = sessionOptions.intevalTimeUs;
    LOG(INFO) << "load configuration, leaseTime_ = " << leaseTime_
              << ", toleranceTime_ = " << toleranceTime_
              << ", intevalTime_ = " << intevalTime_;

    sessionScanStop_ = false;

    if (scanLatency_.expose("session_scan", "lat") != 0) {
        LOG(ERROR) << "expose session_scan latency recorder failed";
    }

    return true;
}

void SessionManager::Start() {
    scanThread = new common::Thread(&SessionManager::SessionScanFunc, this);
    return;
}

bool SessionManager::LoadSession() {
    common::WriteLockGuard wl(rwLock_);

    openFileNum_ = 0;
    // 启动mds时从数据库中加载session信息
    std::vector<SessionRepoItem> sessionList;
    if (repo_->LoadSessionRepoItems(&sessionList) != repo::OperationOK) {
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
        if (sesssionIter->second.GetSessionStatus()
                        == SessionStatus::kSessionOK) {
            openFileNum_++;
        }
        LOG(INFO) << "load session from repo, session id = "
                  << sesssionIter->second.GetSessionId()
                  << ", filename = " << sesssionIter->first;
        sesssionIter++;
    }
    return true;
}

void SessionManager::SessionScanFunc() {
    LOG(INFO) << "start session scan thread.";

    // 周期性扫描内存中的session
    while (!sessionScanStop_) {
        // 1、先扫描过期session，过期的session状态标记为kSessionStaled
        uint64_t timeStart = ::curve::common::TimeUtility::GetTimeofDayUs();
        ScanSessionMap();
        uint64_t timeEnd = ::curve::common::TimeUtility::GetTimeofDayUs();
        scanLatency_ << (timeEnd - timeStart);

        // 2、扫描deleteSessionList_，把列表中的session从数据库中删除
        HandleDeleteSessionList();

        // 3、睡眠一段时间
        common::UniqueLock lk(exitmtx_);
        exitcv_.wait_for(lk, std::chrono::microseconds(intevalTime_),
                         [&]()->bool{ return sessionScanStop_;});
    }

    // 退出之前，过期的session状态更新，需要删除的session都从数据库中删除
    ScanSessionMap();
    HandleDeleteSessionList();

    LOG(INFO) << "stop session scan thread.";
    return;
}

void SessionManager::Stop() {
    sessionScanStop_ = true;
    LOG(INFO) << "stop SessionManager, join thread.";

    // 通过信号量快速唤醒处在睡眠过程中的线程，以实现快速退出
    exitcv_.notify_one();

    scanThread->join();

    // 退出之前，更新session在数据库的状态
    UpdateRepoSesssions();
    return;
}
}  // namespace mds
}  // namespace curve
