/*
 * Project: curve
 * Created Date: 2018-12-06
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include "src/mds/repo/repo.h"

// TODO(hzchenwei7): 对session的数据库操作加锁

namespace curve {
namespace repo {
int Repo::InsertSessionRepo(const SessionRepo &r) {
    return db_->ExecUpdate(makeSql.makeInsert(r));
}

int Repo::LoadSessionRepo(std::vector<SessionRepo> *sessionList) {
    assert(sessionList != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRows(SessionRepo{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        sessionList->push_back(
            SessionRepo(res->getString("fileName"),
                        res->getString("sessionID"),
                        res->getString("token"),
                        res->getUInt("leaseTime"),
                        static_cast<uint8_t>(res->getUInt("sessionStatus")),
                        res->getUInt64("createTime"),
                        res->getString("clientIP")));
    }

    delete (res);
    return OperationOK;
}

int Repo::DeleteSessionRepo(const std::string &sessionID) {
    return db_->ExecUpdate(makeSql.makeDelete(SessionRepo(sessionID)));
}

int Repo::UpdateSessionRepo(const SessionRepo &repo) {
    return db_->ExecUpdate(makeSql.makeUpdate(repo));
}

int Repo::QuerySessionRepo(const std::string &sessionID, SessionRepo *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRow(SessionRepo(sessionID)),
                                 &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    if (res->next()) {
        repo->fileName = res->getString("fileName");
        repo->sessionID = res->getString("sessionID");
        repo->token = res->getString("token");
        repo->leaseTime = res->getUInt("leaseTime");
        repo->sessionStatus =
                        static_cast<uint8_t>(res->getUInt("sessionStatus"));
        repo->createTime = res->getUInt64("createTime");
        repo->clientIP = res->getString("clientIP");
    }

    delete (res);
    return OperationOK;
}
}  // namespace repo
}  // namespace curve
