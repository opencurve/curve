/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */
#include "src/mds/repo/repo.h"

namespace curve {
namespace repo {
// copyset operation
int Repo::InsertCopySetRepo(const CopySetRepo &cr) {
    return db_->ExecUpdate(makeSql.makeInsert(cr));
}

int Repo::LoadCopySetRepos(std::vector<CopySetRepo> *copySetList) {
    assert(copySetList != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRows(CopySetRepo{}), &res);
    if (resCode != OperationOK) {
        return resCode;
    }

    while (res->next()) {
        copySetList->push_back(
            CopySetRepo(res->getUInt("copySetID"),
                        static_cast<uint16_t>(res->getUInt("logicalPoolID")),
                        static_cast<uint64_t >(res->getUInt64("epoch")),
                        res->getString("chunkServerIDList")));
    }
    delete (res);
    return resCode;
}

int Repo::DeleteCopySetRepo(CopySetIDType id, LogicalPoolIDType lid) {
    return db_->ExecUpdate(makeSql.makeDelete(CopySetRepo(id, lid)));
}

int Repo::UpdateCopySetRepo(const curve::repo::CopySetRepo &cr) {
    return db_->ExecUpdate(makeSql.makeUpdate(cr));
}

int Repo::QueryCopySetRepo(curve::repo::CopySetIDType id,
                           LogicalPoolIDType lid,
                           curve::repo::CopySetRepo *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int status =
        db_->QueryRows(makeSql.makeQueryRow(CopySetRepo(id, lid)), &res);
    if (status != OperationOK) {
        return status;
    }
    while (res->next()) {
        repo->copySetID = res->getUInt("copySetID");
        repo->logicalPoolID =
            static_cast<uint16_t>(res->getUInt("logicalPoolID"));
        repo->epoch = static_cast<uint64_t >(res->getUInt64("epoch"));
        repo->chunkServerIDList = res->getString("chunkServerIDList");
    }

    delete (res);
    return status;
}
}  // namespace repo
}  // namespace curve
