/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/repo/repo.h"

namespace curve {
namespace repo {
// zone operation
int Repo::InsertZoneRepo(const ZoneRepo &zr) {
    return db_->ExecUpdate(makeSql.makeInsert(zr));
}

int Repo::LoadZoneRepos(std::vector<ZoneRepo> *zonelist) {
    assert(zonelist != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRows(ZoneRepo{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }
    while (res->next()) {
        zonelist->push_back(*new ZoneRepo(
            static_cast<uint32_t>(res->getInt("zoneID")),
            res->getString("zoneName"),
            static_cast<uint16_t>(res->getInt("poolID")),
            res->getString("desc")));
    }

    delete (res);
    return resCode;
}

int Repo::DeleteZoneRepo(ZoneIDType id) {
    return db_->ExecUpdate(makeSql.makeDelete(ZoneRepo(id)));
}

int Repo::UpdateZoneRepo(const curve::repo::ZoneRepo &zr) {
    return db_->ExecUpdate(makeSql.makeUpdate(zr));
}

int Repo::QueryZoneRepo(curve::repo::ZoneIDType id,
                        curve::repo::ZoneRepo *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRow(ZoneRepo(id)), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        repo->zoneID = res->getUInt("zoneID");
        repo->zoneName = res->getString("zoneName");
        repo->poolID = static_cast<uint16_t>(res->getUInt("poolID"));
        repo->desc = res->getString("desc");
    }

    delete (res);
    return resCode;
}
}  // namespace repo
}  // namespace curve
