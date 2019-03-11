/*
 * Project: curve
 * Created Date: Wed Sep 04 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/repo/repo.h"
#include <iostream>

namespace curve {
namespace repo {

std::string MakeSql::makeCondtion(const curve::repo::RepoItem &t) {
    std::map<std::string, std::string> primaryKV;
    t.getPrimaryKV(&primaryKV);
    if (primaryKV.empty()) {
        return "";
    }

    auto iter = primaryKV.begin();
    auto endIter = --primaryKV.end();
    std::string condition;
    while (iter != primaryKV.end()) {
        if (iter == endIter) {
            condition += iter->first + "=" + iter->second;
        } else {
            condition += iter->first + "=" + iter->second + " and ";
        }
        iter++;
    }

    return condition;
}

std::string MakeSql::makeInsert(const RepoItem &t) {
    std::map<std::string, std::string> kvMap;
    t.getKV(&kvMap);
    if (kvMap.empty()) {
        return "";
    }

    auto iter = kvMap.begin();
    auto endIter = --kvMap.end();
    std::string rows = "(";
    std::string values = "(";
    while (iter != kvMap.end()) {
        if (iter == endIter) {
            rows += iter->first + ")";
            values += iter->second + ")";
        } else {
            rows += iter->first + ",";
            values += iter->second + ",";
        }
        iter++;
    }

    const size_t kLen =
        InsertLen + t.getTable().size() + rows.size() + values.size() + 1;
    char res[kLen];
    snprintf(res, kLen, Insert,
             t.getTable().c_str(),
             rows.c_str(),
             values.c_str());
    return std::string(res);
}

std::string MakeSql::makeQueryRows(const RepoItem &t) {
    const size_t kLen = QueryAllLen + t.getTable().size() + 1;
    char res[kLen];
    snprintf(res, kLen, QueryAll, t.getTable().c_str());
    return std::string(res);
}

std::string MakeSql::makeQueryRow(const RepoItem &t) {
    std::string condition = makeCondtion(t);
    if (condition.empty()) {
        return "";
    }

    const size_t kLen = QueryLen + t.getTable().size() + condition.size() + 1;
    char res[kLen];
    snprintf(res, kLen, Query, t.getTable().c_str(), condition.c_str());
    return std::string(res);
}

std::string MakeSql::makeDelete(const RepoItem &t) {
    std::string condition = makeCondtion(t);
    if (condition.empty()) {
        return "";
    }

    const size_t kLen = DeleteLen + t.getTable().size() + condition.size() + 1;
    char res[kLen];
    snprintf(res, kLen, Delete, t.getTable().c_str(), condition.c_str());
    return std::string(res);
}

std::string MakeSql::makeUpdate(const RepoItem &t) {
    std::map<std::string, std::string> kvMap;
    t.getKV(&kvMap);
    if (kvMap.empty()) {
        return "";
    }

    auto iter = kvMap.begin();
    auto endIter = --kvMap.end();
    std::string newValues;
    while (iter != kvMap.end()) {
        if (iter == --kvMap.end()) {
            newValues += iter->first + "=" + iter->second;
        } else {
            newValues += iter->first + "=" + iter->second + ",";
        }
        iter++;
    }

    std::string condition = makeCondtion(t);
    if (condition.empty()) {
        return "";
    }

    const size_t kLen =
        UpdateLen + t.getTable().size() + newValues.size() + condition.size()
            + 1;
    char res[kLen];
    snprintf(res, kLen, Update,
             t.getTable().c_str(),
             newValues.c_str(),
             condition.c_str());
    return std::string(res);
}

}  // namespace repo
}  // namespace curve

