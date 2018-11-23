/*
 * Project: curve
 * Created Date: Thu Oct 25 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_item.h"

#include <string>

#include "json/json.h"

namespace curve {
namespace mds {
namespace topology {

bool LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
    const std::string &jsonStr,
    LogicalPoolType type,
    RedundanceAndPlaceMentPolicy *rap) {
    Json::Reader reader;
    Json::Value rapJson;
    if (!reader.parse(jsonStr, rapJson)) {
        return false;
    }

    switch (type) {
        case LogicalPoolType::PAGEFILE: {
            if (!rapJson["replicaNum"].isNull()) {
                rap->pageFileRAP.replicaNum = rapJson["replicaNum"].asInt();
            } else {
                return false;
            }
            if (!rapJson["copysetNum"].isNull()) {
                rap->pageFileRAP.copysetNum = rapJson["copysetNum"].asInt();
            } else {
                return false;
            }
            if (!rapJson["zoneNum"].isNull()) {
                rap->pageFileRAP.zoneNum = rapJson["zoneNum"].asInt();
            } else {
                return false;
            }
            break;
        }
        case LogicalPoolType::APPENDFILE: {
            // TODO(xuchaojie): it is not done.
            return false;
        }
        case LogicalPoolType::APPENDECFILE: {
            // TODO(xuchaojie): it is not done.
            return false;
        }
        default: {
            return false;
        }
    }
    return true;
}

bool LogicalPool::TransUserPolicyFromJsonStr(
    const std::string &jsonStr, LogicalPoolType type, UserPolicy *policy) {
    // TODO(xuchaojie): to finish it.
    return true;
}

bool LogicalPool::SetRedundanceAndPlaceMentPolicyByJson(
    const std::string &jsonStr) {
    return LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
               jsonStr,
               GetLogicalPoolType(),
               &rap_);
}

std::string LogicalPool::GetRedundanceAndPlaceMentPolicyJsonStr() const {
    std::string rapStr;
    Json::Value rapJson;
    switch (GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE : {
            rapJson["replicaNum"] = rap_.pageFileRAP.replicaNum;
            rapJson["copysetNum"] = rap_.pageFileRAP.copysetNum;
            rapJson["zoneNum"] = rap_.pageFileRAP.zoneNum;
            rapStr = rapJson.toStyledString();
            break;
        }
        case LogicalPoolType::APPENDFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        case LogicalPoolType::APPENDECFILE : {
            // TODO(xuchaojie): fix it
            break;
        }
        default:
            break;
    }
    return rapStr;
}


bool LogicalPool::SetUserPolicyByJson(const std::string &jsonStr) {
    return LogicalPool::TransUserPolicyFromJsonStr(
               jsonStr,
               GetLogicalPoolType(),
               &policy_);
}

std::string LogicalPool::GetUserPolicyJsonStr() const {
    // TODO(xuchaojie): to fix it
    return "{\"policy\" : 1}";
}


std::string CopySetInfo::GetCopySetMembersStr() const {
    Json::Value copysetMemJson;
    for (ChunkServerIdType id : peers_) {
        copysetMemJson.append(id);
    }
    std::string chunkServerListStr = copysetMemJson.toStyledString();
    return chunkServerListStr;
}

bool CopySetInfo::SetCopySetMembersByJson(const std::string &jsonStr) {
    Json::Reader reader;
    Json::Value copysetMemJson;
    if (!reader.parse(jsonStr, copysetMemJson)) {
        return false;
    }
    peers_.clear();
    for (int i = 0; i < copysetMemJson.size(); i++) {
        if (copysetMemJson[i].isInt()) {
            peers_.insert(copysetMemJson[i].asInt());
        } else {
            return false;
        }
    }
    return true;
}


}  // namespace topology
}  // namespace mds
}  // namespace curve
