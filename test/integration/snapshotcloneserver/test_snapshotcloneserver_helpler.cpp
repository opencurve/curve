/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Mon Oct 21 2019
 * Author: xuchaojie
 */

#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"

#include <string>
#include <vector>

extern const char* kSnapshotCloneServerIpPort;


namespace curve {
namespace snapshotcloneserver {

brpc::Channel channel_;

int SendRequest(const std::string &url, Json::Value *jsonObj) {
    brpc::ChannelOptions option;
    option.protocol = "http";
    if (channel_.Init(url.c_str(), "", &option) != 0) {
        LOG(ERROR) << "Fail to init channel"
               << std::endl;
        return -1;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(20000);
    cntl.http_request().uri() = url.c_str();

    channel_.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
		return -1;
    }

    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();
    LOG(INFO) << data;
    Json::Reader jsonReader;
    if (!jsonReader.parse(data, *jsonObj)) {
        LOG(ERROR) << "parse json fail, data = " << data;
        return -1;
    }
    return 0;
}

int MakeSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &snapName,
    std::string *uuidOut) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr+ "=" + kCreateSnapshotAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kFileStr + "=" + fileName + "&"
                    + kNameStr + "=" + snapName;
    LOG(INFO) << "Make snapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    if (ret == 0) {
        *uuidOut =  jsonObj[kUUIDStr].asString();
    }
    return ret;
}

int CancelSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" + kCancelSnapshotAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kFileStr + "=" + fileName + "&"
                    + kUUIDStr + "=" + uuid;

    LOG(INFO) << "CancelSnapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    return ret;
}

int GetSnapshotInfo(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid,
    FileSnapshotInfo *info) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kGetFileSnapshotInfoAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kFileStr + "=" + fileName + "&"
                    + kUUIDStr + "=" + uuid;
    LOG(INFO) << "GetSnapshotInfo, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj[kCodeStr].asString());
    if (ret == 0) {
        if (jsonObj[kTotalCountStr].asUInt() != 1) {
            return -1;
        }
        if (jsonObj[kSnapshotsStr].size() != 1) {
            return -1;
        }
        info->LoadFromJsonObj(jsonObj[kSnapshotsStr][0]);
    }
    return ret;
}

int ListFileSnapshotInfo(
    const std::string &user,
    const std::string &fileName,
    int limit,
    int offset,
    std::vector<FileSnapshotInfo> *infoVec) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kGetFileSnapshotInfoAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kFileStr + "=" + fileName + "&"
                    + kLimitStr + "=" + std::to_string(limit)
                    + kOffsetStr + "=" + std::to_string(offset);
    LOG(INFO) << "GetFileSnapshotInfo, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());

    for (int i = 0; i < jsonObj[kSnapshotsStr].size(); i++) {
        FileSnapshotInfo info;
        info.LoadFromJsonObj(jsonObj[kSnapshotsStr][i]);
        infoVec->push_back(info);
    }

    return ret;
}

int DeleteSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kDeleteSnapshotAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kFileStr + "=" + fileName + "&"
                    + kUUIDStr + "=" + uuid;
    LOG(INFO) << "DeleteSnapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    return ret;
}

int CloneOrRecover(
    const std::string &action,
    const std::string &user,
    const std::string &src,
    const std::string &dst,
    bool lazy,
    std::string *uuidOut) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" + action + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kSourceStr + "=" + src + "&"
                    + kDestinationStr + "=" + dst + "&"
                    + kLazyStr + "=" + (lazy ? "1" : "0");

    LOG(INFO) << "request, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    *uuidOut =  jsonObj[kUUIDStr].asString();
    return ret;
}

int Flatten(
    const std::string &user,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kFlattenAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kUUIDStr + "=" + uuid;
    LOG(INFO) << "request, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    return ret;
}

int GetCloneTaskInfo(
    const std::string &user,
    const std::string &uuid,
    TaskCloneInfo *info) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kGetCloneTasksAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kUUIDStr + "=" + uuid;
    LOG(INFO) << "GetCloneTasks, url = " << url;

    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj[kCodeStr].asString());
    if (ret == 0) {
        if (jsonObj[kTotalCountStr].asUInt() != 1) {
            return -1;
        }
        if (jsonObj[kTaskInfosStr].size() != 1) {
            return -1;
        }
        info->LoadFromJsonObj(jsonObj[kTaskInfosStr][0]);
    }
    return ret;
}

int ListCloneTaskInfo(
    const std::string &user,
    int limit,
    int offset,
    std::vector<TaskCloneInfo> *infoVec) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kGetCloneTasksAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kLimitStr + "=" + std::to_string(limit)
                    + kOffsetStr + "=" + std::to_string(offset);
    LOG(INFO) << "GetCloneTasks, url = " << url;

    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj[kCodeStr].asString());

    for (int i = 0; i < jsonObj[kTaskInfosStr].size(); i++) {
        TaskCloneInfo info;
        info.LoadFromJsonObj(jsonObj[kTaskInfosStr][i]);
        infoVec->push_back(info);
    }
    return ret;
}

int CleanCloneTask(
    const std::string &user,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/" + kServiceName + "?"
                    + kActionStr + "=" +kCleanCloneTaskAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kUUIDStr + "=" + uuid;
    LOG(INFO) << "CleanCloneTask, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj[kCodeStr].asString());
    return ret;
}

bool CheckSnapshotSuccess(
    const std::string &user,
    const std::string &file,
    const std::string &uuid) {
    bool success = false;
    for (int i = 0; i < 600; i++) {
        FileSnapshotInfo info1;
        int retCode = GetSnapshotInfo(
            user, file, uuid, &info1);
        if (retCode != 0) {
            LOG(INFO) << "GetSnapshotInfo fail, retCode = " << retCode;
            break;
        }
        if (info1.GetSnapshotInfo().GetStatus() == Status::pending) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            continue;
        } else if (info1.GetSnapshotInfo().GetStatus() == Status::done) {
            success = true;
            break;
        } else {
            LOG(ERROR) << "Snapshot Fail On status = "
                   << static_cast<int>(info1.GetSnapshotInfo().GetStatus());
            break;
        }
    }
    return success;
}

int DeleteAndCheckSnapshotSuccess(
    const std::string &user,
    const std::string &file,
    const std::string &uuid) {
    int retCode = DeleteSnapshot(user, file, uuid);
    if (retCode != 0) {
        return retCode;
    }
    for (int i = 0; i < 600; i++) {
        std::this_thread::sleep_for(std::chrono::seconds(3));

        FileSnapshotInfo info1;
        int retCode = GetSnapshotInfo(
            user, file, uuid, &info1);
        if (kErrCodeFileNotExist == retCode) {
            return 0;
        }
        if ((info1.GetSnapshotInfo().GetStatus() != Status::deleting) &&
            (info1.GetSnapshotInfo().GetStatus() != Status::errorDeleting)) {
            LOG(ERROR) << "DeleteSnapshot has error.";
            return -1;
        }
    }
    return -1;
}

bool CheckCloneOrRecoverSuccess(
    const std::string &user,
    const std::string &uuid,
    bool isClone) {
    bool success = false;
    for (int i = 0; i < 600; i++) {
        TaskCloneInfo info1;
        int retCode = GetCloneTaskInfo(
            user, uuid, &info1);
        if (retCode != 0) {
            break;
        }
        CloneStatus st;
        if (isClone) {
            st = CloneStatus::cloning;
        } else {
            st = CloneStatus::recovering;
        }
        if (info1.GetCloneInfo().GetStatus() == st ||
            info1.GetCloneInfo().GetStatus() == CloneStatus::retrying) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            continue;
        } else if (info1.GetCloneInfo().GetStatus() == CloneStatus::done) {
            success = true;
            break;
        } else {
            LOG(ERROR) << "Clone Fail On status = "
                   << static_cast<int>(info1.GetCloneInfo().GetStatus());
            break;
        }
    }
    return success;
}

bool WaitMetaInstalledSuccess(
    const std::string &user,
    const std::string &uuid,
    bool isClone) {
    bool success = false;
    for (int i = 0; i < 600; i++) {
        TaskCloneInfo info1;
        int retCode = GetCloneTaskInfo(
            user, uuid, &info1);
        if (retCode != 0) {
            break;
        }
        CloneStatus st;
        if (isClone) {
            st = CloneStatus::cloning;
        } else {
            st = CloneStatus::recovering;
        }
        if (info1.GetCloneInfo().GetStatus() == st) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        } else if (info1.GetCloneInfo().GetStatus() ==
            CloneStatus::metaInstalled) {
            success = true;
            break;
        } else {
            LOG(ERROR) << "Clone Fail On status = "
                   << static_cast<int>(info1.GetCloneInfo().GetStatus());
            break;
        }
    }
    return success;
}

}  // namespace snapshotcloneserver
}  // namespace curve
