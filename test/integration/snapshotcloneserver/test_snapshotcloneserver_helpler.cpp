/*
 * Project: curve
 * Created Date: Mon Oct 21 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"

#include <string>
#include <vector>

extern const char* kEtcdClientIpPort;
extern const char* kEtcdPeerIpPort;
extern const char* kMdsIpPort;
extern const char* kChunkServerIpPort1;
extern const char* kChunkServerIpPort2;
extern const char* kChunkServerIpPort3;
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
                    + "/SnapshotCloneService?Action=CreateSnapshot"
                    + "&Version=1&User=" + user
                    + "&File=" + fileName
                    + "&Name=" + snapName;
    LOG(INFO) << "Make snapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());
    if (ret == 0) {
        *uuidOut =  jsonObj["UUID"].asString();
    }
    return ret;
}

int CancelSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/SnapshotCloneService?Action=CancelSnapshot"
                    + "&Version=1&User=" + user
                    + "&File=" + fileName
                    + "&UUID=" + uuid;
    LOG(INFO) << "CancelSnapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());
    return ret;
}

int GetSnapshotInfo(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid,
    FileSnapshotInfo *info) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo"
                    + "&Version=1&User=" + user
                    + "&File=" + fileName
                    + "&UUID=" + uuid;
    LOG(INFO) << "GetSnapshotInfo, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj["Code"].asString());
    if (ret == 0) {
        if (jsonObj["TotalCount"].asUInt() != 1) {
            return -1;
        }
        if (jsonObj["Snapshots"].size() != 1) {
            return -1;
        }
        info->LoadFromJsonObj(jsonObj["Snapshots"][0]);
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
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo"
                    + "&Version=1&User=" + user
                    + "&File=" + fileName
                    + "&Limit=" + std::to_string(limit);
                    + "&Offset=" + std::to_string(offset);
    LOG(INFO) << "GetFileSnapshotInfo, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());

    for (int i = 0; i < jsonObj["Snapshots"].size(); i++) {
        FileSnapshotInfo info;
        info.LoadFromJsonObj(jsonObj["Snapshots"][i]);
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
                    + "/SnapshotCloneService?Action=DeleteSnapshot"
                    + "&Version=1&User=" + user
                    + "&File=" + fileName
                    + "&UUID=" + uuid;
    LOG(INFO) << "DeleteSnapshot, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());
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
                    + "/SnapshotCloneService?Action=" + action
                    + "&Version=1&User=" + user
                    + "&Source=" + src
                    + "&Destination=" + dst
                    + "&Lazy=" + (lazy ? "1" : "0");

    LOG(INFO) << "request, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());
    if (ret == 0) {
        *uuidOut =  jsonObj["UUID"].asString();
    }
    return ret;
}

int GetCloneTaskInfo(
    const std::string &user,
    const std::string &uuid,
    TaskCloneInfo *info) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/SnapshotCloneService?Action=GetCloneTasks"
                    + "&Version=1&User=" + user
                    + "&UUID=" + uuid;
    LOG(INFO) << "GetCloneTasks, url = " << url;

    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj["Code"].asString());
    if (ret == 0) {
        if (jsonObj["TotalCount"].asUInt() != 1) {
            return -1;
        }
        if (jsonObj["TaskInfos"].size() != 1) {
            return -1;
        }
        info->LoadFromJsonObj(jsonObj["TaskInfos"][0]);
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
                    + "/SnapshotCloneService?Action=GetCloneTasks"
                    + "&Version=1&User=" + user
                    + "&Limit=" + std::to_string(limit)
                    + "&Offset=" + std::to_string(offset);
    LOG(INFO) << "GetCloneTasks, url = " << url;

    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }
    ret = std::stoi(jsonObj["Code"].asString());

    for (int i = 0; i < jsonObj["TaskInfos"].size(); i++) {
        TaskCloneInfo info;
        info.LoadFromJsonObj(jsonObj["TaskInfos"][i]);
        infoVec->push_back(info);
    }
    return ret;
}

int CleanCloneTask(
    const std::string &user,
    const std::string &uuid) {
    std::string url = std::string("http://")
                    + kSnapshotCloneServerIpPort
                    + "/SnapshotCloneService?Action=CleanCloneTask"
                    + "&Version=1&User=" + user
                    + "&UUID=" + uuid;
    LOG(INFO) << "CleanCloneTask, url = " << url;
    Json::Value jsonObj;
    int ret = SendRequest(url, &jsonObj);
    if (ret < 0) {
        return ret;
    }

    ret = std::stoi(jsonObj["Code"].asString());
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
        if (info1.GetCloneInfo().GetStatus() == st) {
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

}  // namespace snapshotcloneserver
}  // namespace curve
