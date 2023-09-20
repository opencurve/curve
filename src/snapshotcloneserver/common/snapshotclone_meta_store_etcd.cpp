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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/common/snapshotclone_meta_store_etcd.h"

#include <vector>
#include <string>

namespace curve {
namespace snapshotcloneserver {

int SnapshotCloneMetaStoreEtcd::Init() {
    int ret = LoadSnapshotInfos();
    if (ret < 0) {
        return -1;
    }
    ret = LoadCloneInfos();
    if (ret < 0) {
        return -1;
    }
    return 0;
}

const char kSnapPathSeprator[] = "@";

inline std::string MakeSnapshotKey(const std::string &filePath,
    const std::string &snapName) {
    return filePath + kSnapPathSeprator + snapName;
}

int SnapshotCloneMetaStoreEtcd::AddSnapshot(const SnapshotInfo &info) {
    std::string key = codec_->EncodeSnapshotKey(info.GetUuid());
    std::string value;
    bool ret = codec_->EncodeSnapshotData(info, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeSnapshotData err"
                   << ", snapInfo : " << info;
        return -1;
    }

    WriteLockGuard guard(snapInfos_mutex);
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put snapInfo into etcd err"
                   << ", errcode = " << errCode
                   << ", snapInfo : " << info;
        return -1;
    }

    auto item = std::make_shared<SnapshotInfo>(info);
    snapInfos_.emplace(info.GetUuid(), item);

    std::string snapPath = MakeSnapshotKey(info.GetFileName(),
        info.GetSnapshotName());
    snapInfosByName_.emplace(snapPath, item);
    return 0;
}

int SnapshotCloneMetaStoreEtcd::DeleteSnapshot(const UUID &uuid) {
    std::string key = codec_->EncodeSnapshotKey(uuid);
    WriteLockGuard guard(snapInfos_mutex);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete snapInfo from etcd err"
                   << ", errcode = " << errCode
                   << ", uuid = " << uuid;
        return -1;
    }
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        snapInfos_.erase(search);
        std::string snapPath = MakeSnapshotKey(search->second->GetFileName(),
            search->second->GetSnapshotName());
        snapInfosByName_.erase(snapPath);
    }
    return 0;
}

int SnapshotCloneMetaStoreEtcd::UpdateSnapshot(const SnapshotInfo &info) {
    std::string key = codec_->EncodeSnapshotKey(info.GetUuid());
    std::string value;
    bool ret = codec_->EncodeSnapshotData(info, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeSnapshotData err"
                   << ", snapInfo : " << info;
        return -1;
    }
    WriteLockGuard guard(snapInfos_mutex);
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put snapInfo into etcd err"
                   << ", errcode = " << errCode
                   << ", snapInfo : " << info;
        return -1;
    }

    std::string snapPath = MakeSnapshotKey(info.GetFileName(),
        info.GetSnapshotName());

    auto search = snapInfos_.find(info.GetUuid());
    if (search != snapInfos_.end()) {
        *(search->second) = info;
        *(snapInfosByName_[snapPath]) = info;
    } else {
        auto item = std::make_shared<SnapshotInfo>(info);
        snapInfos_.emplace(info.GetUuid(), item);
        snapInfosByName_.emplace(snapPath, item);
    }
    return 0;
}

int SnapshotCloneMetaStoreEtcd::CASSnapshot(const UUID& uuid, CASFunc cas) {
    WriteLockGuard guard(snapInfos_mutex);
    auto iter = snapInfos_.find(uuid);
    auto info = cas(iter == snapInfos_.end() ? nullptr : iter->second.get());
    if (nullptr == info) {  // Not needed to update snapshot
        return 0;
    }

    int retCode;
    std::string value;
    auto key = codec_->EncodeSnapshotKey(uuid);
    if (!codec_->EncodeSnapshotData(*info, &value)) {
        LOG(ERROR) << "EncodeSnapshotData failed, snapshotInfo: " << *info;
        return -1;
    } else if ((retCode = client_->Put(key, value)) != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put snapshotInfo into etcd failed"
                   <<", errCode: " << retCode << ", snapshotInfo: " << *info;
        return -1;
    }

    std::string snapPath = MakeSnapshotKey(info->GetFileName(),
        info->GetSnapshotName());

    if (iter != snapInfos_.end()) {
        *(iter->second) = *info;
        *(snapInfosByName_[snapPath]) = *info;
    } else {
        auto item = std::make_shared<SnapshotInfo>(*info);
        snapInfos_.emplace(uuid, item);
        snapInfosByName_.emplace(snapPath, item);
    }
    return 0;
}

int SnapshotCloneMetaStoreEtcd::GetSnapshotInfo(
    const UUID &uuid, SnapshotInfo *info) {
    ReadLockGuard guard(snapInfos_mutex);
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        *info = *(search->second);
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::GetSnapshotInfo(
    const std::string &file, const std::string &snapshotName,
    SnapshotInfo *info) {
    std::string snapshotPath = MakeSnapshotKey(file, snapshotName);
    auto search = snapInfosByName_.find(snapshotPath);
    if (search != snapInfosByName_.end()) {
        *info = *(search->second);
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::GetSnapshotList(const std::string &filename,
    std::vector<SnapshotInfo> *v) {
    ReadLockGuard guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
         it != snapInfos_.end();
         it++) {
        if (filename == it->second->GetFileName()) {
            v->push_back(*(it->second));
        }
    }
    if (v->size() != 0) {
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::GetSnapshotList(
    std::vector<SnapshotInfo> *list) {
    ReadLockGuard guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
          it != snapInfos_.end();
          it++) {
       list->push_back(*(it->second));
    }
    if (list->size() != 0) {
        return 0;
    }
    return -1;
}

uint32_t SnapshotCloneMetaStoreEtcd::GetSnapshotCount() {
    ReadLockGuard guard(snapInfos_mutex);
    return snapInfos_.size();
}

int SnapshotCloneMetaStoreEtcd::AddCloneInfo(const CloneInfo &info) {
    std::string key = codec_->EncodeCloneInfoKey(info.GetTaskId());
    std::string value;
    bool ret = codec_->EncodeCloneInfoData(info, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeCloneInfoData err"
                   << ", cloneInfo : " << info;
        return -1;
    }
    WriteLockGuard guard(cloneInfos_lock_);
    int errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put cloneInfo into etcd err"
                   << ", errcode = " << errCode
                   << ", cloneInfo : " << info;
        return -1;
    }
    cloneInfos_.emplace(info.GetTaskId(), info);
    return 0;
}

int SnapshotCloneMetaStoreEtcd::DeleteCloneInfo(const std::string &uuid) {
    std::string key = codec_->EncodeCloneInfoKey(uuid);
    WriteLockGuard guard(cloneInfos_lock_);
    int errCode = client_->Delete(key);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete cloneInfo from etcd err"
                   << ", errcode = " << errCode
                   << ", uuid = " << uuid;
        return -1;
    }
    auto search = cloneInfos_.find(uuid);
    if (search != cloneInfos_.end()) {
        cloneInfos_.erase(search);
    }
    return 0;
}

int SnapshotCloneMetaStoreEtcd::UpdateCloneInfo(const CloneInfo &info) {
    std::string key = codec_->EncodeCloneInfoKey(info.GetTaskId());
    std::string value;
    bool ret = codec_->EncodeCloneInfoData(info, &value);
    if (!ret) {
        LOG(ERROR) << "EncodeCloneInfoData err"
                   << ", cloneInfo : " << info;
        return -1;
    }
    WriteLockGuard guard(cloneInfos_lock_);
    // if old record not exist, return failed
    std::string oldValue;
    int errCode = client_->Get(key, &oldValue);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Get old cloneInfo from etcd err"
                   << ", errcode = " << errCode
                   << ", cloneInfo : " << info;
        return -1;
    }

    errCode = client_->Put(key, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put cloneInfo into etcd err"
                   << ", errcode = " << errCode
                   << ", cloneInfo : " << info;
        return -1;
    }
    auto search = cloneInfos_.find(info.GetTaskId());
    if (search != cloneInfos_.end()) {
        search->second = info;
    } else {
        LOG(ERROR) << "UpdateCloneInfo old record not exist";
        return -1;
    }
    return 0;
}

int SnapshotCloneMetaStoreEtcd::GetCloneInfo(
    const std::string &uuid, CloneInfo *info) {
    ReadLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(uuid);
    if (search != cloneInfos_.end()) {
        *info = search->second;
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::GetCloneInfoByFileName(
    const std::string &fileName, std::vector<CloneInfo> *list) {
    ReadLockGuard guard(cloneInfos_lock_);
    for (auto it = cloneInfos_.begin(); it != cloneInfos_.end(); it++) {
        if (it->second.GetDest() == fileName) {
            list->push_back(it->second);
        }
    }
    if (list->size() != 0) {
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::GetCloneInfoList(std::vector<CloneInfo> *list) {
    ReadLockGuard guard(cloneInfos_lock_);
    for (auto it = cloneInfos_.begin();
             it != cloneInfos_.end();
             it++) {
            list->push_back(it->second);
    }
    if (list->size() != 0) {
        return 0;
    }
    return -1;
}

int SnapshotCloneMetaStoreEtcd::LoadSnapshotInfos() {
    std::string startKey = SnapshotCloneCodec::GetSnapshotInfoKeyPrefix();
    std::string endKey = SnapshotCloneCodec::GetSnapshotInfoKeyEnd();
    WriteLockGuard guard(snapInfos_mutex);
    std::vector<std::string> out;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return -1;
    }
    for (int i = 0; i < out.size(); i++) {
        auto item = std::make_shared<SnapshotInfo>();
        errCode = codec_->DecodeSnapshotData(out[i], item.get());
        if (!errCode) {
            LOG(ERROR) << "DecodeSnapshotData err";
            return -1;
        }
        snapInfos_.emplace(item->GetUuid(), item);

        std::string snapPath = MakeSnapshotKey(item->GetFileName(),
                                               item->GetSnapshotName());
        snapInfosByName_.emplace(snapPath, item);
    }
    LOG(INFO) << "LoadSnapshotInfos size = " << snapInfos_.size();
    return 0;
}

int SnapshotCloneMetaStoreEtcd::LoadCloneInfos() {
    std::string startKey = SnapshotCloneCodec::GetCloneInfoKeyPrefix();
    std::string endKey = SnapshotCloneCodec::GetCloneInfoKeyEnd();
    WriteLockGuard guard(cloneInfos_lock_);
    std::vector<std::string> out;
    int errCode = client_->List(startKey, endKey, &out);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return -1;
    }
    for (int i = 0; i < out.size(); i++) {
        CloneInfo data;
        errCode = codec_->DecodeCloneInfoData(out[i], &data);
        if (!errCode) {
            LOG(ERROR) << "DecodeCloneInfoData err";
            return -1;
        }
        cloneInfos_.emplace(data.GetTaskId(), data);
    }
    LOG(INFO) << "LoadCloneInfos size = " << cloneInfos_.size();
    return 0;
}

}  // namespace snapshotcloneserver
}  // namespace curve

