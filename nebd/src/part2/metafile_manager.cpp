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
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
 */

#include <fstream>
#include <utility>

#include "nebd/src/part2/metafile_manager.h"
#include "nebd/src/part2/request_executor.h"

namespace nebd {
namespace server {

NebdMetaFileManager::NebdMetaFileManager()
    : metaFilePath_("")
    , wrapper_(nullptr)
    , parser_(nullptr) {}

NebdMetaFileManager::~NebdMetaFileManager() {}

int NebdMetaFileManager::Init(const NebdMetaFileManagerOption& option) {
    metaFilePath_ = option.metaFilePath;
    wrapper_ = option.wrapper;
    parser_ = option.parser;
    int ret = LoadFileMeta();
    if (ret < 0) {
        LOG(ERROR) << "Load file meta from " << metaFilePath_ << " failed.";
        return -1;
    }
    LOG(INFO) << "Init metafilemanager success.";
    return 0;
}

int NebdMetaFileManager::UpdateFileMeta(const std::string& fileName,
                                        const NebdFileMeta& fileMeta) {
    WriteLockGuard writeLock(rwLock_);
    bool needUpdate = metaCache_.find(fileName) == metaCache_.end()
                      || fileMeta != metaCache_[fileName];
    // If the metadata information has not changed, there is no need to write a file
    if (!needUpdate) {
        return 0;
    }

    FileMetaMap tempMap = metaCache_;
    tempMap[fileName] = fileMeta;

    int res = UpdateMetaFile(tempMap);
    if (res != 0) {
        LOG(ERROR) << "Update file meta failed, fileName: " << fileName;
        return -1;
    }
    metaCache_ = std::move(tempMap);
    LOG(INFO) << "Update file meta success. "
              << "file meta: " << fileMeta;
    return 0;
}

int NebdMetaFileManager::RemoveFileMeta(const std::string& fileName) {
    WriteLockGuard writeLock(rwLock_);
    bool isExist = metaCache_.find(fileName) != metaCache_.end();
    if (!isExist) {
        return 0;
    }

    FileMetaMap tempMap = metaCache_;
    tempMap.erase(fileName);

    int res = UpdateMetaFile(tempMap);
    if (res != 0) {
        LOG(ERROR) << "Remove file meta failed, fileName: " << fileName;
        return -1;
    }
    metaCache_ = std::move(tempMap);
    LOG(INFO) << "Remove file meta success. "
              << "file name: " << fileName;
    return 0;
}

int NebdMetaFileManager::UpdateMetaFile(const FileMetaMap& fileMetas) {
    Json::Value root = parser_->ConvertFileMetasToJson(fileMetas);
    int res = AtomicWriteFile(root);
    if (res != 0) {
        LOG(ERROR) << "AtomicWriteFile fail.";
        return -1;
    }
    return 0;
}

int NebdMetaFileManager::AtomicWriteFile(const Json::Value& root) {
    // Write tmp file
    std::string tmpFilePath = metaFilePath_ + ".tmp";
    int fd = wrapper_->open(tmpFilePath.c_str(), O_CREAT|O_RDWR, 0644);
    // Open file failed
    if (fd <= 0) {
        LOG(ERROR) << "Open tmp file " << tmpFilePath << " fail";
        return -1;
    }
    // Write
    std::string jsonString = root.toStyledString();
    int writeSize = wrapper_->pwrite(fd, jsonString.c_str(),
                                     jsonString.size(), 0);
    wrapper_->close(fd);
    if (writeSize != static_cast<int>(jsonString.size())) {
        LOG(ERROR) << "Write tmp file " << tmpFilePath << " fail";
        return -1;
    }

    // Rename
    int res = wrapper_->rename(tmpFilePath.c_str(), metaFilePath_.c_str());
    if (res != 0) {
        LOG(ERROR) << "rename file " << tmpFilePath << " to "
                   << metaFilePath_ << " fail";
        return -1;
    }
    return 0;
}

int NebdMetaFileManager::LoadFileMeta() {
    ReadLockGuard readLock(rwLock_);
    FileMetaMap tempMetas;
    std::ifstream in(metaFilePath_, std::ios::binary);
    if (!in) {
        // There should be no error returned here, the file may not have been created during the first initialization
        LOG(WARNING) << "File not exist: " << metaFilePath_;
        return 0;
    }

    Json::CharReaderBuilder reader;
    Json::Value root;
    JSONCPP_STRING errs;
    bool ok = Json::parseFromStream(reader, in, &root, &errs);
    in.close();
    if (!ok) {
        LOG(ERROR) << "Parse meta file " << metaFilePath_
                   << " fail: " << errs;
        return -1;
    }

    int res = parser_->Parse(root, &tempMetas);
    if (res != 0) {
        LOG(ERROR) << "ConvertJsonToFileRecord fail";
        return -1;
    }
    metaCache_ = std::move(tempMetas);
    return 0;
}

int NebdMetaFileManager::ListFileMeta(std::vector<NebdFileMeta>* fileMetas) {
    CHECK(fileMetas != nullptr) << "fileMetas is nullptr.";
    ReadLockGuard readLock(rwLock_);
    fileMetas->clear();
    for (const auto& metaPair : metaCache_) {
        fileMetas->emplace_back(metaPair.second);
    }
    return 0;
}

int NebdMetaFileParser::Parse(Json::Value root,
                              FileMetaMap* fileMetas) {
    if (!fileMetas) {
        LOG(ERROR) << "the argument fileMetas is null pointer";
        return -1;
    }
    fileMetas->clear();
    // Check crc
    if (root[kCRC].isNull()) {
        LOG(ERROR) << "Parse json: " << root
                   << " fail, no crc";
        return -1;
    }
    uint32_t crcValue = root[kCRC].asUInt();
    root.removeMember(kCRC);
    std::string jsonString = root.toStyledString();
    uint32_t crcCalc = nebd::common::CRC32(jsonString.c_str(),
                                           jsonString.size());
    if (crcValue != crcCalc) {
        LOG(ERROR) << "Parse json: " << root
                   << " fail, crc not match";
        return -1;
    }

    // No volume field
    const auto& volumes = root[kVolumes];
    if (volumes.isNull()) {
        LOG(WARNING) << "No volumes in json: " << root;
        return 0;
    }

    for (const auto& volume : volumes) {
        std::string fileName;
        NebdFileMeta meta;

        if (volume[kFileName].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no filename";
            return -1;
        } else {
            meta.fileName = volume[kFileName].asString();
        }

        if (volume[kFd].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no fd";
            return -1;
        } else {
            meta.fd = volume[kFd].asInt();
        }

        // Except for the parts of filename and fd, they are uniformly placed in xattr
        Json::Value::Members mem = volume.getMemberNames();
        ExtendAttribute xattr;
        for (auto iter = mem.begin(); iter != mem.end(); iter++) {
            if (*iter == kFileName || *iter == kFd) {
                continue;
            }
            meta.xattr.emplace(*iter, volume[*iter].asString());
        }
        fileMetas->emplace(meta.fileName, meta);
    }
    return 0;
}

Json::Value NebdMetaFileParser::ConvertFileMetasToJson(
                        const FileMetaMap& fileMetas) {
    Json::Value volumes;
    for (const auto& meta : fileMetas) {
        Json::Value volume;
        volume[kFileName] = meta.second.fileName;
        volume[kFd] = meta.second.fd;
        for (const auto &item : meta.second.xattr) {
            volume[item.first] = item.second;
        }
        volumes.append(volume);
    }
    Json::Value root;
    root[kVolumes] = volumes;

    // Calculate crc
    std::string jsonString = root.toStyledString();
    uint32_t crc = nebd::common::CRC32(jsonString.c_str(), jsonString.size());
    root[kCRC] = crc;
    return root;
}

}  // namespace server
}  // namespace nebd
