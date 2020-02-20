/*
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include <fstream>

#include "src/part2/metafile_manager.h"
#include "src/part2/util.h"
#include "src/part2/request_executor.h"

namespace nebd {
namespace server {

NebdMetaFileManager::NebdMetaFileManager(
    const std::string& metaFilePath,
    std::shared_ptr<common::PosixWrapper> wrapper,
    std::shared_ptr<NebdMetaFileParser> parser)
    : metaFilePath_(metaFilePath)
    , wrapper_(wrapper)
    , parser_(parser) {}

NebdMetaFileManager::~NebdMetaFileManager() {}

int NebdMetaFileManager::UpdateMetaFile(const FileRecordMap& fileRecords) {
    Json::Value root = parser_->ConvertFileRecordsToJson(fileRecords);

    int res = AtomicWriteFile(root);
    if (res != 0) {
        LOG(ERROR) << "AtomicWriteFile fail";
        return -1;
    }
    return 0;
}

int NebdMetaFileManager::AtomicWriteFile(const Json::Value& root) {
    // 写入tmp文件
    std::string tmpFilePath = metaFilePath_ + ".tmp";
    int fd = wrapper_->open(tmpFilePath.c_str(), O_CREAT|O_RDWR, 0644);
    // open文件失败
    if (fd <= 0) {
        LOG(ERROR) << "Open tmp file " << tmpFilePath << " fail";
        return -1;
    }
    // 写入
    std::string jsonString = root.toStyledString();
    int writeSize = wrapper_->pwrite(fd, jsonString.c_str(),
                                     jsonString.size(), 0);
    wrapper_->close(fd);
    if (writeSize != jsonString.size()) {
        LOG(ERROR) << "Write tmp file " << tmpFilePath << " fail";
        return -1;
    }

    // 重命名
    int res = wrapper_->rename(tmpFilePath.c_str(), metaFilePath_.c_str());
    if (res != 0) {
        LOG(ERROR) << "rename file " << tmpFilePath << " to "
                   << metaFilePath_ << " fail";
        return -1;
    }
    return 0;
}

int NebdMetaFileManager::ListFileRecord(FileRecordMap* fileRecords) {
    CHECK(fileRecords != nullptr) << "fileRecords is null.";
    fileRecords->clear();
    std::ifstream in(metaFilePath_, std::ios::binary);
    if (!in) {
        // 这里不应该返回错误，第一次初始化的时候文件可能还未创建
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

    int res = parser_->Parse(root, fileRecords);
    if (res != 0) {
        LOG(ERROR) << "ConvertJsonToFileRecord fail";
        return -1;
    }
    return 0;
}

int NebdMetaFileParser::Parse(Json::Value root,
                              FileRecordMap* fileRecords) {
    if (!fileRecords) {
        LOG(ERROR) << "the argument fileRecords is null pointer";
        return -1;
    }
    fileRecords->clear();
    // 检验crc
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


    // 没有volume字段
    const auto& volumes = root[kVolumes];
    if (volumes.isNull()) {
        LOG(WARNING) << "No volumes in json: " << root;
        return 0;
    }

    for (const auto& volume : volumes) {
        std::string fileName;
        int fd;
        NebdFileRecord record;

        if (volume[kFileName].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no filename";
            return -1;
        } else {
            record.fileName = volume[kFileName].asString();
        }

        if (volume[kFd].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no fd";
            return -1;
        } else {
            record.fd = volume[kFd].asInt();
        }

        auto fileType = GetFileType(record.fileName);
        record.fileInstance = NebdFileInstanceFactory::GetInstance(fileType);
        if (!record.fileInstance) {
            LOG(ERROR) << "Unknown file type, filename: " << record.fileName;
            return -1;
        }

        // 除了filename和fd的部分统一放到fileinstance的addition里面
        Json::Value::Members mem = volume.getMemberNames();
        AdditionType addition;
        for (auto iter = mem.begin(); iter != mem.end(); iter++) {
            if (*iter == kFileName || *iter == kFd) {
                continue;
            }
            record.fileInstance->addition.emplace(
                *iter, volume[*iter].asString());
        }
        fileRecords->emplace(record.fd, record);
    }
    return 0;
}

Json::Value NebdMetaFileParser::ConvertFileRecordsToJson(
                        const FileRecordMap& fileRecords) {
    Json::Value volumes;
    for (const auto& record : fileRecords) {
        Json::Value volume;
        volume[kFileName] = record.second.fileName;
        volume[kFd] = record.second.fd;
        if (record.second.fileInstance) {
            for (const auto item : record.second.fileInstance->addition) {
                volume[item.first] = item.second;
            }
        }
        volumes.append(volume);
    }
    Json::Value root;
    root[kVolumes] = volumes;

    // 计算crc
    std::string jsonString = root.toStyledString();
    uint32_t crc = nebd::common::CRC32(jsonString.c_str(), jsonString.size());
    root[kCRC] = crc;
    return root;
}

}  // namespace server
}  // namespace nebd
