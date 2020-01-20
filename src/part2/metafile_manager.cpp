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

NebdMetaFileManager::NebdMetaFileManager(const std::string& metaFilePath,
                                std::shared_ptr<common::PosixWrapper> wrapper,
                                std::shared_ptr<NebdMetaFileParser> parser)
    : metaFilePath_(metaFilePath), wrapper_(wrapper),
      parser_(parser), loaded_(false) {}

NebdMetaFileManager::~NebdMetaFileManager() {}

int NebdMetaFileManager::RemoveFileRecord(const std::string& fileName) {
    std::unique_lock<std::mutex> lock(mtx_);
    std::vector<NebdFileRecordPtr> fileRecords;
    if (ListFileRecordUnlocked(&fileRecords) != 0) {
        LOG(ERROR) << "List file record fail";
        return -1;
    }

    // 删除fileName对应的元素
    bool exist = false;
    for (auto it = fileRecords.begin(); it != fileRecords.end();) {
        if ((*it)->fileName == fileName) {
            exist = true;
            it = fileRecords.erase(it);
            break;
        } else {
            ++it;
        }
    }

    // 卷不存在返回0，不更新文件
    if (!exist) {
        LOG(INFO) << "File " << fileName << " not found";
        return 0;
    }
    return UpdateMetaFile(fileRecords);
}

int NebdMetaFileManager::UpdateFileRecord(const NebdFileRecordPtr& fileRecord) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (!fileRecord) {
        LOG(ERROR) << "The argument is a null pointer!";
        return -1;
    }

    std::vector<NebdFileRecordPtr> fileRecords;
    if (ListFileRecordUnlocked(&fileRecords) != 0) {
        LOG(ERROR) << "List file record fail";
        return -1;
    }

    bool exist = false;
    for (const auto& record : fileRecords) {
        // 如果文件已存在，则更新fd和addtion
        if (record->fileName == fileRecord->fileName) {
            // 如果fd和addition都没有变化的话直接return
            if (RecordsEqual(record, fileRecord)) {
                return 0;
            }
            record->fd = fileRecord->fd;
            record->fileInstance = fileRecord->fileInstance;
            exist = true;
            break;
        }
    }

    // 不存在的话push一个进去
    if (!exist) {
        fileRecords.emplace_back(fileRecord);
    }
    return UpdateMetaFile(fileRecords);
}

int NebdMetaFileManager::UpdateMetaFile(
            const std::vector<NebdFileRecordPtr>& fileRecords) {
    // 构建json
    Json::Value volumes;
    for (const auto& record : fileRecords) {
        Json::Value volume;
        volume[kFileName] = record->fileName;
        volume[kFd] = record->fd;
        if (record->fileInstance) {
            for (const auto item : record->fileInstance->addition) {
                volume[item.first] = item.second;
            }
        }
        volumes.append(volume);
    }
    Json::Value root;
    root[kVolumes] = volumes;

    int res = AtomicWriteFile(root);
    if (res != 0) {
        LOG(ERROR) << "AtomicWriteFile fail";
        return -1;
    }
    metaDataJson_ = root;
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

int NebdMetaFileManager::ListFileRecord(
            std::vector<NebdFileRecordPtr>* fileRecords) {
    fileRecords->clear();
    std::unique_lock<std::mutex> lock(mtx_);
    return ListFileRecordUnlocked(fileRecords);
}

int NebdMetaFileManager::ListFileRecordUnlocked(
            std::vector<NebdFileRecordPtr>* fileRecords) {
    // 判断是否从文件加载过
    if (loaded_) {
        return parser_->Parse(metaDataJson_, fileRecords);
    }
    return ListFileRecordFromFile(fileRecords);
}

int NebdMetaFileManager::ListFileRecordFromFile(
            std::vector<NebdFileRecordPtr>* fileRecords) {
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
    metaDataJson_ = root;
    loaded_ = true;
    return 0;
}

bool NebdMetaFileManager::RecordsEqual(const NebdFileRecordPtr& record1,
                                       const NebdFileRecordPtr& record2) {
    return record1->fileName == record2->fileName
        && record1->fd == record2->fd
        && record1->fileInstance->addition == record2->fileInstance->addition;
}

int NebdMetaFileParser::Parse(const Json::Value& root,
                              std::vector<NebdFileRecordPtr>* fileRecords) {
    // 没有volume字段
    const auto& volumes = root[kVolumes];
    if (volumes.isNull()) {
        LOG(ERROR) << "No volumes in json: " << root;
        return -1;
    }

    for (const auto& volume : volumes) {
        std::string fileName;
        int fd;
        auto record = std::make_shared<NebdFileRecord>();

        if (volume[kFileName].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no filename";
            return -1;
        } else {
            record->fileName = volume[kFileName].asString();
        }

        if (volume[kFd].isNull()) {
            LOG(ERROR) << "Parse json: " << root
                       << " fail, no fd";
            return -1;
        } else {
            record->fd = volume[kFd].asInt();
        }

        auto fileType = GetFileType(record->fileName);
        record->fileInstance = NebdFileInstanceFactory::GetInstance(fileType);
        if (!record->fileInstance) {
            LOG(ERROR) << "Unknown file type, filename: " << record->fileName;
            return -1;
        }

        // 除了filename和fd的部分统一放到fileinstance的addition里面
        Json::Value::Members mem = volume.getMemberNames();
        AdditionType addition;
        for (auto iter = mem.begin(); iter != mem.end(); iter++) {
            if (*iter == kFileName || *iter == kFd) {
                continue;
            }
            record->fileInstance->addition.emplace(*iter,
                                            volume[*iter].asString());
        }
        fileRecords->emplace_back(record);
    }
    return 0;
}

}  // namespace server
}  // namespace nebd
