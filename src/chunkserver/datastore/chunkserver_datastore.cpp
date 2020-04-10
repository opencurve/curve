/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:04:03 pm
 * Author: yangyaokai
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <list>
#include <memory>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/filename_operator.h"

namespace curve {
namespace chunkserver {

CSDataStore::CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<ChunkfilePool> chunkfilePool,
                         const DataStoreOptions& options)
    : chunkSize_(options.chunkSize),
      pageSize_(options.pageSize),
      baseDir_(options.baseDir),
      locationLimit_(options.locationLimit),
      chunkfilePool_(chunkfilePool),
      lfs_(lfs) {
    CHECK(!baseDir_.empty()) << "Create datastore failed";
    CHECK(lfs_ != nullptr) << "Create datastore failed";
    CHECK(chunkfilePool_ != nullptr) << "Create datastore failed";
}

CSDataStore::~CSDataStore() {
}

bool CSDataStore::Initialize() {
    // 确保baseDir目录存在
    if (!lfs_->DirExists(baseDir_.c_str())) {
        int rc = lfs_->Mkdir(baseDir_.c_str());
        if (rc < 0) {
            LOG(ERROR) << "Create " << baseDir_ << " failed.";
            return false;
        }
    }

    vector<string> files;
    int rc = lfs_->List(baseDir_, &files);
    if (rc < 0) {
        LOG(ERROR) << "List " << baseDir_ << " failed.";
        return false;
    }

    // 如果之前加载过，这里要重新加载
    metaCache_.Clear();
    metric_ = std::make_shared<DataStoreMetric>();
    for (size_t i = 0; i < files.size(); ++i) {
        FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(files[i]);
        if (info.type == FileNameOperator::FileType::CHUNK) {
            // chunk文件如果还未加载，则加载到metaCache
            CSErrorCode errorCode = loadChunkFile(info.id);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load chunk file failed: " << files[i];
                return false;
            }
        } else if (info.type == FileNameOperator::FileType::SNAPSHOT) {
            string chunkFilePath = baseDir_ + "/" +
                        FileNameOperator::GenerateChunkFileName(info.id);

            // chunk文件如果不存在，打印日志
            if (!lfs_->FileExists(chunkFilePath)) {
                LOG(WARNING) << "Can't find snapshot "
                             << files[i] << "' chunk.";
                continue;
            }
            // chunk文件存在，则先加载chunk文件到metaCache
            CSErrorCode errorCode = loadChunkFile(info.id);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load chunk file failed.";
                return false;
            }

            // 加载snapshot到内存
            errorCode = metaCache_.Get(info.id)->LoadSnapshot(info.sn);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load snapshot failed.";
                return false;
            }
        } else {
            LOG(WARNING) << "Unknown file: " << files[i];
        }
    }
    LOG(INFO) << "Initialize data store success.";
    return true;
}

CSErrorCode CSDataStore::DeleteChunk(ChunkID id, SequenceNum sn) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSErrorCode errorCode = chunkFile->Delete(sn);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Delete chunk file failed."
                         << "ChunkID = " << id;
            return errorCode;
        }
        metaCache_.Remove(id);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::DeleteSnapshotChunkOrCorrectSn(
    ChunkID id, SequenceNum correctedSn) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSErrorCode errorCode = chunkFile->DeleteSnapshotOrCorrectSn(correctedSn);  // NOLINT
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Delete snapshot chunk or correct sn failed."
                         << "ChunkID = " << id
                         << ", correctedSn = " << correctedSn;
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::ReadChunk(ChunkID id,
                                   SequenceNum sn,
                                   char * buf,
                                   off_t offset,
                                   size_t length) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        return CSErrorCode::ChunkNotExistError;
    }

    CSErrorCode errorCode = chunkFile->Read(buf, offset, length);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Read chunk file failed."
                     << "ChunkID = " << id;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::ReadSnapshotChunk(ChunkID id,
                                           SequenceNum sn,
                                           char * buf,
                                           off_t offset,
                                           size_t length) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        return CSErrorCode::ChunkNotExistError;
    }
    CSErrorCode errorCode =
        chunkFile->ReadSpecifiedChunk(sn, buf, offset, length);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Read snapshot chunk failed."
                     << "ChunkID = " << id;
    }
    return errorCode;
}

CSErrorCode CSDataStore::WriteChunk(ChunkID id,
                                    SequenceNum sn,
                                    const char * buf,
                                    off_t offset,
                                    size_t length,
                                    uint32_t* cost) {
    // 请求版本号不允许为0，snapsn=0时会当做快照不存在的判断依据
    if (sn == kInvalidSeq) {
        LOG(ERROR) << "Sequence num should not be zero."
                   << "ChunkID = " << id;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // 如果chunk文件不存在，则先创建chunk文件
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
        options.metric = metric_;
        chunkFile = std::make_shared<CSChunkFile>(lfs_,
                                                  chunkfilePool_,
                                                  options);
        CSErrorCode errorCode = chunkFile->Open(true);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Create chunk file failed."
                         << "ChunkID = " << id;
            return errorCode;
        }
        // 如果有两个操作并发去创建chunk文件，
        // 那么其中一个操作产生的chunkFile会优先加入metaCache，
        // 后面的操作放弃当前产生的chunkFile使用前面产生的chunkFile
        chunkFile = metaCache_.Set(id, chunkFile);
    }
    // 写chunk文件
    CSErrorCode errorCode = chunkFile->Write(sn,
                                             buf,
                                             offset,
                                             length,
                                             cost);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Write chunk file failed."
                     << "ChunkID = " << id;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::CreateCloneChunk(ChunkID id,
                                          SequenceNum sn,
                                          SequenceNum correctedSn,
                                          ChunkSizeType size,
                                          const string& location) {
    // 检查参数的合法性
    if (size != chunkSize_
        || sn == 0
        || location.empty()
        || location.size() > locationLimit_) {
        LOG(ERROR) << "Invalid arguments."
                   << "ChunkID = " << id
                   << ", sn = " << sn
                   << ", correctedSn = " << correctedSn
                   << ", size = " << size
                   << ", location = " << location
                   << ", location limit length = " << locationLimit_;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // 如果chunk文件不存在，则先创建chunk文件
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.correctedSn = correctedSn;
        options.location = location;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
        options.metric = metric_;
        chunkFile = std::make_shared<CSChunkFile>(lfs_,
                                                  chunkfilePool_,
                                                  options);
        CSErrorCode errorCode = chunkFile->Open(true);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Create chunk file failed."
                         << "ChunkID = " << id;
            return errorCode;
        }
        // 如果有两个操作并发去创建chunk文件，
        // 那么其中一个操作产生的chunkFile会优先加入metaCache，
        // 后面的操作放弃当前产生的chunkFile使用前面产生的chunkFile
        chunkFile = metaCache_.Set(id, chunkFile);
    }
    // 判断指定参数与存在的Chunk中的信息是否相符
    // 不需要放到else当中，因为用户可能同时调用该接口
    // 参数中指定了不同版本或者位置信息，就可能并发冲突，也需要进行判断
    CSChunkInfo info;
    chunkFile->GetInfo(&info);
    if (info.location.compare(location) != 0
        || info.curSn != sn
        || info.correctedSn != correctedSn) {
        LOG(ERROR) << "Conflict chunk already exists."
                   << "sn in arg = " << sn
                   << ", correctedSn in arg = " << correctedSn
                   << ", location in arg = " << location
                   << ", sn in chunk = " << info.curSn
                   << ", location in chunk = " << info.location
                   << ", corrected sn in chunk = " << info.correctedSn;
        return CSErrorCode::ChunkConflictError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::PasteChunk(ChunkID id,
                                    const char * buf,
                                    off_t offset,
                                    size_t length) {
    auto chunkFile = metaCache_.Get(id);
    // Paste Chunk要求Chunk必须存在
    if (chunkFile == nullptr) {
        LOG(WARNING) << "Paste Chunk failed, Chunk not exists."
                     << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    CSErrorCode errcode = chunkFile->Paste(buf, offset, length);
    if (errcode != CSErrorCode::Success) {
        LOG(WARNING) << "Paste Chunk failed, Chunk not exists."
                     << "ChunkID = " << id;
        return errcode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::GetChunkInfo(ChunkID id,
                                      CSChunkInfo* chunkInfo) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(INFO) << "Get ChunkInfo failed, Chunk not exists."
                  << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    chunkFile->GetInfo(chunkInfo);
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::GetChunkHash(ChunkID id,
                                      off_t offset,
                                      size_t length,
                                      std::string* hash) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(INFO) << "Get ChunkHash failed, Chunk not exists."
                  << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    return chunkFile->GetHash(offset, length, hash);
}

DataStoreStatus CSDataStore::GetStatus() {
    DataStoreStatus status;
    status.chunkFileCount = metric_->chunkFileCount.get_value();
    status.cloneChunkCount = metric_->cloneChunkCount.get_value();
    status.snapshotCount = metric_->snapshotCount.get_value();
    return status;
}

CSErrorCode CSDataStore::loadChunkFile(ChunkID id) {
    // 如果chunk文件还未加载，则加载到metaCache当中
    if (metaCache_.Get(id) == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = 0;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
        options.metric = metric_;
        CSChunkFilePtr chunkFilePtr =
            std::make_shared<CSChunkFile>(lfs_,
                                          chunkfilePool_,
                                          options);
        CSErrorCode errorCode = chunkFilePtr->Open(false);
        if (errorCode != CSErrorCode::Success)
            return errorCode;
        metaCache_.Set(id, chunkFilePtr);
    }
    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
