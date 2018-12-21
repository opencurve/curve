/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:04:03 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <list>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/filename_operator.h"

namespace curve {
namespace chunkserver {

CSDataStore::CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<ChunkfilePool> ChunkfilePool,
                         const DataStoreOptions& options)
    : chunkSize_(options.chunkSize),
      pageSize_(options.pageSize),
      baseDir_(options.baseDir),
      chunkfilePool_(ChunkfilePool),
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
        CSErrorCode errorCode = chunkFile->Delete();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete chunk file failed."
                        << "ChunkID = " << id;
            return errorCode;
        }
        metaCache_.Remove(id);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::DeleteSnapshotChunk(ChunkID id,
                                             SequenceNum snapshotSn) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSErrorCode errorCode = chunkFile->DeleteSnapshot(snapshotSn);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete snapshot chunk failed."
                        << "ChunkID = " << id
                        << ", SnapshotSn = " << snapshotSn;
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
        LOG(ERROR) << "Read chunk file failed."
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
    CSChunkInfo info;
    chunkFile->GetInfo(&info);
    CSErrorCode errorCode =
        chunkFile->ReadSpecifiedChunk(sn, buf, offset, length);
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Read snapshot chunk failed."
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
    auto chunkFile = metaCache_.Get(id);
    // 如果chunk文件不存在，则先创建chunk文件
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
        chunkFile = std::make_shared<CSChunkFile>(lfs_,
                                                  chunkfilePool_,
                                                  options);
        CSErrorCode errorCode = chunkFile->Open(true);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Create chunk file failed."
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
        LOG(ERROR) << "Write chunk file failed."
                   << "ChunkID = " << id;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::GetChunkInfo(ChunkID id,
                                      vector<SequenceNum>* sns) {
    sns->clear();
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSChunkInfo info;
        chunkFile->GetInfo(&info);
        sns->push_back(info.curSn);
        if (info.snapSn > 0)
            sns->push_back(info.snapSn);
    }
    return CSErrorCode::Success;
}

inline CSErrorCode CSDataStore::loadChunkFile(ChunkID id) {
    // 如果chunk文件还未加载，则加载到metaCache当中
    if (metaCache_.Get(id) == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = 0;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
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
