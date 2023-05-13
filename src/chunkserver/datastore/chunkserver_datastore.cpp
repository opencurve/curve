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
 * File Created: Wednesday, 5th September 2018 8:04:03 pm
 * Author: yangyaokai
 */

#include <gflags/gflags.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <list>
#include <memory>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/common/location_operator.h"

namespace curve {
namespace chunkserver {

CSDataStore::CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<FilePool> chunkFilePool,
                         const DataStoreOptions& options)
    : chunkSize_(options.chunkSize),
      pageSize_(options.pageSize),
      locationLimit_(options.locationLimit),
      baseDir_(options.baseDir),
      chunkFilePool_(chunkFilePool),
      lfs_(lfs),
      enableOdsyncWhenOpenChunkFile_(options.enableOdsyncWhenOpenChunkFile) {
    CHECK(!baseDir_.empty()) << "Create datastore failed";
    CHECK(lfs_ != nullptr) << "Create datastore failed";
    CHECK(chunkFilePool_ != nullptr) << "Create datastore failed";
}

CSDataStore::~CSDataStore() {
}

bool CSDataStore::Initialize() {
    // Make sure the baseDir directory exists
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

    // If loaded before, reload here
    metaCache_.Clear();
    metric_ = std::make_shared<DataStoreMetric>();
    for (size_t i = 0; i < files.size(); ++i) {
        FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(files[i]);
        if (info.type == FileNameOperator::FileType::CHUNK) {
            // If the chunk file has not been loaded yet, load it to metaCache
            CSErrorCode errorCode = loadChunkFile(info.id);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load chunk file failed: " << files[i];
                return false;
            }
        } else if (info.type == FileNameOperator::FileType::SNAPSHOT) {
            string chunkFilePath = baseDir_ + "/" +
                        FileNameOperator::GenerateChunkFileName(info.id);

            // If the chunk file does not exist, print the log
            if (!lfs_->FileExists(chunkFilePath)) {
                LOG(WARNING) << "Can't find snapshot "
                             << files[i] << "' chunk.";
                continue;
            }
            // If the chunk file exists, load the chunk file to metaCache first
            CSErrorCode errorCode = loadChunkFile(info.id);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load chunk file failed.";
                return false;
            }

            // Load snapshot to memory
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
    (void)sn;
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

CSErrorCode CSDataStore::ReadChunkMetaPage(ChunkID id, SequenceNum sn,
                                           char * buf) {
    (void)sn;
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        return CSErrorCode::ChunkNotExistError;
    }

    CSErrorCode errorCode = chunkFile->ReadMetaPage(buf);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Read chunk meta page failed."
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

CSErrorCode CSDataStore::CreateChunkFile(const ChunkOptions & options,
                                         CSChunkFilePtr* chunkFile) {
        if (!options.location.empty() &&
            options.location.size() > locationLimit_) {
            LOG(ERROR) << "Location is too long."
                       << "ChunkID = " << options.id
                       << ", location = " << options.location
                       << ", location size = " << options.location.size()
                       << ", location limit size = " << locationLimit_;
            return CSErrorCode::InvalidArgError;
        }
        auto tempChunkFile = std::make_shared<CSChunkFile>(lfs_,
                                                  chunkFilePool_,
                                                  options);
        CSErrorCode errorCode = tempChunkFile->Open(true);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Create chunk file failed."
                         << "ChunkID = " << options.id
                         << ", ErrorCode = " << errorCode;
            return errorCode;
        }
        // If there are two operations concurrently to create a chunk file,
        // Then the chunkFile generated by one of the operations will be added
        // to metaCache first, the subsequent operation abandons the currently
        // generated chunkFile and uses the previously generated chunkFile
        *chunkFile = metaCache_.Set(options.id, tempChunkFile);
        return CSErrorCode::Success;
}


CSErrorCode CSDataStore::WriteChunk(ChunkID id,
                            SequenceNum sn,
                            const butil::IOBuf& buf,
                            off_t offset,
                            size_t length,
                            uint32_t* cost,
                            const std::string & cloneSourceLocation)  {
    // The requested sequence number is not allowed to be 0, when snapsn=0,
    // it will be used as the basis for judging that the snapshot does not exist
    if (sn == kInvalidSeq) {
        LOG(ERROR) << "Sequence num should not be zero."
                   << "ChunkID = " << id;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // If the chunk file does not exist, create the chunk file first
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.location = cloneSourceLocation;
        options.pageSize = pageSize_;
        options.metric = metric_;
        options.enableOdsyncWhenOpenChunkFile = enableOdsyncWhenOpenChunkFile_;
        CSErrorCode errorCode = CreateChunkFile(options, &chunkFile);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
    }
    // write chunk file
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

CSErrorCode CSDataStore::SyncChunk(ChunkID id) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(WARNING) << "Sync chunk not exist, ChunkID = " << id;
        return CSErrorCode::Success;
    }
    CSErrorCode errorCode = chunkFile->Sync();
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Sync chunk file failed."
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
    // Check the validity of the parameters
    if (size != chunkSize_
        || sn == kInvalidSeq
        || location.empty()) {
        LOG(ERROR) << "Invalid arguments."
                   << "ChunkID = " << id
                   << ", sn = " << sn
                   << ", correctedSn = " << correctedSn
                   << ", size = " << size
                   << ", location = " << location;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // If the chunk file does not exist, create the chunk file first
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
        CSErrorCode errorCode = CreateChunkFile(options, &chunkFile);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
    }
    // Determine whether the specified parameters match the information
    // in the existing Chunk
    // No need to put in else, because users may call this interface at the
    // same time
    // If different sequence or location information are specified in the
    // parameters, there may be concurrent conflicts, and judgments are also
    // required
    CSChunkInfo info;
    chunkFile->GetInfo(&info);
    if (info.location.compare(location) != 0
        || info.curSn != sn
        || info.correctedSn != correctedSn) {
        LOG(WARNING) << "Conflict chunk already exists."
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
    // Paste Chunk requires Chunk must exist
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
    // If the chunk file has not been loaded yet, load it into metaCache
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
                                          chunkFilePool_,
                                          options);
        CSErrorCode errorCode = chunkFilePtr->Open(false);
        if (errorCode != CSErrorCode::Success)
            return errorCode;
        metaCache_.Set(id, chunkFilePtr);
    }
    return CSErrorCode::Success;
}

ChunkMap CSDataStore::GetChunkMap() {
    return metaCache_.GetMap();
}

}  // namespace chunkserver
}  // namespace curve
