/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:04:03 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <list>

#include "src/chunkserver/chunkserverStorage/adaptor_util.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"

namespace curve {
namespace chunkserver {

CSDataStore::CSDataStore() :
    isInited_(false),
    copysetDir_(),
    copysetStoragePool_(),
    sfsptr_(nullptr) {
}

CSDataStore::~CSDataStore() {
    UnInitInternal();
}

bool CSDataStore::Initialize(std::shared_ptr<CSSfsAdaptor> fsadaptor,
                             std::string copysetdir) {
    return InitInternal(fsadaptor, copysetdir);
}

void CSDataStore::UnInitialize() {
}

bool CSDataStore::InitInternal(std::shared_ptr<CSSfsAdaptor> fsadaptor,
                               std::string copysetdir) {
    if (isInited_) {
        return true;
    }

    if (fsadaptor == nullptr) {
        // LOG(FATAL) << "DataStore path invalid!";
        abort();
    }

    copysetDir_ = copysetdir;
    copysetStoragePool_.clear();
    sfsptr_ = fsadaptor;

    int err = 0;
    do {
        std::list<std::string> dirpaths =
            FsAdaptorUtil::ParserDirPath(copysetDir_);
        for (auto iter : dirpaths) {
            if (!sfsptr_->DirExists(iter.c_str())) {
                err = sfsptr_->Mkdir(iter.c_str(), 0755);
                if (err == -1) {
                    break;
                }
            }
        }
        if (err != -1 && !sfsptr_->DirExists((copysetDir_ + "/data").c_str())) {
            err = sfsptr_->Mkdir((copysetDir_ + "/data").c_str(), 0755);
            if (err == -1) {
                break;
            }
        }
    } while (0);
    err == -1 ? isInited_ = false : isInited_ = true;
    return isInited_;
}

bool CSDataStore::DeleteChunk(ChunkID id) {
    bool ret = false;
    do {
        auto iter = copysetStoragePool_.find(id);
        if (iter != copysetStoragePool_.end()) {
            ret = iter->second->DeleteChunk();
            copysetStoragePool_.erase(iter);
            break;
        }
        std::string filepath = ChunkID2FileName(id);
        ret = (sfsptr_->Delete(filepath.c_str()) == 0);
    } while (0);
    return ret;
}

bool CSDataStore::ReadChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t* length) {
    auto iter = copysetStoragePool_.find(id);
    if (CURVE_LIKELY(iter != copysetStoragePool_.end())) {
        return iter->second->ReadChunk(buf, offset, length);
    }
    std::string filepath = ChunkID2FileName(id);

    CSDataStoreExecutorPtr newstorage =
        CSDataStoreExecutorPtr(new CSDataStoreExecutor(sfsptr_, filepath));
    copysetStoragePool_.insert(std::make_pair(id, newstorage));
    return newstorage->ReadChunk(buf, offset, length);
}

bool CSDataStore::WriteChunk(ChunkID id,
                            const char * buf,
                            off_t offset,
                            size_t length) {
    auto iter = copysetStoragePool_.find(id);
    if (CURVE_LIKELY(iter != copysetStoragePool_.end())) {
        return iter->second->WriteChunk(buf, offset, length);
    } else {
        std::string filepath = ChunkID2FileName(id);
        CSDataStoreExecutorPtr newstorage =
            CSDataStoreExecutorPtr(new CSDataStoreExecutor(sfsptr_, filepath));
        copysetStoragePool_.insert(std::make_pair(id, newstorage));
        return newstorage->WriteChunk(buf, offset, length);
    }
}

int CSDataStore::AioReadChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t length,
                            std::function<void(void*)> callback) {
    return 0;
}

int CSDataStore::AioWriteChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t length,
                            std::function<void(void*)> callback) {
    return 0;
}

int CSDataStore::ValidateChunk(ChunkID id) {
    return 0;
}

bool CSDataStore::CreateSnapshot(ChunkID cid, SnapshotID sid) {
    return 0;
}

bool CSDataStore::DeleteSnapshot(ChunkID cid, SnapshotID sid) {
    return 0;
}

int CSDataStore::ReadSnapshot(ChunkID cid,
                            SnapshotID sid,
                            char* buff,
                            off_t offset,
                            size_t length) {
    return 0;
}

bool CSDataStore::RevertSnapshot2ID(ChunkID cid, SnapshotID targetID) {
    return 0;
}

std::string CSDataStore::ChunkID2FileName(ChunkID cid) {
    static std::string datapath = copysetDir_ + "/data/chunk_";

    return datapath + std::to_string(cid).append(".chunk");
}

void CSDataStore::UnInitInternal() {
}
}  // namespace chunkserver
}  // namespace curve
