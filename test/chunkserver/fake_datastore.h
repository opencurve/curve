/*
 * Project: curve
 * Created Date: 18-11-14
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_FAKE_DATASTORE_H_
#define TEST_CHUNKSERVER_FAKE_DATASTORE_H_

#include <memory>
#include <string>
#include <set>
#include <vector>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

class FakeCSDataStore : public CSDataStore {
 public:
    FakeCSDataStore(DataStoreOptions options,
                    std::shared_ptr<LocalFileSystem> fs) :
        CSDataStore(fs,
                    std::make_shared<ChunkfilePool>(fs),
                    options) {
        chunk_ = new (std::nothrow) char[options.chunkSize];
        ::memset(chunk_, 0, options.chunkSize);
        sn_ = 0;
        snapDeleteFlag_ = false;
        error_ = CSErrorCode::Success;
        chunkSize_ = options.chunkSize;
    }
    virtual ~FakeCSDataStore() {
        delete chunk_;
        chunk_ = nullptr;
    }

    bool Initialize() override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return false;
        }
        return true;
    }

    CSErrorCode DeleteChunk(ChunkID id, SequenceNum sn) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (chunkIds_.find(id) != chunkIds_.end()) {
            chunkIds_.erase(id);
            return CSErrorCode::Success;
        } else {
            return CSErrorCode::ChunkNotExistError;
        }
    }

    CSErrorCode DeleteSnapshotChunkOrCorrectSn(
        ChunkID id, SequenceNum correctedSn) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (snapDeleteFlag_ == false) {
            snapDeleteFlag_ = true;
            return CSErrorCode::Success;
        } else {
            snapDeleteFlag_ = false;
            return CSErrorCode::ChunkNotExistError;
        }
    }

    CSErrorCode ReadChunk(ChunkID id,
                          SequenceNum sn,
                          char *buf,
                          off_t offset,
                          size_t length) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (chunkIds_.find(id) == chunkIds_.end()) {
            return CSErrorCode::ChunkNotExistError;
        }
        ::memcpy(buf, chunk_+offset, length);
        if (HasInjectError()) {
            return CSErrorCode::InternalError;
        }
        return CSErrorCode::Success;
    }

    CSErrorCode ReadSnapshotChunk(ChunkID id,
                                  SequenceNum sn,
                                  char *buf,
                                  off_t offset,
                                  size_t length) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (chunkIds_.find(id) == chunkIds_.end()) {
            return CSErrorCode::ChunkNotExistError;
        }
        ::memcpy(buf, chunk_+offset, length);
        return CSErrorCode::Success;
    }

    CSErrorCode WriteChunk(ChunkID id,
                           SequenceNum sn,
                           const char *buf,
                           off_t offset,
                           size_t length,
                           uint32_t *cost,
                           const std::string & csl = "") override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        ::memcpy(chunk_+offset, buf, length);
        *cost = length;
        chunkIds_.insert(id);
        sn_ = sn;
        return CSErrorCode::Success;
    }

    CSErrorCode CreateCloneChunk(ChunkID id,
                                 SequenceNum sn,
                                 SequenceNum correctedSn,
                                 ChunkSizeType size,
                                 const string& location) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        chunkIds_.insert(id);
        sn_ = sn;
        return CSErrorCode::Success;
    }

    CSErrorCode PasteChunk(ChunkID id,
                           const char * buf,
                           off_t offset,
                           size_t length) {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (chunkIds_.find(id) == chunkIds_.end()) {
            return CSErrorCode::ChunkNotExistError;
        }
        ::memcpy(chunk_+offset, buf, length);
        return CSErrorCode::Success;
    }

    CSErrorCode GetChunkInfo(ChunkID id,
                             CSChunkInfo* info) override {
        CSErrorCode errorCode = HasInjectError();
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        if (chunkIds_.find(id) != chunkIds_.end()) {
            info->curSn = sn_;
            info->snapSn = 0;
            return CSErrorCode::Success;
        } else {
            return CSErrorCode::ChunkNotExistError;
        }
    }

    CSErrorCode GetChunkHash(ChunkID id,
                             off_t offset,
                             size_t length,
                             std::string *hash) {
        uint32_t crc32c = 0;
        if (chunkIds_.find(id) != chunkIds_.end()) {
            crc32c = curve::common::CRC32(chunk_ + offset, length);
            *hash = std::to_string(crc32c);
            return CSErrorCode::Success;
        } else {
            return CSErrorCode::ChunkNotExistError;
        }
    }

    void InjectError(CSErrorCode errorCode = CSErrorCode::InternalError) {
        error_ = errorCode;
    }

    CSErrorCode HasInjectError() {
        CSErrorCode errorCode = error_;
        if (errorCode == CSErrorCode::Success) {
            return error_;
        } else {
            // 注入错误自动恢复
            error_ = CSErrorCode::Success;
            return errorCode;
        }
    }

 private:
    char *chunk_;
    std::set<ChunkID> chunkIds_;
    bool snapDeleteFlag_;
    SequenceNum sn_;
    CSErrorCode error_;
    uint32_t chunkSize_;
};

class FakeChunkfilePool : public ChunkfilePool {
 public:
    explicit FakeChunkfilePool(std::shared_ptr<LocalFileSystem> lfs)
        : ChunkfilePool(lfs) {}
    ~FakeChunkfilePool() {}

    bool Initialize(const ChunkfilePoolOptions &cfop) {
        LOG(INFO) << "FakeChunkfilePool init success";
        return true;
    }
    int GetChunk(const std::string &chunkpath, char *metapage) { return 0; }
    int RecycleChunk(const std::string &chunkpath) { return 0; }
    size_t Size() { return 4; }
    void UnInitialize() { }
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_FAKE_DATASTORE_H_
