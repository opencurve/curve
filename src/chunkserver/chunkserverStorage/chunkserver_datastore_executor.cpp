/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 10:49:53 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <fcntl.h>
#include <gflags/gflags.h>

#include "src/chunkserver/chunkserverStorage/chunkserver_datastore_executor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"

DEFINE_int32(ChunkFileLength, 4*1024*1024, "chunk file size(Bytes)");

namespace curve {
namespace chunkserver {

CSDataStoreExecutor::CSDataStoreExecutor(std::shared_ptr<CSSfsAdaptor> sfsada, std::string path): fd_(-1) {
    filePath_ = path;
    lfs_ = sfsada;
}

CSDataStoreExecutor::~CSDataStoreExecutor() {
    lfs_->Close(fd_);
}

bool CSDataStoreExecutor::DeleteChunk() {
    return lfs_->Delete(filePath_.c_str()) == 0;
}

bool CSDataStoreExecutor::ReadChunk(char * buf, off_t offset, size_t* length) {
    int readlength = 0;
    if (CURVE_LIKELY(fd_.Valid())) {
        readlength = lfs_->Read(fd_, reinterpret_cast<void*>(buf), offset, *length);
    } else {
        bool exist = lfs_->FileExists(filePath_.c_str());
        fd_ = lfs_->Open(filePath_.c_str(), O_RDWR | O_CREAT, 0644);
        if (!exist) {
            fchmod(fd_.fd_, S_IRWXU);
            // FIXME(guangxun): eliminate hardcoded chunksize
            lfs_->Fallocate(fd_, 0, 0, FLAGS_ChunkFileLength);
        }
        if (fd_.Valid()) {
            readlength = lfs_->Read(fd_, reinterpret_cast<void*>(buf), offset, *length);
        }
    }
    return (*length = readlength);
}

bool CSDataStoreExecutor::WriteChunk(const char * buf, off_t offset, size_t length) {
    bool ret = false;
    do {
        /* valid check : if offset + length > chunkfile length, we do not allow write*/
        if (CURVE_LIKELY(fd_.Valid())) {
            ret = lfs_->Write(fd_, reinterpret_cast<const void*>(buf), offset, length);
        } else {
            bool exist = lfs_->FileExists(filePath_.c_str());
            fd_ = lfs_->Open(filePath_.c_str(), O_RDWR | O_CREAT, 0644);
            if (!exist) {
                // FIXME(guangxun): encapsulate chmod()
                fchmod(fd_.fd_, S_IRWXU);
                lfs_->Fallocate(fd_, 0, 0, FLAGS_ChunkFileLength);
            }
            if (fd_.Valid()) {
                ret = lfs_->Write(fd_, reinterpret_cast<const void*>(buf), offset, length);
                // lfs_->Fsync(fd_);
            }
        }
    } while (0);
    return ret;
}

int CSDataStoreExecutor::AioReadChunk(char * buf, off_t offset, size_t length, std::function<void(void*)> callback) {
    // TODO(tongguangxun):
    return 0;
}

int CSDataStoreExecutor::AioWriteChunk(char * buf, off_t offset, size_t length, std::function<void(void*)> callback) {
    // TODO(tongguangxun):
    return 0;
}

int CSDataStoreExecutor::ValidateChunk() {
    return 0;
}

bool CSDataStoreExecutor::CreateSnapshot(SnapshotID sid) {
    // TODO(tongguangxun): create snapshot logic
    return false;
}

bool CSDataStoreExecutor::DeleteSnapshot(SnapshotID sid) {
    return false;
}

int CSDataStoreExecutor::ReadSnapshot(SnapshotID sid, char* buff, off_t offset, size_t length) {
    return 0;
}

bool CSDataStoreExecutor::RevertSnapshot2ID(SnapshotID targetID) {
    return false;
}

}  // namespace chunkserver
}  // namespace curve
