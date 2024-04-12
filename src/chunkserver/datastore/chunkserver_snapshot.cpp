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
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 */

#include <errno.h>
#include <memory>
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"

namespace curve {
namespace chunkserver {

void SnapshotMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &damaged, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    uint32_t bits = bitmap->Size();
    memcpy(buf + len, &bits, sizeof(bits));
    len += sizeof(bits);
    size_t bitmapBytes = (bits + 8 - 1) / 8;
    memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
    len += bitmapBytes;
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode SnapshotMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    len += sizeof(version);
    memcpy(&damaged, buf + len, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    uint32_t bits = 0;
    memcpy(&bits, buf + len, sizeof(bits));
    len += sizeof(bits);
    bitmap = std::make_shared<Bitmap>(bits, buf + len);
    size_t bitmapBytes = (bitmap->Size() + 8 - 1) / 8;
    len += bitmapBytes;
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // Verify crc, return an error code if the verification fails
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;
    }

    // TODO(yyk) judge version compatibility, simple processing at present,
    // detailed implementation later
    if (version != FORMAT_VERSION) {
        LOG(ERROR) << "File format version incompatible."
                    << "file version: "
                    << static_cast<uint32_t>(version)
                    << ", format version: "
                    << static_cast<uint32_t>(FORMAT_VERSION);
        return CSErrorCode::IncompatibleError;
    }
    return CSErrorCode::Success;
}

SnapshotMetaPage::SnapshotMetaPage(const SnapshotMetaPage& metaPage) {
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
}

SnapshotMetaPage& SnapshotMetaPage::operator =(
    const SnapshotMetaPage& metaPage) {
    if (this == &metaPage)
        return *this;
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
    return *this;
}

CSSnapshot::CSSnapshot(std::shared_ptr<LocalFileSystem> lfs,
                       std::shared_ptr<FilePool> chunkFilePool,
                       const ChunkOptions& options)
    : fd_(-1),
      chunkId_(options.id),
      size_(options.chunkSize),
      blockSize_(options.blockSize),
      metaPageSize_(options.metaPageSize),
      baseDir_(options.baseDir),
      lfs_(lfs),
      chunkFilePool_(chunkFilePool),
      metric_(options.metric) {
    CHECK(!baseDir_.empty()) << "Create snapshot failed";
    CHECK(lfs_ != nullptr) << "Create snapshot failed";
    uint32_t bits = size_ / blockSize_;
    metaPage_.bitmap = std::make_shared<Bitmap>(bits);
    metaPage_.sn = options.sn;
    if (metric_ != nullptr) {
        metric_->snapshotCount << 1;
    }
}

CSSnapshot::~CSSnapshot() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
    }

    if (metric_ != nullptr) {
        metric_->snapshotCount << -1;
    }
}

CSErrorCode CSSnapshot::Open(bool createFile) {
    string snapshotPath = path();
    // Create a new file, if the snapshot file already exists,
    // no need to create it
    // The existence of snapshot files may be caused by the following conditions
    // getchunk succeeded, but failed later in stat or loadmetapage,
    // when the download is opened again;
    if (createFile
        && !lfs_->FileExists(snapshotPath)
        && metaPage_.sn > 0) {
        std::unique_ptr<char[]> buf(new char[metaPageSize_]);
        memset(buf.get(), 0, metaPageSize_);
        metaPage_.encode(buf.get());
        int ret = chunkFilePool_->GetFile(snapshotPath, buf.get());
        if (ret != 0) {
            LOG(ERROR) << "Error occured when create snapshot."
                   << " filepath = " << snapshotPath;
            return ret == -ENOSPC ? CSErrorCode::NoSpaceError :
             CSErrorCode::InternalError;
        }
    }
    int rc = lfs_->Open(snapshotPath, O_RDWR|O_NOATIME|O_DSYNC);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when opening file."
                   << " filepath = "<< snapshotPath;
        return CSErrorCode::InternalError;
    }
    fd_ = rc;
    struct stat fileInfo;
    rc = lfs_->Fstat(fd_, &fileInfo);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when stating file."
                   << " filepath = " << snapshotPath;
        return CSErrorCode::InternalError;
    }
    if (fileInfo.st_size != fileSize()) {
        LOG(ERROR) << "Wrong file size."
                   << " filepath = " << snapshotPath
                   << ",filesize = " << fileInfo.st_size;
        return CSErrorCode::FileFormatError;
    }
    return loadMetaPage();
}

CSErrorCode CSSnapshot::Read(char * buf, off_t offset, size_t length) {
    // TODO(yyk) Do you need to compare the bit state of the offset?
    int rc = readData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading snapshot."
                   << " filepath = "<< path();
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Delete() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkFilePool_->RecycleFile(path());
    if (ret < 0)
        return CSErrorCode::InternalError;
    return CSErrorCode::Success;
}

SequenceNum CSSnapshot::GetSn() const {
    return metaPage_.sn;
}

std::shared_ptr<const Bitmap> CSSnapshot::GetPageStatus() const {
    return metaPage_.bitmap;
}

CSErrorCode CSSnapshot::Write(const char * buf, off_t offset, size_t length) {
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return rc == -ENOSPC ? CSErrorCode::NoSpaceError :
         CSErrorCode::InternalError;
    }
    uint32_t pageBeginIndex = offset / blockSize_;
    uint32_t pageEndIndex = (offset + length - 1) / blockSize_;
    for (uint32_t i = pageBeginIndex; i <= pageEndIndex; ++i) {
        dirtyPages_.insert(i);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Flush() {
    SnapshotMetaPage tempMeta = metaPage_;
    for (auto pageIndex : dirtyPages_) {
        tempMeta.bitmap->Set(pageIndex);
    }
    CSErrorCode errorCode = updateMetaPage(&tempMeta);
    if (errorCode == CSErrorCode::Success)
        metaPage_.bitmap = tempMeta.bitmap;
    dirtyPages_.clear();
    return errorCode;
}

CSErrorCode CSSnapshot::updateMetaPage(SnapshotMetaPage* metaPage) {
    std::unique_ptr<char[]> buf(new char[metaPageSize_]);
    memset(buf.get(), 0, metaPageSize_);
    metaPage->encode(buf.get());
    int rc = writeMetaPage(buf.get());
    if (rc < 0) {
        LOG(ERROR) << "Update metapage failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::loadMetaPage() {
    std::unique_ptr<char[]> buf(new char[metaPageSize_]);
    memset(buf.get(), 0, metaPageSize_);
    int rc = readMetaPage(buf.get());
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading metaPage_."
                   << " filepath = " << path();
        return CSErrorCode::InternalError;
    }
    return metaPage_.decode(buf.get());
}

}  // namespace chunkserver
}  // namespace curve
