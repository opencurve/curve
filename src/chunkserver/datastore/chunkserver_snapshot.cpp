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

#include <memory>
#include "src/common/bitmap.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"
#include "src/fs/wrap_posix.h"

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
      metric_(options.metric),
      cloneNo_ (options.cloneNo) {
    CHECK(!baseDir_.empty()) << "Create snapshot failed";
    CHECK(lfs_ != nullptr) << "Create snapshot failed";

    //if the blockSize == 0, than use the default size 4096
    if (0 == blockSize_) {
        blockSize_ = 4096; //default block size is 4096
        DVLOG(3) << "CSSnapshot() blockSize_ is 0, use default size 4096";
    }

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
            return CSErrorCode::InternalError;
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

CSErrorCode CSSnapshot::ReadRanges(char * buf, off_t offset, size_t length, std::vector<BitRange>& ranges) {
    off_t readOff;
    size_t readSize;

    for (auto& range : ranges) {
        readOff = range.beginIndex * blockSize_;
        readSize = (range.endIndex - range.beginIndex + 1) * blockSize_;
        DVLOG(9) << "ReadRanges: readOff " << readOff << ", readSize " << readSize
                  << ", offset " << offset << ",  length " << length
                  << ", range.beginIndex " << range.beginIndex << ", range.endIndex " << range.endIndex
                  << ", chunkid " << chunkId_ << ", sn " << metaPage_.sn;
#ifdef MEMORY_SANITY_CHECK
        // check if memory is out of range
        assert((readOff - offset) + readSize <= length);
        assert((readOff - offset) >= 0);
#endif
        int rc = readData(buf + (readOff - offset),
                          readOff,
                          readSize);
        if (rc < 0) {
            LOG(ERROR) << "Read snap chunk file failed. "
                       << "ChunkID: " << chunkId_
                       << ", snap sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
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
        return CSErrorCode::InternalError;
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

CSSnapshots::~CSSnapshots() {
    for (int i = 0; i < snapshots_.size(); i++) {
        delete snapshots_[i];
    }

    snapshots_.clear();
}

std::vector<CSSnapshot*>::iterator CSSnapshots::find(SequenceNum sn) {
    std::vector<CSSnapshot*>::iterator it;
    for (it = snapshots_.begin(); it != snapshots_.end(); it++) {
        if ((*it)->GetSn() == sn) {
            return it;
        }

        if ((*it)->GetSn() > sn) {
            break;
        }
    }

    return snapshots_.end();
}

bool CSSnapshots::insert(CSSnapshot* s) {
    std::vector<CSSnapshot*>::iterator it;
    for (it = snapshots_.begin(); it != snapshots_.end(); it++) {
        if ((*it)->GetSn() == s->GetSn()) {
            return false;
        }

        if ((*it)->GetSn() > s->GetSn()) {
            break;
        }
    }

    snapshots_.insert(it, s);
    return true;
}

CSSnapshot *CSSnapshots::pop(SequenceNum sn) {
    std::vector<CSSnapshot*>::iterator it = find(sn);
    if (it != snapshots_.end()) {
        CSSnapshot* snap = *it;
        snapshots_.erase(it);
        return snap;
    }

    return nullptr;
}

bool CSSnapshots::contains(SequenceNum sn) const {
    for (int i = 0; i < snapshots_.size(); i++) {
        SequenceNum n = snapshots_[i]->GetSn();
        if (n == sn) return true;
        if (n > sn) break;
    }

    return false;
}

SequenceNum CSSnapshots::getCurrentSn() const {
    if (snapshots_.empty()) {
        return 0;
    }

    return (*snapshots_.rbegin())->GetSn();
}

CSSnapshot* CSSnapshots::getCurrentSnapshot() {
    if (snapshots_.empty()) {
        return nullptr;
    }

    return *snapshots_.rbegin();
}

/**
 * Assuming timeline of snapshots, from older to newer:
 *   prev -> curr -> next
*/
CSErrorCode CSSnapshots::Delete(CSChunkFile* chunkf, SequenceNum snapSn, std::shared_ptr<SnapContext> ctx) {
    // snapshot chunk not exists on disk.
    if (!this->contains(snapSn)) {
        return CSErrorCode::Success;
    }

    CSErrorCode errorCode;
    SequenceNum prev = ctx->getPrev(snapSn);
    if (prev == 0) {
        // snapSn is oldest snapshot.  Delete snap chunk directly.
        CSSnapshot *snapshot_ = pop(snapSn);
        errorCode = snapshot_->Delete();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete snapshot failed."
                    << "ChunkID: " << snapshot_->chunkId_
                    << ",snapshot sn: " << snapshot_->GetSn();
            delete snapshot_;
            return errorCode;
        }

        delete snapshot_;
        return CSErrorCode::Success;
    }

    // we need to merge data from current snap chunk to `prev' snap chunk.
    if (!this->contains(prev)) {
        // No cow was generated for snap `prev', set current as `prev'.
        errorCode = Move(snapSn, prev);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }

        return chunkf->loadSnapshot(prev);
    }

    // do merge if both snap chunk `prev' and `curr' exists.
    return Merge(snapSn, prev);
}

/**
 * This method is called with the following guarantees:
 * 1. the chunk file exists;
 * 2. the snapshot chunk (COW file) might *not* exists.
 *
 * The `offset' and `length' is converted to per-page access.
 * Each page is being tested with presence in sorted snapshot files
 * after `sn' until a hit.  If all missed, mark it into
 * `clearRanges' so that caller will read the data from chunk.
 */
CSErrorCode CSSnapshots::Read(SequenceNum sn, char * buf, off_t offset, size_t length, vector<BitRange>* clearRanges) {
    // contains COW with seqnum >= sn, in ascending order
    std::vector<CSSnapshot*> snapshots;
    uint32_t blockBeginIndex = offset / blockSize_;
    uint32_t blockEndIndex = (offset + length - 1) / blockSize_;

    std::copy_if(snapshots_.begin(), snapshots_.end(),
                 std::back_inserter(snapshots),
                 [&](CSSnapshot* s) { return s->GetSn() >= sn; });
    if (snapshots.empty()) {
        BitRange clearRange;
        clearRange.beginIndex = blockBeginIndex;
        clearRange.endIndex = blockEndIndex;
        clearRanges->push_back(clearRange);
        return CSErrorCode::Success;
    }

    // indicate whether given page has COW
    std::unique_ptr<Bitmap> snapBitmap(new Bitmap(blockEndIndex+1));

    for (auto snapshot: snapshots) {
        const auto it = snapshot->GetPageStatus();
        std::unique_ptr<Bitmap> curBitmap(new Bitmap(blockEndIndex+1));
        for (uint32_t i = blockBeginIndex; i <= blockEndIndex; i++) {
            if (!it->Test(i)) continue;  // page not in current COW
            if (snapBitmap->Test(i)) continue; // page already hit in previous COW

            curBitmap->Set(i);  // current copy have to read those set in `curBitmap'
            snapBitmap->Set(i); // further copy must ignore those set in `snapBitmap'
        }

        std::vector<BitRange> copiedRange;
        curBitmap->Divide(blockBeginIndex,
                          blockEndIndex,
                          nullptr,
                          &copiedRange);

        string rangInfo = "";
        for (auto range: copiedRange) {
            rangInfo += "(" + std::to_string(range.beginIndex) + "," + std::to_string(range.endIndex) + ")";
        }
        DLOG(INFO) << "Read from snapshot: " << snapshot->GetSn()
                  << ", buf " << static_cast<const void*>(buf)
                  << ", offset " << offset << ", length " << length 
                  << ", copiedRange: " << rangInfo;

        CSErrorCode errorCode = snapshot->ReadRanges(buf, offset, length, copiedRange);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
    }

    snapBitmap->Divide(blockBeginIndex,
                       blockEndIndex,
                       clearRanges,
                       nullptr);
    return CSErrorCode::Success;
}

/*
    Given the sn and Bit Range range to search to
    get the 
    notInRanges
    and objInfos, which indicate that which snapshot the obj lies in
    and when the snap ptr is null, means that the data is in the chunk its self
*/
bool CSSnapshots::DivideSnapshotObjInfoByIndex (SequenceNum sn, std::vector<BitRange>& range, 
                                                std::vector<BitRange>& notInRanges, 
                                                std::vector<ObjectInfo>& objInfos) {
    CSSnapshot* snapshot = nullptr;
    bool isFinish = false;
    bool isFound = false;

    std::vector<BitRange> clearRanges;
    std::vector<BitRange> setRanges;
    std::vector<BitRange> tmpRanges;
    std::vector<BitRange> searchRanges;
    
    searchRanges = range;

    if (0 == sn) { //sn == 0 means that this is not a snapshot
        for (auto& tmpc : searchRanges) {
            notInRanges.push_back (tmpc);
        }
        return isFinish;
    }

    for (auto it = snapshots_.begin(); it != snapshots_.end(); it++) {
        if (false == isFound) {
            if ((*it)->GetSn() >= sn) {
                snapshot = *it;
                isFound = true;
            } else {
                continue;
            }
        }

        if (true == isFound) {
            snapshot = *it;
            if (nullptr == snapshot) {
                continue;
            }

            for (auto& r : searchRanges) {
                clearRanges.clear();
                setRanges.clear();
                snapshot->GetPageStatus()->Divide(r.beginIndex, r.endIndex, &clearRanges, &setRanges);
                if (true != clearRanges.empty()) {
                    for (auto & ctmp: clearRanges) {
                        tmpRanges.push_back (ctmp);
                    }
                }

                for (auto& tmpr : setRanges) {
                    ObjectInfo objInfo;
                    objInfo.sn = sn;
                    objInfo.snapptr = snapshot;
                    objInfo.offset = tmpr.beginIndex << PAGE_SIZE_SHIFT;
                    objInfo.length = (tmpr.endIndex - tmpr.beginIndex + 1) << PAGE_SIZE_SHIFT;
                    objInfos.push_back(objInfo);
                }
            }

            if (true == tmpRanges.empty()) {
                isFinish = true;
                break;
            }

            //for next snapshot search
            searchRanges = tmpRanges;
            tmpRanges.clear();
        }
    }

    if (false == isFinish) {
        //not any snapshot also lead the tmpRanges to empty
        for (auto& tmpc : searchRanges) {
            notInRanges.push_back (tmpc);
        }
    }

    return isFinish;
}

/**
 * It moves snap chunk `from` to `to', and erase `from' from `snapshots' vector.
 * The caller should load snapshot `to' upon success.
 *
 * Snapshot `to' should have no COW in disk.
*/
CSErrorCode CSSnapshots::Move(SequenceNum from, SequenceNum to) {
    std::shared_ptr<CSSnapshot> snapFrom(pop(from));
    snapFrom->metaPage_.sn = to;
    CSErrorCode errorCode = snapFrom->Flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Move snapshot failed: " << errorCode;
        return errorCode;
    }

    int rc = snapFrom->lfs_->Rename(snapFrom->path(from), snapFrom->path(to), RENAME_NOREPLACE);
    if (rc != 0) {
        LOG(ERROR) << "Rename snap chunk " << snapFrom->path(from)
                   << " to " << snapFrom->path(to) << " failed: " << rc;
        return CSErrorCode::InternalError;
    }

    return CSErrorCode::Success;
}

/**
 * It merges snap chunk `from' to `to', if target bitmap is not set, and erase
 * `from' from `snapshots' vector, after delete the snap chunk in disk.
 *
 * Both snapshot `from' and `to' have COW in disk.
*/
CSErrorCode CSSnapshots::Merge(SequenceNum from, SequenceNum to) {
    std::shared_ptr<CSSnapshot> snapFrom(pop(from));
    CSSnapshot* snapTo = *find(to);

    uint32_t pos = 0;
    char buf[blockSize_];
    // TODO read/write in ranges
    for (pos = snapFrom->metaPage_.bitmap->NextSetBit(pos); pos != Bitmap::NO_POS; pos =snapFrom->metaPage_.bitmap->NextSetBit(pos+1)) {
        if (!snapTo->metaPage_.bitmap->Test(pos)) {
            off_t offset = blockSize_ * pos;
            int rc = snapFrom->readData(buf, offset, blockSize_);
            if (rc < 0) {
                LOG(ERROR) << "Read snap chunk file failed. "
                           << "ChunkID: " << snapFrom->chunkId_
                           << ", chunk sn: " << snapFrom->GetSn()
                           << ", snap sn: " << from;
                return CSErrorCode::InternalError;
            }

            rc = snapTo->Write(buf, offset, blockSize_);
            if (rc < 0) {
                LOG(ERROR) << "Write snap chunk file failed. "
                           << "ChunkID: " << snapFrom->chunkId_
                           << ", chunk sn: " << snapFrom->GetSn()
                           << ", snap sn: " << to;
                return CSErrorCode::InternalError;
            }
        }
    }

    CSErrorCode errorCode = snapTo->Flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Merge snapshot failed upon flush: " << errorCode;
        return errorCode;
    }

    errorCode = snapFrom->Delete();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Delete snapshot failed."
                   << "ChunkID: " << snapFrom->chunkId_
                   << ", chunk sn: " << snapFrom->GetSn()
                   << ", snap sn: " << from;
        return errorCode;
    }

    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
