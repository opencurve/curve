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
 * Created Date: Wednesday September 26th 2018
 * Author: hzsunjianliang
 */

#ifndef TEST_MDS_NAMESERVER2_FAKES_H_
#define TEST_MDS_NAMESERVER2_FAKES_H_

#include <glog/logging.h>
#include <vector>
#include <mutex>   // NOLINT
#include <utility>
#include <map>
#include <string>
#include "src/mds/common/mds_define.h"
#include "src/common/namespace_define.h"
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/topology/topology_chunk_allocator.h"
#include "src/mds/nameserver2/idgenerator/clone_id_generator.h"

using ::curve::mds::topology::TopologyChunkAllocator;
using ::curve::common::SNAPSHOTFILEINFOKEYPREFIX;
using ::curve::common::FILEINFOKEYPREFIX;

const uint64_t FACK_INODE_INITIALIZE = 0;
const uint64_t FACK_CHUNKID_INITIALIZE = 0;
const uint64_t FACK_FILE_INTTIALIZE = 10737418240;

namespace curve {
namespace mds {
class FakeInodeIDGenerator : public InodeIDGenerator {
 public:
    explicit FakeInodeIDGenerator(uint64_t initValue = FACK_INODE_INITIALIZE) {
        value_ = initValue;
    }
    bool GenInodeID(InodeID *id) {
        *id = ++value_;
        return true;
    }
 private:
    std::atomic<uint64_t> value_;
};

class FackChunkIDGenerator: public ChunkIDGenerator {
 public:
    explicit FackChunkIDGenerator(uint64_t val = FACK_CHUNKID_INITIALIZE) {
        value_ = val;
    }
    bool GenChunkID(ChunkID *id) override {
        *id = ++value_;
        return true;
    }
 private:
    std::atomic<uint64_t> value_;
};

class FakeCloneIDGenerator: public CloneIDGenerator {
 public:
    explicit FakeCloneIDGenerator(uint64_t val = CLONEIDINITIALIZE) {
        value_ = val;
    }
    bool GenCloneID(uint64_t *id) override {
        *id = ++value_;
        return true;
    }
 private:
    std::atomic<uint64_t> value_;
};

class FackTopologyChunkAllocator: public TopologyChunkAllocator {
 public:
    using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;
    using FileType = ::curve::mds::FileType;

    FackTopologyChunkAllocator() {}

    bool AllocateChunkRandomInSingleLogicalPool(
            FileType fileType,
            const std::string& pstName,
            uint32_t chunkNumer,
            ChunkSizeType chunkSize,
            std::vector<CopysetIdInfo> *infos) override {
        for (uint32_t i = 0; i != chunkNumer; i++) {
            CopysetIdInfo copysetIdInfo{0, i};
            infos->push_back(copysetIdInfo);
        }
        return true;
    }
    bool AllocateChunkRoundRobinInSingleLogicalPool(
            FileType fileType, const std::string& pstName,
            uint32_t chunkNumer, ChunkSizeType chunkSize,
            std::vector<CopysetIdInfo> *infos) override {
        for (uint32_t i = 0; i != chunkNumer; i++) {
            CopysetIdInfo copysetIdInfo{0, i};
            infos->push_back(copysetIdInfo);
        }
        return true;
    }
    void GetRemainingSpaceInLogicalPool(
        const std::vector<PoolIdType>& logicalPools,
        std::map<PoolIdType, double>* enoughSpacePools,
        const std::string& /*pstName*/) override {
        for (auto i = logicalPools.begin(); i != logicalPools.end(); i++) {
            enoughSpacePools->insert(
                std::pair<PoolIdType, double>(*i, 10 * FACK_FILE_INTTIALIZE));
        }
    }
    void UpdateChunkFilePoolAllocConfig(bool useChunkFilepool_,
            bool  useChunkFilePoolAsWalPool_,
            uint32_t useChunkFilePoolAsWalPoolReserve_) {}
};

class FakeNameServerStorage : public NameServerStorage {
 public:
    StoreStatus GetStoreKey(FileType filetype,
        InodeID id,
        const std::string& filename,
        std::string* storeKey) {
        switch (filetype) {
            case FileType::INODE_PAGEFILE:
            case FileType::INODE_DIRECTORY:
            case FileType::INODE_CLONE_PAGEFILE:
                *storeKey = NameSpaceStorageCodec::EncodeFileStoreKey(id, filename);
                break;
            case FileType::INODE_SNAPSHOT_PAGEFILE:
                *storeKey =
                    NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(id, filename);
                break;
            default:
                LOG(ERROR) << "filetype: "
                           << filetype << " of " << filename << " not exist";
                return StoreStatus::InternalError;
        }
        return StoreStatus::OK;
    }

    StoreStatus PutFile(const FileInfo & fileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                                        fileInfo.parentid(),
                                                        fileInfo.filename());
        auto iter = memKvMap_.find(storeKey);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }

        std::string value = fileInfo.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (storeKey, std::move(value))));
        return StoreStatus::OK;
    }

    StoreStatus Put2File(const FileInfo &fileInfo1,
        const FileInfo &fileInfo2) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string key1;
        auto res = GetStoreKey(fileInfo1.filetype(),
                        fileInfo1.parentid(),
                        fileInfo1.filename(),
                        &key1);
        if (res != StoreStatus::OK) {
            LOG(ERROR) << "get store key failed, filename = "
                       << fileInfo1.filename();
            return StoreStatus::InternalError;
        }

        std::string key2;
        res = GetStoreKey(fileInfo2.filetype(),
                        fileInfo2.parentid(),
                        fileInfo2.filename(),
                        &key2);
        if (res != StoreStatus::OK) {
            LOG(ERROR) << "get store key failed, filename = "
                       << fileInfo2.filename();
            return StoreStatus::InternalError;
        }
        auto iter = memKvMap_.find(key1);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }
        std::string value1 = fileInfo1.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (key1, std::move(value1))));

        iter = memKvMap_.find(key2);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }
        std::string value2 = fileInfo2.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (key2, std::move(value2))));
        return StoreStatus::OK;
    }

    StoreStatus GetFile(InodeID id,
                        const std::string &filename,
                        FileInfo * fileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(id, filename);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        fileInfo->ParseFromString(iter->second);
        return StoreStatus::OK;
    }

    StoreStatus GetSnapFile(InodeID id,
                        const std::string &filename,
                        FileInfo * fileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(id, filename);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        fileInfo->ParseFromString(iter->second);
        return StoreStatus::OK;
    }

    StoreStatus DeleteFile(InodeID id,
                            const std::string &filename) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(id, filename);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);
        return StoreStatus::OK;
    }

    StoreStatus DeleteSnapshotFile(InodeID id,
                            const std::string &filename) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(id, filename);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);
        return StoreStatus::OK;
    }

    StoreStatus RenameFile(const FileInfo &oldfileInfo,
                           const FileInfo &newfileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);

        std::string oldStoreKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  oldfileInfo.parentid(),
                                                  oldfileInfo.filename());
        std::string newStoreKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  newfileInfo.parentid(),
                                                  newfileInfo.filename());

        auto iter = memKvMap_.find(oldStoreKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);

        std::string value = newfileInfo.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (newStoreKey, std::move(value))));

        return StoreStatus::OK;
    }

    StoreStatus ReplaceFileAndRecycleOldFile(const FileInfo &oldFInfo,
                                    const FileInfo &newFInfo,
                                    const FileInfo &conflictFInfo,
                                    const FileInfo &recycleFInfo) override {
        std::lock_guard<std::mutex> guard(lock_);

        std::string oldStoreKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  oldFInfo.parentid(),
                                                  oldFInfo.filename());
        std::string newStoreKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  newFInfo.parentid(),
                                                  newFInfo.filename());
        std::string conflictStoreKey =
                            NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  conflictFInfo.parentid(),
                                                  conflictFInfo.filename());
        std::string recyclestoreKey =
                            NameSpaceStorageCodec::EncodeFileStoreKey(
                                                  recycleFInfo.parentid(),
                                                  recycleFInfo.filename());

        auto iter = memKvMap_.find(conflictStoreKey);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }

        std::string value = recycleFInfo.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (recyclestoreKey, std::move(value))));

        auto iter1 = memKvMap_.find(oldStoreKey);
        if (iter1 == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter1);

        value = newFInfo.SerializeAsString();
        memKvMap_.insert(
            std::move(std::pair<std::string, std::string>
            (newStoreKey, std::move(value))));

        return StoreStatus::OK;
    }

    StoreStatus MoveFileToRecycle(
    const FileInfo &originFileInfo, const FileInfo &recycleFileInfo) {
        std::string originFileInfoKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(
            originFileInfo.parentid(), originFileInfo.filename());

        std::string recycleFileInfoKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(
            recycleFileInfo.parentid(), recycleFileInfo.filename());

        std::string encodeRecycleFInfo;
        if (!NameSpaceStorageCodec::EncodeFileInfo(
            recycleFileInfo, &encodeRecycleFInfo)) {
            LOG(ERROR) << "encode recycle file: " << recycleFileInfo.filename()
                    << " err";
            return StoreStatus::InternalError;
        }

        auto iter = memKvMap_.find(originFileInfoKey);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }

        auto iter1 = memKvMap_.find(recycleFileInfoKey);
        if (iter1 != memKvMap_.end()) {
            memKvMap_.erase(iter1);
        }

        memKvMap_.insert(
                std::move(std::pair<std::string, std::string>
                (recycleFileInfoKey, std::move(encodeRecycleFInfo))));

        FileInfo  validFile;
        validFile.ParseFromString(memKvMap_.find(recycleFileInfoKey)->second);
        return StoreStatus::OK;
    }

    StoreStatus MoveCloneFileToRecycle(const FileInfo &originFileInfo,
        const FileInfo &snapshotFileInfo,
        const FileInfo &recycleFileInfo) {
        std::string originFileInfoKey;
        auto res = GetStoreKey(originFileInfo.filetype(), originFileInfo.parentid(),
            originFileInfo.filename(), &originFileInfoKey);
        if (res != StoreStatus::OK) {
            LOG(ERROR) << "get store key failed, filename = "
                       << originFileInfo.filename();
            return StoreStatus::InternalError;
        }

        std::string snapshotFileInfoKey;
        res = GetStoreKey(snapshotFileInfo.filetype(), snapshotFileInfo.parentid(),
            snapshotFileInfo.filename(), &snapshotFileInfoKey);
        if (res != StoreStatus::OK) {
            LOG(ERROR) << "get store key failed, filename = "
                       << snapshotFileInfo.filename();
            return StoreStatus::InternalError;
        }

        std::string encodeSnapshotFInfo;
        if (!NameSpaceStorageCodec::EncodeFileInfo(
            snapshotFileInfo, &encodeSnapshotFInfo)) {
            LOG(ERROR) << "encode snapshot file: " << snapshotFileInfo.filename()
                      << " err";
            return StoreStatus::InternalError;
        }

        std::string recycleFileInfoKey;
        res = GetStoreKey(recycleFileInfo.filetype(), recycleFileInfo.parentid(),
            recycleFileInfo.filename(), &recycleFileInfoKey);
        if (res != StoreStatus::OK) {
            LOG(ERROR) << "get store key failed, filename = "
                       << recycleFileInfo.filename();
            return StoreStatus::InternalError;
        }

        std::string encodeRecycleFInfo;
        if (!NameSpaceStorageCodec::EncodeFileInfo(
            recycleFileInfo, &encodeRecycleFInfo)) {
            LOG(ERROR) << "encode recycle file: " << recycleFileInfo.filename()
                      << " err";
            return StoreStatus::InternalError;
        }

        auto iter = memKvMap_.find(originFileInfoKey);
        if (iter != memKvMap_.end()) {
            memKvMap_.erase(iter);
        }

        auto iter1 = memKvMap_.find(recycleFileInfoKey);
        if (iter1 != memKvMap_.end()) {
            memKvMap_.erase(iter1);
        }

        auto iter2 = memKvMap_.find(snapshotFileInfoKey);
        if (iter2 != memKvMap_.end()) {
            memKvMap_.erase(iter2);
        }

        memKvMap_.insert(
                std::move(std::pair<std::string, std::string>
                (recycleFileInfoKey, std::move(encodeRecycleFInfo))));
        memKvMap_.insert(
                std::move(std::pair<std::string, std::string>
                (snapshotFileInfoKey, std::move(encodeSnapshotFInfo))));

        FileInfo  validFile;
        validFile.ParseFromString(memKvMap_.find(recycleFileInfoKey)->second);
        return StoreStatus::OK;
    }

    StoreStatus ListFile(InodeID startid,
                         InodeID endid,
                         std::vector<FileInfo> * files) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string startStoreKey =
                NameSpaceStorageCodec::EncodeFileStoreKey(startid, "");
        std::string endStoreKey =
                NameSpaceStorageCodec::EncodeFileStoreKey(endid, "");

        for (auto iter = memKvMap_.begin(); iter != memKvMap_.end(); iter++) {
            if (iter->first.compare(startStoreKey) >= 0) {
                if (iter->first.compare(endStoreKey) < 0) {
                    FileInfo  validFile;
                    validFile.ParseFromString(iter->second);
                    files->push_back(validFile);
                }
            }
        }

        return StoreStatus::OK;
    }

    StoreStatus ListSegment(InodeID id,
                            std::vector<PageFileSegment> *segments) {
        std::lock_guard<std::mutex> guard(lock_);
        std::string startStoreKey =
                NameSpaceStorageCodec::EncodeSegmentStoreKey(id, 0);
        std::string endStoreKey =
                NameSpaceStorageCodec::EncodeSegmentStoreKey(id + 1, 0);

        for (auto iter = memKvMap_.begin(); iter != memKvMap_.end(); iter++) {
            if (iter->first.compare(startStoreKey) >= 0) {
                if (iter->first.compare(endStoreKey) < 0) {
                    PageFileSegment segment;
                    segment.ParseFromString(iter->second);
                    segments->push_back(segment);
                }
            }
        }

        return StoreStatus::OK;
    }

    StoreStatus ListSnapshotFile(InodeID startid,
                         InodeID endid,
                         std::vector<FileInfo> * files) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string startStoreKey =
                NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(startid, "");
        std::string endStoreKey =
                NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(endid, "");

        for (auto iter = memKvMap_.begin(); iter != memKvMap_.end(); iter++) {
            if (iter->first.compare(startStoreKey) >= 0) {
                if (iter->first.compare(endStoreKey) < 0) {
                    FileInfo  validFile;
                    validFile.ParseFromString(iter->second);
                    files->push_back(validFile);
                }
            }
        }

        return StoreStatus::OK;
    }

    StoreStatus GetSegment(InodeID id,
                            uint64_t off,
                           PageFileSegment *segment) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);

        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        segment->ParseFromString(iter->second);
        return StoreStatus::OK;
    }

    StoreStatus PutSegment(InodeID id,
                           uint64_t off,
                           const PageFileSegment * segment,
                           int64_t *revision) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);

        std::string value = segment->SerializeAsString();
        memKvMap_.insert(std::move(std::pair<std::string, std::string>
            (storeKey, std::move(value))));
        return StoreStatus::OK;
    }

    StoreStatus DeleteSegment(
        InodeID id, uint64_t off, int64_t *revision) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string storeKey =
            NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);

        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);
        return StoreStatus::OK;
    }

    StoreStatus SnapShotFile(const FileInfo *originalFileInfo,
                            const FileInfo * snapshotFileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
        auto originalFileKey = NameSpaceStorageCodec::EncodeFileStoreKey(
                                            originalFileInfo->parentid(),
                                            originalFileInfo->filename());
        auto snapshotFileKey = NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(   // NOLINT
                                            snapshotFileInfo->parentid(),
                                            snapshotFileInfo->filename());

        std::string originalFileData = originalFileInfo->SerializeAsString();
        std::string snapshotFileData = snapshotFileInfo->SerializeAsString();
        memKvMap_.erase(originalFileKey);
        memKvMap_.insert(std::move(std::pair<std::string, std::string>
            (originalFileKey, std::move(originalFileData))));
        memKvMap_.insert(std::move(std::pair<std::string, std::string>
            (snapshotFileKey, std::move(snapshotFileData))));
        return StoreStatus::OK;
    }

    StoreStatus LoadSnapShotFile(std::vector<FileInfo> *snapShotFiles)
    override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string snapshotStartKey = SNAPSHOTFILEINFOKEYPREFIX;

        for (auto iter = memKvMap_.begin(); iter != memKvMap_.end(); iter++) {
            if ( iter->first.length() > snapshotStartKey.length() ) {
                if (iter->first.substr(0, snapshotStartKey.length()).
                    compare(snapshotStartKey) == 0) {
                    FileInfo  validFile;
                    validFile.ParseFromString(iter->second);
                    snapShotFiles->push_back(validFile);
                }
            }
        }
        return StoreStatus::OK;
    }

    StoreStatus LoadFileInfos(std::vector<FileInfo> *fileInfos) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string startKey = FILEINFOKEYPREFIX;

        for (auto iter = memKvMap_.begin(); iter != memKvMap_.end(); iter++) {
            if ( iter->first.length() > startKey.length() ) {
                if (iter->first.substr(0, startKey.length()).
                    compare(startKey) == 0) {
                    FileInfo  validFile;
                    validFile.ParseFromString(iter->second);
                    fileInfos->push_back(validFile);
                }
            }
        }
        return StoreStatus::OK;
    }

 private:
    std::mutex lock_;
    std::map<std::string, std::string> memKvMap_;
};

}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_FAKES_H_
