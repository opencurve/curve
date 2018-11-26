/*
 * Project: curve
 * Created Date: Wednesday September 26th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_NAMESERVER2_FAKES_H_
#define TEST_MDS_NAMESERVER2_FAKES_H_

#include <glog/logging.h>
#include <vector>
#include <mutex>   // NOLINT
#include <utility>
#include <map>
#include <string>
#include "src/mds/nameserver2/define.h"
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/chunk_id_generator.h"
#include "src/mds/nameserver2/session.h"
#include "src/mds/repo/repo.h"
#include "src/mds/topology/topology_admin.h"

using ::curve::mds::topology::TopologyAdmin;

const uint64_t FACK_INODE_INITIALIZE = 0;
const uint64_t FACK_CHUNKID_INITIALIZE = 0;

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

class FackTopologyAdmin: public TopologyAdmin {
 public:
    using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;
    using FileType = ::curve::mds::FileType;

    FackTopologyAdmin() {}

    bool AllocateChunkRandomInSingleLogicalPool(
            FileType fileType, uint32_t chunkNumer,
            std::vector<CopysetIdInfo> *infos) override {
        for (uint32_t i = 0; i != chunkNumer; i++) {
            CopysetIdInfo copysetIdInfo{0, i};
            infos->push_back(copysetIdInfo);
        }
        return true;
    }
};

class FakeNameServerStorage : public NameServerStorage {
 public:
    StoreStatus PutFile(const std::string & storeKey,
          const FileInfo & fileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);

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

    StoreStatus GetFile(const std::string & storeKey,
          FileInfo * fileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        fileInfo->ParseFromString(iter->second);
        return StoreStatus::OK;
    }

    StoreStatus DeleteFile(const std::string & storekey) override {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = memKvMap_.find(storekey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);
        return StoreStatus::OK;
    }

    StoreStatus RenameFile(const std::string & oldStoreKey,
                           const FileInfo &oldfileInfo,
                           const std::string & newStoreKey,
                           const FileInfo &newfileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);

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

    StoreStatus ListFile(const std::string & startStoreKey,
                         const std::string & endStoreKey,
                         std::vector<FileInfo> * files) override {
        std::lock_guard<std::mutex> guard(lock_);

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


    StoreStatus GetSegment(const std::string & storeKey,
                           PageFileSegment *segment) override {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        segment->ParseFromString(iter->second);
        return StoreStatus::OK;
    }

    StoreStatus PutSegment(const std::string & storeKey,
                           const PageFileSegment * segment) override {
        std::lock_guard<std::mutex> guard(lock_);
        std::string value = segment->SerializeAsString();
        memKvMap_.insert(std::move(std::pair<std::string, std::string>
            (storeKey, std::move(value))));
        return StoreStatus::OK;
    }

    StoreStatus DeleteSegment(const std::string &storeKey) override {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = memKvMap_.find(storeKey);
        if (iter == memKvMap_.end()) {
            return StoreStatus::KeyNotExist;
        }
        memKvMap_.erase(iter);
        return StoreStatus::OK;
    }

    StoreStatus SnapShotFile(const std::string & originalFileKey,
                            const FileInfo *originalFileInfo,
                            const std::string & snapshotFileKey,
                            const FileInfo * snapshotFileInfo) override {
        std::lock_guard<std::mutex> guard(lock_);
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

 private:
    std::mutex lock_;
    std::map<std::string, std::string> memKvMap_;
};

class FakeRepoInterface : public repo::RepoInterface {
 public:
    int connectDB(const std::string &dbName,
                        const std::string &user,
                        const std::string &url,
                        const std::string &password) override {
        return repo::OperationOK;
    }

    int createAllTables() override {
        return repo::OperationOK;
    }

    int createDatabase() override {
        return repo::OperationOK;
    }

    int useDataBase() override {
        return repo::OperationOK;
    }

    int dropDataBase() override {
        return repo::OperationOK;
    }

    // chunkServerRepo operation
    int InsertChunkServerRepo(const repo::ChunkServerRepo &cr) override {
        return repo::OperationOK;
    }

    int LoadChunkServerRepos(
        std::vector<repo::ChunkServerRepo> *chunkServerRepoList) override {
        return repo::OperationOK;
    }

    int DeleteChunkServerRepo(repo::ChunkServerIDType id) override {
        return repo::OperationOK;
    }

    int UpdateChunkServerRepo(const repo::ChunkServerRepo &cr) override {
        return repo::OperationOK;
    }

    int QueryChunkServerRepo(repo::ChunkServerIDType id,
                                    repo::ChunkServerRepo *repo) override {
        return repo::OperationOK;
    }
    // server operation
    int InsertServerRepo(const repo::ServerRepo &sr) override {
        return repo::OperationOK;
    }

    int LoadServerRepos(std::vector<repo::ServerRepo> *serverList) override {
        return repo::OperationOK;
    }

    int DeleteServerRepo(repo::ServerIDType id) override {
        return repo::OperationOK;
    }

    int UpdateServerRepo(const repo::ServerRepo &sr) override {
        return repo::OperationOK;
    }

    int QueryServerRepo(repo::ServerIDType id, repo::ServerRepo *repo)
                                                                override {
        return repo::OperationOK;
    }

    // zone operation
    int InsertZoneRepo(const repo::ZoneRepo &zr) override {
        return repo::OperationOK;
    }

    int LoadZoneRepos(std::vector<repo::ZoneRepo> *zonevector) override {
        return repo::OperationOK;
    }

    int DeleteZoneRepo(repo::ZoneIDType id) {
        return repo::OperationOK;
    }

    int UpdateZoneRepo(const repo::ZoneRepo &zr) override {
        return repo::OperationOK;
    }

    int QueryZoneRepo(repo::ZoneIDType id, repo::ZoneRepo *repo) override {
        return repo::OperationOK;
    }

    // physical pool operation
    int InsertPhysicalPoolRepo(const repo::PhysicalPoolRepo &pr) override {
        return repo::OperationOK;
    }

    int LoadPhysicalPoolRepos(
        std::vector<repo::PhysicalPoolRepo> *physicalPoolvector) override {
        return repo::OperationOK;
    }

    int DeletePhysicalPoolRepo(repo::PhysicalPoolIDType id) override {
        return repo::OperationOK;
    }

    int UpdatePhysicalPoolRepo(const repo::PhysicalPoolRepo &pr) override {
        return repo::OperationOK;
    }

    int QueryPhysicalPoolRepo(repo::PhysicalPoolIDType id,
                                    repo::PhysicalPoolRepo *repo) override {
        return repo::OperationOK;
    }

    // logical pool operation
    int InsertLogicalPoolRepo(const repo::LogicalPoolRepo &lr) override {
        return repo::OperationOK;
    }

    int LoadLogicalPoolRepos(
        std::vector<repo::LogicalPoolRepo> *logicalPoolList) override {
        return repo::OperationOK;
    }

    int DeleteLogicalPoolRepo(repo::LogicalPoolIDType id) override {
        return repo::OperationOK;
    }

    int UpdateLogicalPoolRepo(const repo::LogicalPoolRepo &lr) override {
        return repo::OperationOK;
    }

    int QueryLogicalPoolRepo(repo::LogicalPoolIDType id,
                                    repo::LogicalPoolRepo *repo) override {
        return repo::OperationOK;
    }

    // copyset operation
    int InsertCopySetRepo(const repo::CopySetRepo &cr) override {
        return repo::OperationOK;
    }

    int LoadCopySetRepos(std::vector<repo::CopySetRepo> *copySetList) override {
        return repo::OperationOK;
    }

    int DeleteCopySetRepo(repo::CopySetIDType id, repo::LogicalPoolIDType lid)
                                                                    override {
        return repo::OperationOK;
    }

    int UpdateCopySetRepo(const repo::CopySetRepo &cr) override {
        return repo::OperationOK;
    }

    int QueryCopySetRepo(repo::CopySetIDType id,
                                repo::LogicalPoolIDType lid,
                                repo::CopySetRepo *repo) override {
        return repo::OperationOK;
    }

    // session operation
    int InsertSessionRepo(const repo::SessionRepo &r) override {
        LOG(INFO) << "InsertSessionRepo";
        return repo::OperationOK;
    }

    int LoadSessionRepo(std::vector<repo::SessionRepo> *sessionList) override {
        LOG(INFO) << "LoadSessionRepo";
        return repo::OperationOK;
    }

    int DeleteSessionRepo(const std::string &sessionID) override {
        LOG(INFO) << "DeleteSessionRepo";
        return repo::OperationOK;
    }

    int UpdateSessionRepo(const repo::SessionRepo &r) override {
        LOG(INFO) << "UpdateSessionRepo";
        return repo::OperationOK;
    }

    int QuerySessionRepo(const std::string &sessionID, repo::SessionRepo *r) {
        LOG(INFO) << "QuerySessionRepo";
        return repo::OperationOK;
    }

    int SetAutoCommit(const bool &autoCommit) override {
        return repo::OperationOK;
    }

    int Commit() override {
        return repo::OperationOK;
    }

    int RollBack() override {
        return repo::OperationOK;
    }
};
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_FAKES_H_
