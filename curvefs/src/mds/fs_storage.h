/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_FS_STORAGE_H_
#define CURVEFS_SRC_MDS_FS_STORAGE_H_

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/fs_info_wrapper.h"
#include "curvefs/src/mds/idgenerator/fs_id_generator.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/idgenerator/etcd_id_generator.h"
#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {

constexpr uint64_t INVALID_FS_ID = std::numeric_limits<uint64_t>::max();

class FsStorage {
 public:
    FsStorage() = default;
    virtual ~FsStorage() = default;

    FsStorage(const FsStorage&) = delete;
    FsStorage& operator=(const FsStorage&) = delete;

    virtual bool Init() = 0;
    virtual void Uninit() = 0;

    virtual FSStatusCode Get(uint64_t fsId, FsInfoWrapper* fsInfo) = 0;
    virtual FSStatusCode Get(const std::string& fsName,
                             FsInfoWrapper* fsInfo) = 0;

    virtual FSStatusCode Insert(const FsInfoWrapper& fs) = 0;
    virtual FSStatusCode Update(const FsInfoWrapper& fs) = 0;
    virtual FSStatusCode Delete(const std::string& fsName) = 0;

    virtual FSStatusCode Rename(const FsInfoWrapper& oldFs,
                                const FsInfoWrapper& newFs) = 0;

    virtual bool Exist(uint64_t fsId) = 0;
    virtual bool Exist(const std::string& fsName) = 0;

    virtual uint64_t NextFsId() = 0;
    virtual void GetAll(std::vector<FsInfoWrapper>* fsInfoVec) = 0;
};

class MemoryFsStorage : public FsStorage {
 public:
    bool Init() override;
    void Uninit() override;

    /**
     * @brief insert fs to storage
     *
     * @param[in] fs: the fs need to insert
     *
     * @return If fs exist, return FS_EXIST; else insert the fs
     */
    FSStatusCode Insert(const FsInfoWrapper& fs) override;

    /**
     * @brief get fs from storage
     *
     * @param[in] fsId: the fsId of fs wanted
     * @param[out] fs: the fs got
     *
     * @return If success get , return OK; if no record got, return NOT_EXIST;
     *         else return error code
     */
    FSStatusCode Get(uint64_t fsId, FsInfoWrapper *fs) override;

    /**
     * @brief get fs from storage
     *
     * @param[in] fsName: the fsName of fs wanted
     * @param[out] fs: the fs got
     *
     * @return If success get , return OK; if no record got, return NOT_EXIST;
     *         else return error code
     */
    FSStatusCode Get(const std::string &fsName,
                     FsInfoWrapper *fs) override;

    /**
     * @brief delete fs from storage
     *
     * @param[in] fsName: the fsName of fs want to deleted
     *
     * @return If fs exist, delete fs and return OK;
     *         if fs not exist, return NOT_FOUND
     */
    FSStatusCode Delete(const std::string &fsName) override;

    /**
     * @brief update fs from storage
     *
     * @param[in] fs: the fs need to update
     *
     * @return If fs exist, update fs and return OK;
     *         if fs not exist, return NOT_FOUND
     */
    FSStatusCode Update(const FsInfoWrapper& fs) override;

    /**
     * @brief rename fs from storage
     *
     * @param[in] oldFs: the old fs
     *  @param[in] newFs: the new fs with new name
     *
     * @return If sucess, return OK;
     *         If old fs not exist, return NOT_FOUND;
     */
    FSStatusCode Rename(const FsInfoWrapper& oldFs,
                        const FsInfoWrapper& newFs) override;

    /**
     * @brief check if fs is exist
     *
     * @param[in] fsId: the fsId of fs which need to check
     *
     * @return If fs exist, return true;
     *         if fs not exist, return false
     */
    bool Exist(uint64_t fsId) override;

    /**
     * @brief check if fs is exist
     *
     * @param[in] fsName: the fsName of fs which need to check
     *
     * @return If fs exist, return true;
     *         if fs not exist, return false
     */
    bool Exist(const std::string &fsName) override;

    uint64_t NextFsId() override;
    /**
     * @brief Get the All fsinfo
     *
     * @param fsInfo
     * @details
     */
    void GetAll(std::vector<FsInfoWrapper>* fsInfoVec) override;

 private:
    std::unordered_map<std::string, FsInfoWrapper> fsInfoMap_;
    curve::common::RWLock rwLock_;

    std::atomic<uint64_t> id_;
};

// Persist all data to kvstorage and cache all fsinfo in memory
class PersisKVStorage : public FsStorage {
 public:
    PersisKVStorage(
        const std::shared_ptr<curve::kvstorage::KVStorageClient>& storage);
    ~PersisKVStorage();

    bool Init() override;
    void Uninit() override;

    FSStatusCode Get(uint64_t fsId, FsInfoWrapper* fsInfo) override;
    FSStatusCode Get(const std::string& fsName, FsInfoWrapper* fsInfo) override;

    FSStatusCode Insert(const FsInfoWrapper& fs) override;
    FSStatusCode Update(const FsInfoWrapper& fs) override;
    FSStatusCode Delete(const std::string& fsName) override;

    FSStatusCode Rename(const FsInfoWrapper& oldFs,
                        const FsInfoWrapper& newFs) override;

    bool Exist(uint64_t fsId) override;
    bool Exist(const std::string& fsName) override;

    uint64_t NextFsId() override;

    void GetAll(std::vector<FsInfoWrapper>* fsInfoVec) override;

 private:
    bool LoadAllFs();

    bool FsIDToName(uint64_t fsId, std::string* name) const;

    bool PersistToStorage(const FsInfoWrapper& fs);

    bool RemoveFromStorage(const FsInfoWrapper& fs);

    bool RenameFromStorage(const FsInfoWrapper& oldFs,
                           const FsInfoWrapper& newFs);

 private:
    // for persist data
    std::shared_ptr<curve::kvstorage::KVStorageClient> storage_;

    // fs id generator
    std::unique_ptr<FsIdGenerator> idGen_;

    // protect fs_
    mutable RWLock fsLock_;

    // from fs name to fs info
    std::unordered_map<std::string, FsInfoWrapper> fs_;

    // protect idToName
    mutable RWLock idToNameLock_;

    // from fs id to fs name
    std::unordered_map<uint64_t, std::string> idToName_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_STORAGE_H_
