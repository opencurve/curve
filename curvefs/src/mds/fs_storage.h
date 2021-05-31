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

#include <unordered_map>
#include <string>
#include <functional>
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/fs.h"
#include "src/common/concurrent/rw_lock.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;

namespace curvefs {
namespace mds {
class FsStorage {
 public:
    virtual FSStatusCode Insert(const MdsFsInfo &fs) = 0;
    virtual FSStatusCode Get(uint64_t fsId, MdsFsInfo *fs) = 0;
    virtual FSStatusCode Get(const std::string &fsName, MdsFsInfo *fs) = 0;
    virtual FSStatusCode Delete(const std::string &fsName) = 0;
    virtual FSStatusCode Update(const MdsFsInfo &fs) = 0;
    virtual bool Exist(uint64_t fsId) = 0;
    virtual bool Exist(const std::string &fsName) = 0;
    virtual ~FsStorage() = default;
};

class MemoryFsStorage : public FsStorage {
 public:
    /**
     * @brief insert fs to storage
     *
     * @param[in] fs: the fs need to insert
     *
     * @return If fs exist, return FS_EXIST; else insert the fs
     */
    FSStatusCode Insert(const MdsFsInfo &fs) override;

    /**
     * @brief get fs from storage
     *
     * @param[in] fsId: the fsId of fs wanted
     * @param[out] fs: the fs got
     *
     * @return If success get , return OK; if no record got, return NOT_EXIST;
     *         else return error code
     */
    FSStatusCode Get(uint64_t fsId, MdsFsInfo *fs) override;

    /**
     * @brief get fs from storage
     *
     * @param[in] fsName: the fsName of fs wanted
     * @param[out] fs: the fs got
     *
     * @return If success get , return OK; if no record got, return NOT_EXIST;
     *         else return error code
     */
    FSStatusCode Get(const std::string &fsName, MdsFsInfo *fs) override;

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
    FSStatusCode Update(const MdsFsInfo &fs) override;

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

 private:
    std::unordered_map<std::string, MdsFsInfo> FsInfoMap_;
    curve::common::RWLock rwLock_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_STORAGE_H_
