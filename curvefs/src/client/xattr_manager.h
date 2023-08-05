/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: Thur May 12 2022
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_CLIENT_XATTR_MANAGER_H_
#define CURVEFS_SRC_CLIENT_XATTR_MANAGER_H_

#include <cstdint>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
#include <list>
#include <stack>
#include <mutex>
#include <set>
#include <vector>

#include "curvefs/src/common/define.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/client_operator.h"
#include "src/common/interruptible_sleeper.h"

#define DirectIOAlignment 512

namespace curvefs {
namespace client {

using curvefs::metaserver::FsFileType;
using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;

struct SummaryInfo {
    uint64_t files = 0;
    uint64_t subdirs = 0;
    uint64_t entries = 0;
    uint64_t fbytes = 0;
};

class XattrManager {
 public:
    XattrManager(const std::shared_ptr<InodeCacheManager> &inodeManager,
        const std::shared_ptr<DentryCacheManager> &dentryManager,
        uint32_t listDentryLimit,
        uint32_t listDentryThreads)
        : inodeManager_(inodeManager),
        dentryManager_(dentryManager),
        listDentryLimit_(listDentryLimit),
        listDentryThreads_(listDentryThreads),
        isStop_(false) {}

    ~XattrManager() {}

    void Stop() {
        isStop_.store(true);
    }

    CURVEFS_ERROR GetXattr(const char* name, std::string *value,
        InodeAttr *attr, bool enableSumInDir);

    CURVEFS_ERROR UpdateParentInodeXattr(uint64_t parentId,
        const XAttr &xattr, bool direction);

    CURVEFS_ERROR UpdateParentXattrAfterRename(uint64_t parent,
        uint64_t newparent, const char *newname,
        RenameOperator* renameOp);

 private:
    bool ConcurrentListDentry(
        std::list<Dentry> *dentrys,
        std::stack<uint64_t> *iStack,
        std::mutex *stackMutex,
        bool dirOnly,
        Atomic<uint32_t> *inflightNum,
        Atomic<bool> *ret);

    void ConcurrentGetInodeAttr(
        std::stack<uint64_t> *iStack,
        std::mutex *stackMutex,
        std::unordered_map<uint64_t, uint64_t> *hardLinkMap,
        std::mutex *mapMutex,
        SummaryInfo *summaryInfo,
        std::mutex *valueMutex,
        Atomic<uint32_t> *inflightNum,
        Atomic<bool> *ret);

    void ConcurrentGetInodeXattr(
        std::stack<uint64_t> *iStack,
        std::mutex *stackMutex,
        InodeAttr *attr,
        std::mutex *inodeMutex,
        Atomic<uint32_t> *inflightNum,
        Atomic<bool> *ret);

    CURVEFS_ERROR CalOneLayerSumInfo(InodeAttr *attr);

    CURVEFS_ERROR CalAllLayerSumInfo(InodeAttr *attr);

    CURVEFS_ERROR FastCalOneLayerSumInfo(InodeAttr *attr);

    CURVEFS_ERROR FastCalAllLayerSumInfo(InodeAttr *attr);

 private:
    // inode cache manager
    std::shared_ptr<InodeCacheManager> inodeManager_;

    // dentry cache manager
    std::shared_ptr<DentryCacheManager> dentryManager_;

    InterruptibleSleeper sleeper_;

    uint32_t listDentryLimit_;

    uint32_t listDentryThreads_;

    Atomic<bool> isStop_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_XATTR_MANAGER_H_
