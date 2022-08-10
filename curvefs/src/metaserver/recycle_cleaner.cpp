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
 * @Project: curve
 * @Date: 2022-08-25 15:39:21
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/recycle_cleaner.h"

#include <list>
#include <vector>

using curve::client::MetaServerOption;
using curve::common::StringToUll;
using curvefs::client::common::AddUllStringToFirst;
using curvefs::client::rpcclient::MDSBaseClient;
using curvefs::client::rpcclient::MdsClientImpl;
using curvefs::mds::FsInfo;
using curvefs::mds::FSStatusCode;
using curvefs::mds::topology::PartitionTxId;

namespace curvefs {
namespace metaserver {
bool RecycleCleaner::UpdateFsInfo() {
    uint32_t fsId = partition_->GetFsId();
    FsInfo fsInfo;
    FSStatusCode ret = mdsClient_->GetFsInfo(fsId, &fsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo failed, ret = " << FSStatusCode_Name(ret)
                     << ", fsId = " << fsId;
        return false;
    }

    fsInfo_ = fsInfo;
    VLOG(1) << "UpdateFsInfo, fsId = " << fsId
            << ", recycle time = " << GetRecycleTime();
    return true;
}

bool RecycleCleaner::IsDirTimeOut(const std::string& dir) {
    uint32_t hour = GetRecycleTime();
    if (hour == 0) {
        return true;
    }

    time_t timeNow;
    time(&timeNow);

    struct tm tmDir;
    memset(&tmDir, 0, sizeof(tmDir));
    char* c = strptime(dir.c_str(), "%Y-%m-%d-%H", &tmDir);

    time_t dirTime = mktime(&tmDir);
    if (dirTime <= 0) {
        LOG(WARNING) << "convert dir to time fail, dir = " << dir;
        return false;
    }

    if (timeNow > dirTime + hour * 3600) {
        return true;
    }

    return false;
}

bool RecycleCleaner::DeleteNode(const Dentry& dentry) {
    uint32_t fsId = partition_->GetFsId();
    uint64_t parent = dentry.parentinodeid();
    uint64_t inodeid = dentry.inodeid();
    std::string name = dentry.name();
    FsFileType type = dentry.type();
    LOG(INFO) << "RecycleCleaner DeleteNode, " << dentry.ShortDebugString();
    // Code refers to the implementation of fuse_client.cpp DeleteNode()
    // 1. delete dentry
    auto ret = metaClient_->DeleteDentry(fsId, parent, name, type);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "delete dentry fail, ret = " << MetaStatusCode_Name(ret)
                     << ", dentry: " << dentry.ShortDebugString();
        return false;
    }

    // 2. update parent inode
    Inode parentInode;
    bool isStreaming;
    ret = metaClient_->GetInode(fsId, parent, &parentInode, &isStreaming);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "get parent inode fail, ret = "
                     << MetaStatusCode_Name(ret) << ", fsId = " << fsId
                     << ", inodeId = " << parent;
        return false;
    }

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    InodeAttr parentDirtyAttr;
    parentDirtyAttr.set_ctime(now.tv_sec);
    parentDirtyAttr.set_ctime_ns(now.tv_nsec);
    parentDirtyAttr.set_mtime(now.tv_sec);
    parentDirtyAttr.set_mtime_ns(now.tv_nsec);
    parentInode.set_ctime(now.tv_sec);
    parentInode.set_ctime_ns(now.tv_nsec);
    parentInode.set_mtime(now.tv_sec);
    parentInode.set_mtime_ns(now.tv_nsec);
    ret =
        metaClient_->UpdateInodeAttrWithOutNlink(fsId, parent, parentDirtyAttr);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "update parent inode attr failed, ret = "
                     << MetaStatusCode_Name(ret)
                     << ", parent inodeid = " << parentInode.inodeid();
        return false;
    }

    // 3. unlink inode
    Inode inode;
    ret = metaClient_->GetInode(fsId, inodeid, &inode, &isStreaming);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "get inode fail, ret = " << MetaStatusCode_Name(ret)
                     << ", fsId = " << fsId << ", inodeId = " << inodeid;
        return false;
    }

    if (inode.nlink() == 0) {
        LOG(WARNING) << "Unlink find nlink == 0, nlink = " << inode.nlink()
                     << ", inode = " << inodeid;
        return false;
    }

    uint32_t newnlink = inode.nlink() - 1;
    if (newnlink == 1 && inode.type() == FsFileType::TYPE_DIRECTORY) {
        newnlink--;
    }

    InodeAttr dirtyAttr;
    dirtyAttr.set_nlink(newnlink);
    dirtyAttr.set_ctime(now.tv_sec);
    dirtyAttr.set_ctime_ns(now.tv_nsec);
    dirtyAttr.set_mtime(now.tv_sec);
    dirtyAttr.set_mtime_ns(now.tv_nsec);
    // newlink == 0 will be deleted at metasever
    // dir will not update parent
    // parent = 0; is useless
    if (newnlink != 0 && inode.type() != FsFileType::TYPE_DIRECTORY &&
        parent != 0) {
        auto parents = inode.mutable_parent();
        for (auto iter = parents->begin(); iter != parents->end(); iter++) {
            if (*iter == parent) {
                parents->erase(iter);
                break;
            }
        }
    }

    *dirtyAttr.mutable_parent() = inode.parent();
    ret = metaClient_->UpdateInodeAttr(fsId, inodeid, dirtyAttr);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "UpdateInodeAttr failed, ret = "
                     << MetaStatusCode_Name(ret)
                     << ", inodeid = " << inode.inodeid();
        return false;
    }

    // when use recycle, should disable SumInDir

    return true;
}

// delete file under dir, delete dir
bool RecycleCleaner::DeleteDirRecursive(const Dentry& dentry) {
    uint32_t fsId = partition_->GetFsId();
    uint64_t inodeid = dentry.inodeid();
    std::string last = "";
    bool onlyDir = false;

    while (true) {
        // 1. list dir
        std::list<Dentry> dentryList;
        auto ret = metaClient_->ListDentry(fsId, inodeid, last, limit_, onlyDir,
                                           &dentryList);
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "DeleteDirRecursive list dentry fail, ret = "
                         << MetaStatusCode_Name(ret)
                         << ", dentry: " << dentry.ShortDebugString();
            return false;
        }
        // 2. delete file under dir
        for (const auto& it : dentryList) {
            if (isStop_ || !copysetNode_->IsLeaderTerm()) {
                LOG(WARNING) << "recycle cleaner stop or not leader, isStop = "
                             << isStop_
                             << ", isLeader = " << copysetNode_->IsLeaderTerm();
                return false;
            }

            FsFileType type;
            if (it.has_type()) {
                type = it.type();
            } else {
                InodeAttr attr;
                ret = metaClient_->GetInodeAttr(fsId, it.inodeid(), &attr);
                if (ret != MetaStatusCode::OK) {
                    LOG(WARNING)
                        << "DeleteDirRecursive get inode attr fail, ret = "
                        << MetaStatusCode_Name(ret) << ", fsId = " << fsId
                        << ", inodeid = " << it.inodeid();
                    return false;
                }
                type = attr.type();
            }

            if (type == FsFileType::TYPE_DIRECTORY) {
                // if type is diretory, delete recursive
                if (!DeleteDirRecursive(it)) {
                    LOG(WARNING) << "DeleteDirRecursive delete sub dentry fail,"
                                 << " dentry: " << it.ShortDebugString();
                    return false;
                }
            } else {
                // if type is not directory, delete direct
                if (!DeleteNode(it)) {
                    LOG(WARNING) << "DeleteDirRecursive delete node fail, "
                                 << "dentry: " << it.ShortDebugString();
                    return false;
                }
            }
        }

        if (dentryList.size() < limit_) {
            break;
        } else {
            last = dentryList.back().name();
        }
    }

    // 3. delete dir
    if (!DeleteNode(dentry)) {
        LOG(WARNING) << "DeleteDirRecursive delete node fail, "
                     << "dentry: " << dentry.ShortDebugString();
        return false;
    }

    LOG(INFO) << "DeleteDirRecursive success, dentry: "
              << dentry.ShortDebugString();
    return true;
}

uint64_t RecycleCleaner::GetTxId() {
    std::vector<PartitionTxId> txIds;
    if (FSStatusCode::OK !=
        mdsClient_->GetLatestTxId(partition_->GetFsId(), &txIds)) {
        LOG(WARNING) << "GetLatestTxId fail, use UINT64_MAX, fsId = "
                     << GetFsId() << ", inode = " << RECYCLEINODEID;
        return UINT64_MAX;
    }

    for (const auto& it : txIds) {
        if (it.partitionid() == partition_->GetPartitionId()) {
            return it.txid();
        }
    }

    return UINT64_MAX;
}

bool RecycleCleaner::ScanRecycle() {
    if (isStop_) {
        LOG(WARNING) << "recycle cleaner is stop, fsId = " << GetFsId()
                     << ", partitionId = " << GetPartitionId();
        return true;
    }

    if (!copysetNode_->IsLeaderTerm()) {
        LOG(INFO) << "ScanRecycle recycle cleaner is not copyset leader, skip"
                  << ", fsId = " << GetFsId()
                  << ", partitionId = " << GetPartitionId();
        return false;
    }

    // update fs info every time it's called to get lastest recycle time
    if (!UpdateFsInfo()) {
        LOG(WARNING) << "ScanRecylce update fs info fail, fsid = "
                     << partition_->GetFsId();
        return false;
    }

    LOG(INFO) << "Scan recycle dir, fsId = " << GetFsId()
              << ", partitionId = " << GetPartitionId();

    bool onlyDir = true;
    std::string last = "";
    Dentry dentry;
    dentry.set_fsid(partition_->GetFsId());
    dentry.set_parentinodeid(RECYCLEINODEID);
    dentry.set_txid(GetTxId());
    uint32_t count = 0;
    uint32_t timeoutCount = 0;
    while (true) {
        dentry.set_name(last);
        std::vector<Dentry> tempDentrys;
        auto ret =
            partition_->ListDentry(dentry, &tempDentrys, limit_, onlyDir);
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "Scan recycle, list dentry fail, dentry = "
                         << dentry.ShortDebugString();
            return false;
        }

        for (const auto& it : tempDentrys) {
            if (isStop_) {
                LOG(WARNING) << "recycle cleaner is stop"
                             << ", fsId = " << GetFsId()
                             << ", partitionId = " << GetPartitionId();
                return true;
            }

            if (!copysetNode_->IsLeaderTerm()) {
                LOG(INFO) << "recycle cleaner is not copyset leader, skip"
                          << ", fsId = " << GetFsId()
                          << ", partitionId = " << GetPartitionId();
                return false;
            }

            if (it.has_type() && it.type() != FsFileType::TYPE_DIRECTORY) {
                LOG(WARNING) << "some file under recycle dir and not directory"
                             << ", dentry: " << it.ShortDebugString();
                continue;
            }

            count++;
            if (IsDirTimeOut(it.name())) {
                timeoutCount++;
                LOG(INFO) << "recycle dir is timeout, delete dir recursive"
                          << ", dir = " << it.name()
                          << ", recycle time = " << GetRecycleTime()
                          << ", fsId = " << GetFsId()
                          << ", partitionId = " << GetPartitionId();
                if (!DeleteDirRecursive(it)) {
                    LOG(WARNING)
                        << "ScanRecycle dir is timeout"
                        << ", delete dir recursive fail, dir = " << it.name()
                        << ", recycleTimeHour = " << GetRecycleTime()
                        << ", fsId = " << GetFsId()
                        << ", partitionId = " << GetPartitionId();
                }
            } else {
                LOG(INFO) << "recycle dir is not timeout, skip. dir = "
                          << it.name()
                          << ", recycle time = " << GetRecycleTime()
                          << ", fsId = " << GetFsId()
                          << ", partitionId = " << GetPartitionId();
            }
        }

        if (tempDentrys.size() < limit_) {
            break;
        } else {
            last = tempDentrys.back().name();
        }
    }

    LOG(INFO) << "Scan recycle dir end, fsId = " << GetFsId()
              << ", partitionId = " << GetPartitionId()
              << ", scan dir count = " << count
              << ", timeout dir count = " << timeoutCount;
    return false;
}
}  // namespace metaserver
}  // namespace curvefs
