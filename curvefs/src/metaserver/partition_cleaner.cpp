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
 * @Project: curve
 * @Date: 2021-12-15 10:54:37
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/partition_cleaner.h"

#include <list>

#include "curvefs/src/metaserver/copyset/meta_operator.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::mds::FSStatusCode;
using ::curvefs::mds::FSStatusCode_Name;

bool PartitionCleaner::ScanPartition() {
    if (!copysetNode_->IsLeaderTerm()) {
        return false;
    }

    std::list<uint64_t> InodeIdList;
    if (!partition_->GetInodeIdList(&InodeIdList)) {
        return false;
    }

    for (auto inodeId : InodeIdList) {
        if (isStop_ || !copysetNode_->IsLeaderTerm()) {
            return false;
        }
        Inode inode;
        MetaStatusCode ret =
            partition_->GetInode(partition_->GetFsId(), inodeId, &inode);
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "ScanPartition get inode fail, fsId = "
                         << partition_->GetFsId()
                         << ", inodeId = " << inodeId;
            continue;
        }

        ret = CleanDataAndDeleteInode(inode);
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "ScanPartition clean inode fail, inode = "
                         << inode.ShortDebugString();
            continue;
        }
        usleep(inodeDeletePeriodMs_);
    }

    uint32_t partitionId = partition_->GetPartitionId();
    if (partition_->EmptyInodeStorage()) {
        LOG(INFO) << "Inode num is 0, delete partition from metastore"
                  << ", partitonId = " << partitionId;
        MetaStatusCode ret = DeletePartition();
        if (ret == MetaStatusCode::OK) {
            VLOG(3) << "DeletePartition success, partitionId = " << partitionId;
            return true;
        } else {
            LOG(WARNING) << "delete partition from copyset fail, partitionId = "
                         << partitionId << ", ret = "
                         << MetaStatusCode_Name(ret);
            return false;
        }
    }

    return false;
}

MetaStatusCode PartitionCleaner::CleanDataAndDeleteInode(const Inode& inode) {
    // TODO(cw123) : consider FsFileType::TYPE_FILE
    if (FsFileType::TYPE_S3 == inode.type()) {
         // get s3info from mds
        FsInfo fsInfo;
        if (fsInfoMap_.find(inode.fsid()) == fsInfoMap_.end()) {
            auto ret = mdsClient_->GetFsInfo(inode.fsid(), &fsInfo);
            if (ret != FSStatusCode::OK) {
                if (FSStatusCode::NOT_FOUND == ret) {
                    LOG(ERROR)
                        << "The fsName not exist, fsId = " << inode.fsid();
                    return MetaStatusCode::S3_DELETE_ERR;
                } else {
                    LOG(ERROR)
                        << "GetFsInfo failed, FSStatusCode = " << ret
                        << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                        << ", fsId = " << inode.fsid();
                    return MetaStatusCode::S3_DELETE_ERR;
                }
            }
            fsInfoMap_.insert({inode.fsid(), fsInfo});
        } else {
            fsInfo = fsInfoMap_.find(inode.fsid())->second;
        }
        const auto& s3Info = fsInfo.detail().s3info();
        // reinit s3 adaptor
        S3ClientAdaptorOption clientAdaptorOption;
        s3Adaptor_->GetS3ClientAdaptorOption(&clientAdaptorOption);
        clientAdaptorOption.blockSize = s3Info.blocksize();
        clientAdaptorOption.chunkSize = s3Info.chunksize();
        clientAdaptorOption.objectPrefix = s3Info.objectprefix();
        s3Adaptor_->Reinit(clientAdaptorOption, s3Info.ak(), s3Info.sk(),
            s3Info.endpoint(), s3Info.bucketname());
        int retVal = s3Adaptor_->Delete(inode);
        if (retVal != 0) {
            LOG(ERROR) << "S3ClientAdaptor delete s3 data failed"
                       << ", ret = " << retVal << ", fsId = " << inode.fsid()
                       << ", inodeId = " << inode.inodeid();
            return MetaStatusCode::S3_DELETE_ERR;
        }
    }

    // send request to copyset to delete inode
    MetaStatusCode ret = DeleteInode(inode);
    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "Delete Inode fail, fsId = " << inode.fsid()
                   << ", inodeId = " << inode.inodeid()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode PartitionCleaner::DeleteInode(const Inode& inode) {
    DeleteInodeRequest request;
    request.set_poolid(partition_->GetPoolId());
    request.set_copysetid(partition_->GetCopySetId());
    request.set_partitionid(partition_->GetPartitionId());
    request.set_fsid(inode.fsid());
    request.set_inodeid(inode.inodeid());
    DeleteInodeResponse response;
    PartitionCleanerClosure done;
    auto deleteInodeOp = new copyset::DeleteInodeOperator(
        copysetNode_, nullptr, &request, &response, &done);
    deleteInodeOp->Propose();
    done.WaitRunned();
    return response.statuscode();
}

MetaStatusCode PartitionCleaner::DeletePartition() {
    DeletePartitionRequest request;
    request.set_poolid(partition_->GetPoolId());
    request.set_copysetid(partition_->GetCopySetId());
    request.set_partitionid(partition_->GetPartitionId());
    DeletePartitionResponse response;
    PartitionCleanerClosure done;
    auto deletePartitionOp = new copyset::DeletePartitionOperator(
        copysetNode_, nullptr, &request, &response, &done);
    deletePartitionOp->Propose();
    done.WaitRunned();
    return response.statuscode();
}
}  // namespace metaserver
}  // namespace curvefs
