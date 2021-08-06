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
 * @Date: 2021-08-30 19:42:18
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_METASTORE_H_
#define CURVEFS_SRC_METASERVER_METASTORE_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/partition.h"
#include "curvefs/src/metaserver/copyset/snapshot_closure.h"

namespace curvefs {
namespace metaserver {
// dentry
using curvefs::metaserver::GetDentryRequest;
using curvefs::metaserver::GetDentryResponse;
using curvefs::metaserver::ListDentryRequest;
using curvefs::metaserver::ListDentryResponse;
using curvefs::metaserver::CreateDentryRequest;
using curvefs::metaserver::CreateDentryResponse;
using curvefs::metaserver::DeleteDentryRequest;
using curvefs::metaserver::DeleteDentryResponse;
// inode
using curvefs::metaserver::GetInodeRequest;
using curvefs::metaserver::GetInodeResponse;
using curvefs::metaserver::CreateInodeRequest;
using curvefs::metaserver::CreateInodeResponse;
using curvefs::metaserver::UpdateInodeRequest;
using curvefs::metaserver::UpdateInodeResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::UpdateInodeS3VersionRequest;
using curvefs::metaserver::UpdateInodeS3VersionResponse;
// partition
using curvefs::metaserver::CreatePartitionRequest;
using curvefs::metaserver::CreatePartitionResponse;
using curvefs::metaserver::DeletePartitionRequest;
using curvefs::metaserver::DeletePartitionResponse;

using ::curvefs::metaserver::copyset::OnSnapshotSaveDoneClosure;

class MetaStore {
 public:
    MetaStore() = default;
    virtual ~MetaStore() = default;

    virtual bool Load(const std::string& path) = 0;
    virtual bool Save(const std::string& path,
                      OnSnapshotSaveDoneClosure* done) = 0;
    virtual bool Clear() = 0;
    virtual MetaStatusCode CreatePartition(
        const CreatePartitionRequest* request,
        CreatePartitionResponse* response) = 0;

    virtual MetaStatusCode DeletePartition(
        const DeletePartitionRequest* request,
        DeletePartitionResponse* response) = 0;

    // dentry
    virtual MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                                        CreateDentryResponse* response) = 0;

    virtual MetaStatusCode GetDentry(const GetDentryRequest* request,
                                     GetDentryResponse* response) = 0;

    virtual MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                                        DeleteDentryResponse* response) = 0;

    virtual MetaStatusCode ListDentry(const ListDentryRequest* request,
                                      ListDentryResponse* response) = 0;

    // inode
    virtual MetaStatusCode CreateInode(const CreateInodeRequest* request,
                                       CreateInodeResponse* response) = 0;

    virtual MetaStatusCode CreateRootInode(
        const CreateRootInodeRequest* request,
        CreateRootInodeResponse* response) = 0;

    virtual MetaStatusCode GetInode(const GetInodeRequest* request,
                                    GetInodeResponse* response) = 0;

    virtual MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                                       DeleteInodeResponse* response) = 0;

    virtual MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                                       UpdateInodeResponse* response) = 0;

    virtual MetaStatusCode UpdateInodeS3Version(
        const UpdateInodeS3VersionRequest* request,
        UpdateInodeS3VersionResponse* response) = 0;

    virtual MetaStatusCode PrepareRenameTx(
        const PrepareRenameTxRequest* request,
        PrepareRenameTxResponse* response) = 0;
};

class MetaStoreImpl : public MetaStore {
 public:
    MetaStoreImpl();

    bool Load(const std::string& path);
    bool Save(const std::string& path, OnSnapshotSaveDoneClosure* done);
    bool Clear();
    MetaStatusCode CreatePartition(const CreatePartitionRequest* request,
                                   CreatePartitionResponse* response);

    MetaStatusCode DeletePartition(const DeletePartitionRequest* request,
                                   DeletePartitionResponse* response);

    // dentry
    MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                                CreateDentryResponse* response);

    MetaStatusCode GetDentry(const GetDentryRequest* request,
                             GetDentryResponse* response);

    MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                                DeleteDentryResponse* response);

    MetaStatusCode ListDentry(const ListDentryRequest* request,
                              ListDentryResponse* response);

    // inode
    MetaStatusCode CreateInode(const CreateInodeRequest* request,
                               CreateInodeResponse* response);

    MetaStatusCode CreateRootInode(const CreateRootInodeRequest* request,
                                   CreateRootInodeResponse* response);

    MetaStatusCode GetInode(const GetInodeRequest* request,
                            GetInodeResponse* response);

    MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                               DeleteInodeResponse* response);

    MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                               UpdateInodeResponse* response);

    MetaStatusCode UpdateInodeS3Version(
        const UpdateInodeS3VersionRequest* request,
        UpdateInodeS3VersionResponse* response);

    MetaStatusCode PrepareRenameTx(const PrepareRenameTxRequest* request,
                                   PrepareRenameTxResponse* response);

    std::shared_ptr<Partition> GetPartition(uint32_t partitionId);

 private:
    void SaveBack(const std::string& path, OnSnapshotSaveDoneClosure* done);

 private:
    RWLock rwLock_;  // protect partitionMap_
    std::map<uint32_t, std::shared_ptr<Partition>> partitionMap_;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_METASTORE_H_
