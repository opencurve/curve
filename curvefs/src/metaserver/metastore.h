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
#include "curvefs/src/metaserver/copyset/snapshot_closure.h"
#include "curvefs/src/metaserver/partition.h"
#include "curvefs/src/metaserver/metastore_fstream.h"
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/common/rpc_stream.h"

namespace curvefs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

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
using curvefs::metaserver::BatchGetInodeAttrRequest;
using curvefs::metaserver::BatchGetInodeAttrResponse;
using curvefs::metaserver::BatchGetXAttrRequest;
using curvefs::metaserver::BatchGetXAttrResponse;
using curvefs::metaserver::CreateInodeRequest;
using curvefs::metaserver::CreateInodeResponse;
using curvefs::metaserver::UpdateInodeRequest;
using curvefs::metaserver::UpdateInodeResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;

// partition
using curvefs::metaserver::CreatePartitionRequest;
using curvefs::metaserver::CreatePartitionResponse;
using curvefs::metaserver::DeletePartitionRequest;
using curvefs::metaserver::DeletePartitionResponse;

using ::curvefs::metaserver::copyset::OnSnapshotSaveDoneClosure;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::common::StreamServer;
using ::curvefs::common::StreamConnection;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

class MetaStore {
 public:
    MetaStore() = default;
    virtual ~MetaStore() = default;

    virtual bool Load(const std::string& pathname) = 0;
    virtual bool Save(const std::string& path,
                      OnSnapshotSaveDoneClosure* done) = 0;
    virtual bool Clear() = 0;
    virtual MetaStatusCode CreatePartition(
        const CreatePartitionRequest* request,
        CreatePartitionResponse* response) = 0;

    virtual MetaStatusCode DeletePartition(
        const DeletePartitionRequest* request,
        DeletePartitionResponse* response) = 0;

    virtual bool GetPartitionInfoList(
                            std::list<PartitionInfo> *partitionInfoList) = 0;

    virtual std::shared_ptr<StreamServer> GetStreamServer() = 0;

    // dentry
    virtual MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                                        CreateDentryResponse* response) = 0;

    virtual MetaStatusCode GetDentry(const GetDentryRequest* request,
                                     GetDentryResponse* response) = 0;

    virtual MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                                        DeleteDentryResponse* response) = 0;

    virtual MetaStatusCode ListDentry(const ListDentryRequest* request,
                                      ListDentryResponse* response) = 0;

    virtual MetaStatusCode PrepareRenameTx(
        const PrepareRenameTxRequest* request,
        PrepareRenameTxResponse* response) = 0;

    // inode
    virtual MetaStatusCode CreateInode(const CreateInodeRequest* request,
                                       CreateInodeResponse* response) = 0;

    virtual MetaStatusCode CreateRootInode(
        const CreateRootInodeRequest* request,
        CreateRootInodeResponse* response) = 0;

    virtual MetaStatusCode GetInode(const GetInodeRequest* request,
                                    GetInodeResponse* response) = 0;

    virtual MetaStatusCode BatchGetInodeAttr(
                                    const BatchGetInodeAttrRequest* request,
                                    BatchGetInodeAttrResponse* response) = 0;

    virtual MetaStatusCode BatchGetXAttr(const BatchGetXAttrRequest* request,
                                    BatchGetXAttrResponse* response) = 0;

    virtual MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                                       DeleteInodeResponse* response) = 0;

    virtual MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                                       UpdateInodeResponse* response) = 0;

    virtual MetaStatusCode GetOrModifyS3ChunkInfo(
        const GetOrModifyS3ChunkInfoRequest* request,
        GetOrModifyS3ChunkInfoResponse* response,
        std::shared_ptr<Iterator>* iterator) = 0;

    virtual MetaStatusCode SendS3ChunkInfoByStream(
        std::shared_ptr<StreamConnection> connection,
        std::shared_ptr<Iterator> iterator) = 0;

    virtual MetaStatusCode GetVolumeExtent(
        const GetVolumeExtentRequest* request,
        GetVolumeExtentResponse* response) = 0;

    virtual MetaStatusCode UpdateVolumeExtent(
        const UpdateVolumeExtentRequest* request,
        UpdateVolumeExtentResponse* response) = 0;
};

class MetaStoreImpl : public MetaStore {
 public:
    MetaStoreImpl(copyset::CopysetNode* node,
                  std::shared_ptr<KVStorage> kvStorage_);

    bool Load(const std::string& pathname) override;
    bool Save(const std::string& path,
        OnSnapshotSaveDoneClosure* done) override;
    bool Clear() override;

    MetaStatusCode CreatePartition(const CreatePartitionRequest* request,
                                   CreatePartitionResponse* response) override;

    MetaStatusCode DeletePartition(const DeletePartitionRequest* request,
                                   DeletePartitionResponse* response) override;

    bool GetPartitionInfoList(
                        std::list<PartitionInfo> *partitionInfoList) override;

    std::shared_ptr<StreamServer> GetStreamServer() override;

    // dentry
    MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                                CreateDentryResponse* response) override;

    MetaStatusCode GetDentry(const GetDentryRequest* request,
                             GetDentryResponse* response) override;

    MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                                DeleteDentryResponse* response) override;

    MetaStatusCode ListDentry(const ListDentryRequest* request,
                              ListDentryResponse* response) override;

    MetaStatusCode PrepareRenameTx(const PrepareRenameTxRequest* request,
                                   PrepareRenameTxResponse* response) override;

    // inode
    MetaStatusCode CreateInode(const CreateInodeRequest* request,
                               CreateInodeResponse* response) override;

    MetaStatusCode CreateRootInode(const CreateRootInodeRequest* request,
                                   CreateRootInodeResponse* response) override;

    MetaStatusCode GetInode(const GetInodeRequest* request,
                            GetInodeResponse* response) override;

    MetaStatusCode BatchGetInodeAttr(const BatchGetInodeAttrRequest* request,
                                BatchGetInodeAttrResponse* response) override;

    MetaStatusCode BatchGetXAttr(const BatchGetXAttrRequest* request,
                                 BatchGetXAttrResponse* response) override;

    MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                               DeleteInodeResponse* response) override;

    MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                               UpdateInodeResponse* response) override;

    std::shared_ptr<Partition> GetPartition(uint32_t partitionId);

    MetaStatusCode GetOrModifyS3ChunkInfo(
        const GetOrModifyS3ChunkInfoRequest* request,
        GetOrModifyS3ChunkInfoResponse* response,
        std::shared_ptr<Iterator>* iterator) override;

    MetaStatusCode SendS3ChunkInfoByStream(
        std::shared_ptr<StreamConnection> connection,
        std::shared_ptr<Iterator> iterator) override;

    MetaStatusCode GetVolumeExtent(const GetVolumeExtentRequest* request,
                                   GetVolumeExtentResponse* response) override;

    MetaStatusCode UpdateVolumeExtent(
        const UpdateVolumeExtentRequest* request,
        UpdateVolumeExtentResponse* response) override;

 private:
    void PrepareStreamBuffer(butil::IOBuf* buffer,
                             uint64_t chunkIndex,
                             const std::string& value);

    void SaveBackground(const std::string& path,
                        DumpFileClosure* child,
                        OnSnapshotSaveDoneClosure* done);

 private:
    RWLock rwLock_;  // protect partitionMap_
    std::shared_ptr<KVStorage> kvStorage_;
    std::map<uint32_t, std::shared_ptr<Partition>> partitionMap_;
    std::list<uint32_t> partitionIds_;

    copyset::CopysetNode* copysetNode_;

    std::shared_ptr<StreamServer> streamServer_;
};

}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_METASTORE_H_
