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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_

#include <time.h>

#include <list>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <utility>
#include <map>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/common/rpc_stream.h"
#include "absl/types/optional.h"

using ::curvefs::client::metric::MetaServerClientMetric;
using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::InodeAttr;
using ::curvefs::metaserver::XAttr;
using ::curvefs::metaserver::MetaStatusCode;
using ::curvefs::metaserver::S3ChunkInfoList;
using ::curvefs::metaserver::DeallocatableBlockGroup;
using ::curvefs::common::StreamStatus;
using ::curvefs::common::StreamClient;
using ::curvefs::metaserver::Time;

using DeallocatableBlockGroupMap = std::map<uint64_t, DeallocatableBlockGroup>;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

namespace curvefs {
namespace client {
namespace rpcclient {

using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;
using ::curvefs::metaserver::VolumeExtentSliceList;

struct DataIndices {
    absl::optional<S3ChunkInfoMap> s3ChunkInfoMap;
    absl::optional<VolumeExtentSliceList> volumeExtents;
};

inline void SetCreateTime(Time* tm) {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    tm->set_sec(now.tv_sec);
    tm->set_nsec(now.tv_nsec);
}

class MetaServerClient {
 public:
    virtual ~MetaServerClient() = default;

    virtual MetaStatusCode
    Init(const ExcutorOpt &excutorOpt, const ExcutorOpt &excutorInternalOpt,
         std::shared_ptr<MetaCache> metaCache,
         std::shared_ptr<ChannelManager<MetaserverID>> channelManager) = 0;

    virtual MetaStatusCode GetTxId(uint32_t fsId, uint64_t inodeId,
                                   uint32_t *partitionId, uint64_t *txId) = 0;

    virtual void SetTxId(uint32_t partitionId, uint64_t txId) = 0;

    virtual MetaStatusCode GetDentry(uint32_t fsId, uint64_t inodeid,
                                     const std::string &name, Dentry *out) = 0;

    virtual MetaStatusCode ListDentry(uint32_t fsId, uint64_t inodeid,
                                      const std::string &last, uint32_t count,
                                      bool onlyDir,
                                      std::list<Dentry> *dentryList) = 0;

    virtual MetaStatusCode CreateDentry(const Dentry &dentry) = 0;

    virtual MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t inodeid,
                                        const std::string &name,
                                        FsFileType type) = 0;

    virtual MetaStatusCode
    PrepareRenameTx(const std::vector<Dentry> &dentrys) = 0;

    virtual MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeid,
                                    Inode *out, bool* streaming) = 0;

    virtual MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeid,
                                        InodeAttr *attr) = 0;

    virtual MetaStatusCode BatchGetInodeAttr(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::list<InodeAttr> *attr) = 0;

    virtual MetaStatusCode BatchGetInodeAttrAsync(uint32_t fsId,
        const std::vector<uint64_t> &inodeIds, MetaServerClientDone *done) = 0;

    virtual MetaStatusCode BatchGetXAttr(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::list<XAttr> *xattr) = 0;

    virtual MetaStatusCode UpdateInodeAttr(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr) = 0;

    virtual MetaStatusCode UpdateInodeAttrWithOutNlink(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr,
        S3ChunkInfoMap* s3ChunkInfoAdd = nullptr,
        bool internal = false) = 0;

    virtual void UpdateInodeWithOutNlinkAsync(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr,
        MetaServerClientDone* done,
        DataIndices&& indices = {}) = 0;

    virtual MetaStatusCode GetOrModifyS3ChunkInfo(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        bool returnS3ChunkInfoMap = false,
        google::protobuf::Map<
            uint64_t, S3ChunkInfoList> *out = nullptr,
            bool internal = false) = 0;

    virtual void GetOrModifyS3ChunkInfoAsync(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        MetaServerClientDone *done) = 0;

    virtual MetaStatusCode CreateInode(const InodeParam &param, Inode *out) = 0;

    virtual MetaStatusCode CreateManageInode(const InodeParam &param,
                                             Inode *out) = 0;

    virtual MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeid) = 0;

    virtual bool SplitRequestInodes(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::vector<std::vector<uint64_t>> *inodeGroups) = 0;

    virtual void AsyncUpdateVolumeExtent(uint32_t fsId,
                                         uint64_t inodeId,
                                         const VolumeExtentSliceList &extents,
                                         MetaServerClientDone *done) = 0;

    virtual MetaStatusCode GetVolumeExtent(uint32_t fsId,
                                           uint64_t inodeId,
                                           bool streaming,
                                           VolumeExtentSliceList *extents) = 0;

    virtual MetaStatusCode
    UpdateDeallocatableBlockGroup(uint32_t fsId, uint64_t inodeId,
                                  DeallocatableBlockGroupMap *statistic) = 0;
};

class MetaServerClientImpl : public MetaServerClient {
 public:
    MetaServerClientImpl() = default;

    MetaStatusCode
    Init(const ExcutorOpt &excutorOpt, const ExcutorOpt &excutorInternalOpt,
         std::shared_ptr<MetaCache> metaCache,
         std::shared_ptr<ChannelManager<MetaserverID>> channelManager) override;

    MetaStatusCode GetTxId(uint32_t fsId, uint64_t inodeId,
                           uint32_t *partitionId, uint64_t *txId) override;

    void SetTxId(uint32_t partitionId, uint64_t txId) override;

    MetaStatusCode GetDentry(uint32_t fsId, uint64_t inodeid,
                             const std::string &name, Dentry *out) override;

    MetaStatusCode ListDentry(uint32_t fsId, uint64_t inodeid,
                              const std::string &last, uint32_t count,
                              bool onlyDir,
                              std::list<Dentry> *dentryList) override;

    MetaStatusCode CreateDentry(const Dentry &dentry) override;

    MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t inodeid,
                                const std::string &name,
                                FsFileType type) override;

    MetaStatusCode PrepareRenameTx(const std::vector<Dentry> &dentrys) override;

    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeid,
                            Inode *out, bool* streaming) override;

    MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeid,
                                InodeAttr *attr) override;

    MetaStatusCode BatchGetInodeAttr(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::list<InodeAttr> *attr) override;

    MetaStatusCode BatchGetInodeAttrAsync(uint32_t fsId,
        const std::vector<uint64_t> &inodeIds,
        MetaServerClientDone *done) override;

    MetaStatusCode BatchGetXAttr(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::list<XAttr> *xattr) override;

    MetaStatusCode UpdateInodeAttr(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr) override;

    MetaStatusCode UpdateInodeAttrWithOutNlink(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr,
        S3ChunkInfoMap* s3ChunkInfoAdd = nullptr,
        bool internal = false) override;

    void UpdateInodeWithOutNlinkAsync(
        uint32_t fsId,
        uint64_t inodeId,
        const InodeAttr& attr,
        MetaServerClientDone* done,
        DataIndices&& indices = {}) override;

    MetaStatusCode GetOrModifyS3ChunkInfo(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        bool returnS3ChunkInfoMap = false,
        google::protobuf::Map<
            uint64_t, S3ChunkInfoList> *out = nullptr,
            bool internal = false) override;

    void GetOrModifyS3ChunkInfoAsync(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        MetaServerClientDone *done) override;

    MetaStatusCode CreateInode(const InodeParam &param, Inode *out) override;

    MetaStatusCode CreateManageInode(const InodeParam &param,
                                     Inode *out) override;

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeid) override;

    bool SplitRequestInodes(uint32_t fsId,
        const std::set<uint64_t> &inodeIds,
        std::vector<std::vector<uint64_t>> *inodeGroups) override;

    void AsyncUpdateVolumeExtent(uint32_t fsId,
                                 uint64_t inodeId,
                                 const VolumeExtentSliceList &extents,
                                 MetaServerClientDone *done) override;

    MetaStatusCode GetVolumeExtent(uint32_t fsId,
                                   uint64_t inodeId,
                                   bool streaming,
                                   VolumeExtentSliceList *extents) override;

    MetaStatusCode UpdateDeallocatableBlockGroup(
        uint32_t fsId, uint64_t inodeId,
        DeallocatableBlockGroupMap *statistic) override;

 private:
    MetaStatusCode UpdateInode(const UpdateInodeRequest &request,
                               bool internal = false);

    void UpdateInodeAsync(const UpdateInodeRequest &request,
                          MetaServerClientDone *done);

    bool ParseS3MetaStreamBuffer(butil::IOBuf* buffer,
                                 uint64_t* chunkIndex,
                                 S3ChunkInfoList* list);

    bool HandleS3MetaStreamBuffer(butil::IOBuf* buffer, S3ChunkInfoMap* out);

 private:
    ExcutorOpt opt_;
    ExcutorOpt optInternal_;

    std::shared_ptr<MetaCache> metaCache_;
    std::shared_ptr<ChannelManager<MetaserverID>> channelManager_;

    StreamClient streamClient_;
    MetaServerClientMetric metric_;
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
