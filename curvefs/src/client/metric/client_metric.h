/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Created Date: Thur Oct Jun 28 2021
 * Author: lixiaocui
 */


#ifndef CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
#define CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_

#include <bvar/bvar.h>
#include <string>
#include "src/client/client_metric.h"

using curve::client::InterfaceMetric;

namespace curvefs {
namespace client {
namespace metric {

struct MDSClientMetric {
    std::string prefix;
    std::string mdsAddrs;

    InterfaceMetric mountFs;
    InterfaceMetric umountFs;
    InterfaceMetric getFsInfo;
    InterfaceMetric allocateS3Chunk;

    InterfaceMetric commitTx;

    InterfaceMetric getMetaserverInfo;

    explicit MDSClientMetric(const std::string &prefix_ = "")
        : prefix(!prefix_.empty() ? prefix_
                                  : "curvefs_mds_client_" +
                                        curve::common::ToHexString(this)),
          mountFs(prefix, "mountFs"), umountFs(prefix, "unmountFs"),
          getFsInfo(prefix, "getFsInfo"),
          allocateS3Chunk(prefix, "allocateS3Chunk"),
          commitTx(prefix, "commitTx"),
          getMetaserverInfo(prefix, "getMetaserverInfo") {}
};

struct MetaServerClientMetric {
    std::string prefix;

    // dentry
    InterfaceMetric getDentry;
    InterfaceMetric listDentry;
    InterfaceMetric createDentry;
    InterfaceMetric deleteDentry;

    // inode
    InterfaceMetric getInode;
    InterfaceMetric batchGetInodeAttr;
    InterfaceMetric batchGetXattr;
    InterfaceMetric createInode;
    InterfaceMetric updateInode;
    InterfaceMetric deleteInode;
    InterfaceMetric createRootInode;
    InterfaceMetric appendS3ChunkInfo;

    // tnx
    InterfaceMetric prepareRenameTx;

    // partition
    InterfaceMetric createPartition;

    explicit MetaServerClientMetric(const std::string &prefix_ = "")
        : prefix(!prefix_.empty() ? prefix_
                                  : "curvefs_metaserver_client_" +
                                        curve::common::ToHexString(this)),
          getDentry(prefix, "getDentry"),
          listDentry(prefix, "listDentry"),
          createDentry(prefix, "createDentry"),
          deleteDentry(prefix, "deleteDentry"),
          getInode(prefix, "getInode"),
          batchGetInodeAttr(prefix, "batchGetInodeAttr"),
          batchGetXattr(prefix, "batchGetXattr"),
          createInode(prefix, "createInode"),
          updateInode(prefix, "updateInode"),
          deleteInode(prefix, "deleteInode"),
          createRootInode(prefix, "createRootInode"),
          appendS3ChunkInfo(prefix, "appendS3ChunkInfo"),
          prepareRenameTx(prefix, "prepareRenameTx"),
          createPartition(prefix, "createPartition") {}
};

struct FSMetric {
    const std::string prefix = "curvefs_client";

    std::string fsName;

    InterfaceMetric userWrite;
    InterfaceMetric userRead;

    explicit FSMetric(const std::string &name = "")
        : fsName(!name.empty() ? name
                               : prefix + curve::common::ToHexString(this)),
          userWrite(prefix, fsName + "_userWrite"),
          userRead(prefix, fsName + "_userRead") {}
};

struct S3Metric {
    const std::string prefix = "curvefs_s3";

    std::string fsName;
    InterfaceMetric adaptorWrite;
    InterfaceMetric adaptorRead;
    InterfaceMetric adaptorWriteS3;
    InterfaceMetric adaptorWriteDiskCache;
    InterfaceMetric adaptorReadS3;
    InterfaceMetric adaptorReadDiskCache;

    explicit S3Metric(const std::string &name = "")
        : fsName(!name.empty() ? name
                               : prefix + curve::common::ToHexString(this)),
          adaptorWrite(prefix, fsName + "_adaptor_write"),
          adaptorRead(prefix, fsName + "_adaptor_read"),
          adaptorWriteS3(prefix, fsName + "_adaptor_write_s3"),
          adaptorWriteDiskCache(prefix, fsName + "_adaptor_write_disk_cache"),
          adaptorReadS3(prefix, fsName + "_adaptor_read_s3"),
          adaptorReadDiskCache(prefix, fsName + "_adaptor_read_disk_cache") {}
};

struct DiskCacheMetric {
    const std::string prefix = "curvefs_disk_cache";

    std::string fsName;
    InterfaceMetric writeS3;
    explicit DiskCacheMetric(const std::string &name = "")
        : fsName(!name.empty() ? name
                               : prefix + curve::common::ToHexString(this)),
          writeS3(prefix, fsName + "_write_s3") {}
};

}  // namespace metric
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
