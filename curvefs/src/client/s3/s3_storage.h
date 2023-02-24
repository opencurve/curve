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
 * Created Date: 22-04-21
 * Author: huyao (baijiaruo)
 */


/*
#ifndef CURVEFS_SRC_CLIENT_S3_S3_STORAGE_H_
#define CURVEFS_SRC_CLIENT_S3_S3_STORAGE_H_

#include "curvefs/src/client/cache/client_cache_manager.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/under_storage.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/volume/common.h"





using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::S3ChunkInfoList;
using ::curvefs::metaserver::S3ChunkInfo;


using ::curvefs::volume::WritePart;
using ::curvefs::volume::ReadPart;

namespace curvefs {
namespace client {

class S3Storage : public UnderStorage {
 public:
    S3Storage(std::shared_ptr<S3Client> s3Client,
              std::shared_ptr<InodeCacheManager> inodeManager,
              std::shared_ptr<MdsClient> mdsClient) {}
    virtual ~S3Storage() {}
    virtual ssize_t Write(uint64_t ino, off_t offset, size_t len, const char *data)  {return 11;}
    virtual ssize_t Read(uint64_t ino, off_t offset, size_t len, char *data,
                 std::vector<ReadPart> *miss = nullptr) {return 1;}
    virtual CURVEFS_ERROR Truncate(uint64_t ino, size_t size) { return CURVEFS_ERROR:OK;}

 private:


 private:
    uin64_t chunkSize_;
    uint64_t blockSize_;
    std::shared_ptr<S3Client> s3Client_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<MdsClient> mdsClient_;
  //  std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl_;
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_S3_S3_STORAGE_H_

*/