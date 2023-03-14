/*
 * @Author: hzwuhongsong
 * @Date: 2023-03-27 16:19:22
 * @LastEditors: hzwuhongsong hzwuhongsong@corp.netease.com
 * @LastEditTime: 2023-04-11 20:27:45
 * @Description: cursorMoveOnType
 */
/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_

#include <gmock/gmock.h>

#include <memory>
#include <string>

#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/client_storage_adaptor.h"

namespace curvefs {
namespace client {

class MockS3ClientAdaptor : public StorageAdaptor {
 public:
    MockS3ClientAdaptor() : StorageAdaptor() {
    }
    ~MockS3ClientAdaptor() {
        StorageAdaptor::Stop();
    }

    MOCK_METHOD7(Init,
        CURVEFS_ERROR(const S3ClientAdaptorOption &option,
            std::shared_ptr<InodeCacheManager> inodeManager,
            std::shared_ptr<MdsClient> mdsClient,
            std::shared_ptr<FsCacheManager> fsCacheManager,
            std::shared_ptr<DiskCacheManagerImpl>
                diskCacheManagerImpl,
            std::shared_ptr<KVClientManager>
                kvClientManager,
            std::shared_ptr<FsInfo> fsInfo));

    MOCK_METHOD2(FuseOpInit, CURVEFS_ERROR(void *userdata,
      struct fuse_conn_info *conn));

    MOCK_METHOD2(FlushDataCache, CURVEFS_ERROR(const UperFlushRequest& req,
      uint64_t* writeOffset));

    MOCK_METHOD1(ReadFromLowlevel, CURVEFS_ERROR(UperReadRequest request));

    MOCK_METHOD4(Write, int(uint64_t inodeId, uint64_t offset, uint64_t length,
                            const char* buf));

    MOCK_METHOD4(Read, int(uint64_t inodeId, uint64_t offset, uint64_t length,
                           char* buf));
    MOCK_METHOD1(ReleaseCache, void(uint64_t inodeId));
    MOCK_METHOD1(Flush, CURVEFS_ERROR(uint64_t inodeId));
    MOCK_METHOD1(FlushAllCache, CURVEFS_ERROR(uint64_t inodeId));
    MOCK_METHOD0(FsSync, CURVEFS_ERROR());
    MOCK_METHOD0(Stop, int());
    MOCK_METHOD2(Truncate, CURVEFS_ERROR(InodeWrapper* inode, uint64_t size));
    MOCK_METHOD3(AllocS3ChunkId, FSStatusCode(uint32_t fsId, uint32_t idNum,
                                              uint64_t *chunkId));
    MOCK_METHOD1(SetFsId, void(uint32_t fsId));
    MOCK_METHOD1(InitMetrics, void(const std::string &fsName));
    MOCK_METHOD3(CollectMetrics,
                 void(InterfaceMetric *interface, int count, uint64_t start));
    MOCK_METHOD0(GetDiskCacheManager, std::shared_ptr<DiskCacheManagerImpl>());
    MOCK_METHOD0(GetS3Client, std::shared_ptr<S3Client>());
    MOCK_METHOD0(GetBlockSize, uint64_t());
    MOCK_METHOD0(GetChunkSize, uint64_t());
    MOCK_METHOD0(GetObjectPrefix, uint32_t());
    MOCK_METHOD0(HasDiskCache, bool());
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_
