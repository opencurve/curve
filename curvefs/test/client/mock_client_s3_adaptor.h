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

#include <memory>
#include <string>

#include "curvefs/src/client/s3/client_s3_adaptor.h"

namespace curvefs {
namespace client {

class MockS3ClientAdaptor : public S3ClientAdaptor {
 public:
    MockS3ClientAdaptor() {}
    ~MockS3ClientAdaptor() {}
/*
    MOCK_METHOD5(Init,
                 CURVEFS_ERROR(const S3ClientAdaptorOption &option,
                               std::shared_ptr<S3Client> client,
                               std::shared_ptr<InodeCacheManager> inodeManager,
                               std::shared_ptr<MdsClient> mdsClient,
                               std::shared_ptr<DiskCacheManagerImpl>
                                   diskcacheManagerImpl));
    MOCK_METHOD4(Init,
                 CURVEFS_ERROR(const S3ClientAdaptorOption& option,
                               std::shared_ptr<S3Client> client,
                               std::shared_ptr<InodeCacheManager> inodeManager,
                               std::shared_ptr<MdsClient> mdsClient));*/
    MOCK_METHOD7(Init,
                 CURVEFS_ERROR(const S3ClientAdaptorOption &option,
                               std::shared_ptr<S3Client> client,
                               std::shared_ptr<InodeCacheManager> inodeManager,
                               std::shared_ptr<MdsClient> mdsClient,
                               std::shared_ptr<FsCacheManager> fsCacheManager,
                               std::shared_ptr<DiskCacheManagerImpl>
                                   diskCacheManagerImpl,
                               bool startBackGround));

    MOCK_METHOD4(Write, int(uint64_t inodeId, uint64_t offset, uint64_t length,
                            const char* buf));

    MOCK_METHOD4(Read, int(uint64_t inodeId, uint64_t offset, uint64_t length,
                           char* buf));
    MOCK_METHOD1(ReleaseCache, void(uint64_t inodeId));
    MOCK_METHOD1(Flush, CURVEFS_ERROR(uint64_t inodeId));
    MOCK_METHOD1(FlushAllCache, CURVEFS_ERROR(uint64_t inodeId));
    MOCK_METHOD0(FsSync, CURVEFS_ERROR());
    MOCK_METHOD0(Stop, int());
    MOCK_METHOD2(Truncate, CURVEFS_ERROR(Inode* inode, uint64_t size));
    MOCK_METHOD2(AllocS3ChunkId,
                 FSStatusCode(uint32_t fsId, uint64_t* chunkId));
    MOCK_METHOD1(SetFsId, void(uint32_t fsId));
    MOCK_METHOD1(InitMetrics, void(const std::string &fsName));
    MOCK_METHOD3(CollectMetrics,
                 void(InterfaceMetric *interface, int count, uint64_t start));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_
