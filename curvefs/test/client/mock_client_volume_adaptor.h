/*
 * @Author: hzwuhongsong
 * @Date: 2023-03-27 16:19:22
 * @LastEditors: hzwuhongsong hzwuhongsong@corp.netease.com
 * @LastEditTime: 2023-04-19 17:21:51
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

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_VOLUME_ADAPTOR_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_VOLUME_ADAPTOR_H_

#include <gmock/gmock.h>

#include <memory>
#include <string>

#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/client_storage_adaptor.h"

namespace curvefs {
namespace client {

class MockVolumeClientAdaptor : public VolumeClientAdaptorImpl {
 public:
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
    MOCK_METHOD7(Init, CURVEFS_ERROR(
        const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         std::shared_ptr<KVClientManager> kvClientManager,
         std::shared_ptr<FsInfo> fsInfo));


    MOCK_METHOD2(FuseOpInit, CURVEFS_ERROR(void *userdata,
      struct fuse_conn_info *conn));

    MOCK_METHOD2(FlushDataCache, CURVEFS_ERROR(const UperFlushRequest& req,
      uint64_t* writeOffset));

    MOCK_METHOD1(ReadFromLowlevel, CURVEFS_ERROR(UperReadRequest request));

    MOCK_METHOD4(Write, int(uint64_t inodeId, uint64_t offset,
                               uint64_t length, const char *buf));

    MOCK_METHOD4(Read, int(uint64_t inodeId, uint64_t offset,
      uint64_t length, char *buf));

    MOCK_METHOD2(Truncate, CURVEFS_ERROR(InodeWrapper *inodeWrapper,
      uint64_t size));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_VOLUME_ADAPTOR_H_
