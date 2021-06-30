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
 * @Date: 2021-06-02 16:38:47
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_SPACE_CLIENT_H_
#define CURVEFS_SRC_MDS_SPACE_CLIENT_H_

#include <brpc/channel.h>
#include <string>
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"

using curvefs::common::Volume;

namespace curvefs {
namespace mds {
struct SpaceOptions {
    std::string spaceAddr;
    uint32_t rpcTimeoutMs;
};

struct SpaceStat {
    uint64_t blockSize;
    uint64_t totalBlock;
    uint64_t availabalBlock;
    uint64_t usedBlock;
};

class SpaceClient {
 public:
    explicit SpaceClient(const SpaceOptions& option) { options_ = option; }

    bool Init();

    void Uninit();

    FSStatusCode InitSpace(const FsInfo& fsInfo);

    FSStatusCode UnInitSpace(uint32_t fsId);

 private:
    SpaceOptions options_;
    bool inited_;
    brpc::Channel channel_;
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_SPACE_CLIENT_H_
