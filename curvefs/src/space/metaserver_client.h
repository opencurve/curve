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

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_SPACE_METASERVER_CLIENT_H_
#define CURVEFS_SRC_SPACE_METASERVER_CLIENT_H_

#include <brpc/channel.h>

#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/space/common.h"
#include "curvefs/src/space/config.h"

namespace curvefs {
namespace space {

class MetaServerClient {
 public:
    MetaServerClient() = default;

    ~MetaServerClient() = default;

    bool Init(const MetaServerClientOption& opt);

    /**
     * @brief Get all file extents that belongs to `fsId`
     */
    bool GetAllInodeExtents(uint32_t fsId, uint64_t rootInodeId, Extents* exts);

 private:
    /**
     * @brief Recursive traverse all files in the filesystem, and store files'
     * extents
     */
    bool RecursiveListDentry(uint32_t fsId, uint64_t inodeId, Extents* exts);

    void AppendExtents(
        Extents* exts,
        const curvefs::metaserver::VolumeExtentList& protoExts) const;

 private:
    MetaServerClientOption opt_;
    brpc::Channel channel_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_METASERVER_CLIENT_H_
