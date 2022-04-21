/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday Apr 28 15:04:00 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/streaming_utils.h"

#include <butil/iobuf.h>
#include <glog/logging.h>

#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {

MetaStatusCode StreamingSendVolumeExtent(StreamConnection* connection,
                                         const VolumeExtentList& extents) {
    VLOG(9) << "StreamingSendVolumeExtent, extents: "
            << extents.ShortDebugString();
    for (const auto& slice : extents.slices()) {
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!slice.SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Serialize volume slice failed, slice: "
                       << slice.ShortDebugString();
            return MetaStatusCode::PARAM_ERROR;
        }

        if (!connection->Write(data)) {
            LOG(ERROR) << "Stream write failed, slice: "
                       << slice.ShortDebugString();
            return MetaStatusCode::RPC_STREAM_ERROR;
        }
    }

    if (!connection->WriteDone()) {
        LOG(ERROR) << "Stream write done failed in server side";
        return MetaStatusCode::RPC_STREAM_ERROR;
    }

    return MetaStatusCode::OK;
}

}  // namespace metaserver
}  // namespace curvefs
