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
 * Date: Fri Sep  3 15:16:43 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_SNAPSHOT_CLOSURE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_SNAPSHOT_CLOSURE_H_

#include <google/protobuf/stubs/callback.h>

#include "curvefs/proto/metaserver.pb.h"

namespace braft {
class SnapshotWriter;
}  // namespace braft

namespace curvefs {
namespace metaserver {
namespace copyset {

class OnSnapshotSaveDoneClosure : public google::protobuf::Closure {
 public:
    virtual ~OnSnapshotSaveDoneClosure() = default;

    virtual void SetSuccess() = 0;

    virtual void SetError(MetaStatusCode code) = 0;

    // After dump/save metadata, we should add filenames to `SnapshotWriter`
    virtual braft::SnapshotWriter* GetSnapshotWriter() const = 0;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_SNAPSHOT_CLOSURE_H_
