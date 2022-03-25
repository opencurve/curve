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
 * Date: Mon Aug  9 15:15:23 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_RAFT_LOG_CODEC_H_
#define CURVEFS_SRC_METASERVER_COPYSET_RAFT_LOG_CODEC_H_

#include <memory>

#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class RaftLogCodec {
 public:
    /**
     * @brief Encode request and type to butil::IOBuf
     */
    static bool Encode(OperatorType type,
                       const google::protobuf::Message* request,
                       butil::IOBuf* log);

    /**
     * @brief Decode from butil::IOBuf and create corresponding metaoperator
     */
    static std::unique_ptr<MetaOperator> Decode(CopysetNode* node,
                                                butil::IOBuf log);

 private:
    static constexpr size_t kOperatorTypeSize = sizeof(OperatorType);
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_RAFT_LOG_CODEC_H_
