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
 * Date: Wed Aug 11 14:19:56 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_COPYSET_SERVICE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_COPYSET_SERVICE_H_

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class CopysetServiceImpl : public CopysetService {
 public:
    explicit CopysetServiceImpl(CopysetNodeManager* manager)
        : manager_(manager) {}

    void CreateCopysetNode(google::protobuf::RpcController* controller,
                           const CreateCopysetRequest* request,
                           CreateCopysetResponse* response,
                           google::protobuf::Closure* done) override;

    void GetCopysetStatus(google::protobuf::RpcController* controller,
                          const CopysetStatusRequest* request,
                          CopysetStatusResponse* response,
                          google::protobuf::Closure* done) override;

    void GetCopysetsStatus(google::protobuf::RpcController* controller,
                           const CopysetsStatusRequest* request,
                           CopysetsStatusResponse* response,
                           google::protobuf::Closure* done) override;

 private:
    COPYSET_OP_STATUS CreateOneCopyset(
        const CreateCopysetRequest::Copyset& copyset);

    void GetOneCopysetStatus(const CopysetStatusRequest& request,
                             CopysetStatusResponse* response);

 private:
    CopysetNodeManager* manager_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_COPYSET_SERVICE_H_
