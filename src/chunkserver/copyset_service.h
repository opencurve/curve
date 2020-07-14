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
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_COPYSET_SERVICE_H_
#define SRC_CHUNKSERVER_COPYSET_SERVICE_H_

#include "proto/copyset.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class CopysetNodeManager;

/**
 * 复制组管理的Rpc服务，目前仅有创建复制组
 */
class CopysetServiceImpl : public CopysetService {
 public:
    explicit CopysetServiceImpl(CopysetNodeManager* copysetNodeManager) :
        copysetNodeManager_(copysetNodeManager) {}
    ~CopysetServiceImpl() {}

    /**
     * 创建复制组，一次只能创建一个
     */
    void CreateCopysetNode(RpcController *controller,
                           const CopysetRequest *request,
                           CopysetResponse *response,
                           Closure *done);

    /*
     * 创建复制组，一次可以创建多个
     */
    void CreateCopysetNode2(RpcController *controller,
                            const CopysetRequest2 *request,
                            CopysetResponse2 *response,
                            Closure *done);

    void GetCopysetStatus(RpcController *controller,
                          const CopysetStatusRequest *request,
                          CopysetStatusResponse *response,
                          Closure *done);

 private:
    // 复制组管理者
    CopysetNodeManager* copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_SERVICE_H_
