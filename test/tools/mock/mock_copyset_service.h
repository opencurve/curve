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
 * Project: Curve
 * Created Date: Tue June 1 2021
 * Author: Jingli Chen (Wine93)
 */

#ifndef TEST_TOOLS_MOCK_MOCK_COPYSET_SERVICE_H_
#define TEST_TOOLS_MOCK_MOCK_COPYSET_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "proto/copyset.pb.h"

namespace curve {
namespace tool {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::chunkserver::CopysetService;
using ::curve::chunkserver::CopysetRequest;
using ::curve::chunkserver::CopysetResponse;

class MockCopysetService : public CopysetService {
 public:
    MOCK_METHOD4(DeleteBrokenCopyset,
                 void(RpcController* controller,
                      const CopysetRequest* request,
                      CopysetResponse* response,
                      Closure* done));
};

}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_COPYSET_SERVICE_H_
