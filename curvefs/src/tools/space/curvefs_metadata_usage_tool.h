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
 * Created Date: 2021-10-22
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_SPACE_CURVEFS_METADATA_USAGE_TOOL_H_
#define CURVEFS_SRC_TOOLS_SPACE_CURVEFS_METADATA_USAGE_TOOL_H_

#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
<<<<<<< HEAD
#include "curvefs/src/tools/space/space_base_tool.h"
=======
namespace tools {
namespace space {

/**
 * @brief  this class is used to query the metadata usage of cluster
 *
 * @details
 */
class MatedataUsageTool
    : public CurvefsToolRpc<brpc::Channel, brpc::Controller,
                            curvefs::mds::StatMetadataUsageRequest,
                            curvefs::mds::StatMetadataUsageResponse,
                            curvefs::mds::MdsService_Stub> {
 public:
    MatedataUsageTool()
        : CurvefsToolRpc(std::string(kMetedataUsageCmd),
                         std::string(kProgrameName)) {}
    void PrintHelp() override;

 protected:
<<<<<<< HEAD
    void AddUpdateFlagsFuncs() override;
    bool AfterSendRequestToService(const std::string& host) override;
=======
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
>>>>>>> dcb6dde5

 private:
    int Init() override;
};

}  // namespace space
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_SPACE_CURVEFS_METADATA_USAGE_TOOL_H_
