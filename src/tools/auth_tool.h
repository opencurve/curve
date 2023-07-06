/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-06-27
 * Author: wanghai (SeanHai)
 */

#ifndef SRC_TOOLS_AUTH_TOOL_H_
#define SRC_TOOLS_AUTH_TOOL_H_

#include <memory>
#include <string>
#include "src/tools/curve_tool.h"
#include "src/tools/mds_client.h"

namespace curve {
namespace tool {

class AuthTool : public CurveTool {
 public:
    explicit AuthTool(std::shared_ptr<MDSClient> mdsClient)
        : mdsClient_(mdsClient) {}

    int RunCommand(const std::string& command) override;

    void PrintHelp(const std::string& command) override;

    static bool SupportCommand(const std::string& command);

 private:
    int Init();
    int AddKey();
    int DeleteKey();
    int GetKey();
    int UpdateKey();

 private:
    std::shared_ptr<MDSClient> mdsClient_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_AUTH_TOOL_H_
