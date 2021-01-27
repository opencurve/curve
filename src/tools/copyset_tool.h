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
 * Created Date: 2021-02-03
 * Author: charisu
 */

#ifndef SRC_TOOLS_COPYSET_TOOL_H_
#define SRC_TOOLS_COPYSET_TOOL_H_

#include <memory>
#include <string>
#include <vector>
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"
#include "src/tools/mds_client.h"
#include "src/tools/copyset_check_core.h"

namespace curve {
namespace tool {

class CopysetTool : public CurveTool {
 public:
    CopysetTool(std::shared_ptr<CopysetCheckCore> copysetCheck,
                std::shared_ptr<MDSClient> mdsClient) :
                    copysetCheck_(copysetCheck), mdsClient_(mdsClient),
                    inited_(false) {}

    int RunCommand(const std::string& command) override;

    void PrintHelp(const std::string& command) override;

    static bool SupportCommand(const std::string& command);

 private:
    int Init();
    int SetCopysetsUnAvailable();
    int SetCopysetsAvailable();

    std::shared_ptr<CopysetCheckCore> copysetCheck_;
    std::shared_ptr<MDSClient> mdsClient_;
    bool inited_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_TOOL_H_
