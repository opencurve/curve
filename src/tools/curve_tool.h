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
 * Created Date: 2019-12-27
 * Author: charisu
 */

#ifndef SRC_TOOLS_CURVE_TOOL_H_
#define SRC_TOOLS_CURVE_TOOL_H_

#include <string>

namespace curve {
namespace tool {

class CurveTool {
 public:
    virtual int RunCommand(const std::string& command) = 0;
    virtual void PrintHelp(const std::string& command) = 0;
    virtual ~CurveTool() {}
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_H_
