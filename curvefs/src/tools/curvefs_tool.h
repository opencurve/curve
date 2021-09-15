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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_

#include <iostream>
#include <memory>
#include <string>

namespace curvefs {
namespace tool {

class CurvefsTool {
 public:
    explicit CurvefsTool(std::shared_ptr<std::string> command,
                         std::shared_ptr<std::string> programe)
        : command_(command), programe_(programe) {}
    virtual ~CurvefsTool() {}
    virtual void PrintHelp() = 0;
    /**
     * @brief configure the environment for the command
     *
     * @details
     */
    virtual void PreConfigure() = 0;
    virtual void RunCommand() = 0;
    /**
     * @brief output the result of command
     *
     * @details
     */
    virtual void PrintOutput() {
        std::cout << output_ << std::endl;
    }

    virtual int Run() {
        PreConfigure();
        RunCommand();
        PrintOutput();
        return ret_;
    }

 protected:
    std::shared_ptr<std::string> command_;
    std::shared_ptr<std::string> programe_;
    std::string output_;
    int ret_;
};

}  // namespace tool
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
