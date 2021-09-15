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

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_FACTORY_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_FACTORY_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/version/version_tool.h"

namespace curvefs {
namespace tool {

// this template function and RegisterCurvefsTool to add commands
template <class CurvefsTool_t>
std::shared_ptr<CurvefsTool_t> CurvefsToolCreatorTmpl(
    std::shared_ptr<std::string> command,
    std::shared_ptr<std::string> programe) {
    return std::make_shared<CurvefsTool_t>(command, programe);
}

class CurvefsToolFactory {
 public:
    explicit CurvefsToolFactory(std::shared_ptr<std::string> programe);
    virtual ~CurvefsToolFactory() {}

    std::shared_ptr<CurvefsTool> GenerateCurvefsTool(
        const std::string& command);

    /**
     * @brief add commands and function to generate objects
     *
     * @param command
     * @param function
     * @details
     * The same command will only take effect for the first one registered
     */
    virtual void RegisterCurvefsTool(
        const std::string& command,
        const std::function<std::shared_ptr<CurvefsTool>(
            std::shared_ptr<std::string>, std::shared_ptr<std::string>)>&
            function);

 private:
    // storage commands and function to generate objects
    std::unordered_map<std::string, std::function<std::shared_ptr<CurvefsTool>(
                                        std::shared_ptr<std::string>,
                                        std::shared_ptr<std::string>)>>
        command2creator_;
    std::shared_ptr<std::string> programe_;
};

}  // namespace tool
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_FACTORY_H_
