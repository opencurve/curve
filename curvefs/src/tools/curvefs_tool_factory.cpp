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

#include "curvefs/src/tools/curvefs_tool_factory.h"

namespace curvefs {
namespace tool {

CurvefsToolFactory::CurvefsToolFactory(std::shared_ptr<std::string> programe)
    : programe_(programe) {
    // version
    RegisterCurvefsTool(std::string(kVersionCmd),
                        CurvefsToolCreatorTmpl<VersionTool>);
}

std::shared_ptr<CurvefsTool> CurvefsToolFactory::GenerateCurvefsTool(
    const std::string& command) {
    auto search = command2creator_.find(command);
    if (search != command2creator_.end()) {
        return search->second(std::make_shared<std::string>(command),
                              programe_);
    }
    return nullptr;
}

void CurvefsToolFactory::RegisterCurvefsTool(
    const std::string& command,
    const std::function<std::shared_ptr<CurvefsTool>(
        std::shared_ptr<std::string>, std::shared_ptr<std::string>)>&
        function) {
    command2creator_.insert({command, function});
}

}  // namespace tool
}  // namespace curvefs
