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
 * Created Date: 2021-09-24
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_ABSTRACT_CREATOR_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_ABSTRACT_CREATOR_H_

#include <memory>
#include <string>

#include "curvefs/src/tools/curvefs_tool.h"

namespace curvefs {
namespace tools {

template <class CurvefsToolT>
class CurvefsToolCreator {
 public:
    static std::shared_ptr<CurvefsToolT> Create() {
        return std::make_shared<CurvefsToolT>();
    }
};

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_ABSTRACT_CREATOR_H_
