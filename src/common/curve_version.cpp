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
 * File Created: 2019-12-10
 * Author: wuhanqing
 */

#include "src/common/curve_version.h"

#include <bvar/bvar.h>

#include "src/common/macros.h"

namespace curve {
namespace common {

std::string CurveVersion() {
    static const std::string version =
#ifdef CURVEVERSION
        std::string(STRINGIFY(CURVEVERSION));
#else
        std::string("unknown");
#endif
    return version;
}

void ExposeCurveVersion() {
    static bvar::Status<std::string> version;
    version.expose_as("curve", "version");
    version.set_value(CurveVersion());
}

}  // namespace common
}  // namespace curve
