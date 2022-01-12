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
 * Project: nebd
 * Created Date: 2020-03-23
 * Author: charsiu
 */

#include "nebd/src/common/nebd_version.h"

#include "nebd/src/common/stringstatus.h"
#include "src/common/macros.h"

namespace nebd {
namespace common {

// https://gcc.gnu.org/onlinedocs/gcc-4.8.5/cpp/Stringification.html
std::string NebdVersion() {
    static const std::string version =
#ifdef CURVEVERSION
        std::string(STRINGIFY(CURVEVERSION));
#else
        std::string("unknown");
#endif
    return version;
}

const char kNebdMetricPrefix[] = "nebd";
const char kVersion[] = "version";

void ExposeNebdVersion() {
    static StringStatus version;
    version.ExposeAs(kNebdMetricPrefix, kVersion);
    version.Set(kVersion, NebdVersion());
    version.Update();
}

}  // namespace common
}  // namespace nebd
