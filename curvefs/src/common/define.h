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
 * @Project: curve
 * @Date: 2021-06-23 21:05:29
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_COMMON_DEFINE_H_
#define CURVEFS_SRC_COMMON_DEFINE_H_
#include <cstdint>

namespace curvefs {
const uint64_t ROOTINODEID = 1;

const char XATTRFILES[] = "curve.dir.files";
const char XATTRSUBDIRS[] = "curve.dir.subdirs";
const char XATTRENTRIES[] = "curve.dir.entries";
const char XATTRFBYTES[] = "curve.dir.fbytes";
const char XATTRRFILES[] = "curve.dir.rfiles";
const char XATTRRSUBDIRS[] = "curve.dir.rsubdirs";
const char XATTRRENTRIES[] = "curve.dir.rentries";
const char XATTRRFBYTES[] = "curve.dir.rfbytes";
const char SUMMARYPREFIX[] = "curve.dir";
}  // namespace curvefs
#endif  // CURVEFS_SRC_COMMON_DEFINE_H_
