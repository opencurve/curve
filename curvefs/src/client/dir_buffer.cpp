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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/dir_buffer.h"

namespace curvefs {
namespace client {


uint32_t DirBuffer::DirBufferNew() {
    return 0;
}

DirBufferHead* DirBuffer::DirBufferGet(uint32_t dindex) {
    return new DirBufferHead();
}

void DirBuffer::DirBufferRelease(uint32_t dindex) {}

void DirBuffer::DirBufferFreeAll() {}






}  // namespace client
}  // namespace curvefs
