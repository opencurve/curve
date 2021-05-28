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

#ifndef CURVEFS_SRC_CLIENT_DIR_BUFFER_H_
#define CURVEFS_SRC_CLIENT_DIR_BUFFER_H_

#include <cstdint>
#include <unordered_map>


namespace curvefs {
namespace client {

struct DirBufferHead {
    bool wasRead;
    size_t size;
    char *p;
};

class DirBuffer {
 public:
    DirBuffer() {}
    uint32_t DirBufferNew();
    DirBufferHead* DirBufferGet(uint32_t dindex);
    void DirBufferRelease(uint32_t dindex);
    void DirBufferFreeAll();

 private:
    std::unordered_map<uint32_t, DirBufferHead*> buffer_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DIR_BUFFER_H_
