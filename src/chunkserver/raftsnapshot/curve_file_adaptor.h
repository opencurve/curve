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
 * Created Date: 2020-06-10
 * Author: charisu
 */

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_

#include <braft/file_system_adaptor.h>

namespace curve {
namespace chunkserver {

class CurveFileAdaptor : public braft::PosixFileAdaptor {
 public:
    explicit CurveFileAdaptor(int fd) : PosixFileAdaptor(fd) {}
    // sync must come before close to make sure data on disk, no change to other
    // logic
    bool close() override {
        return sync() && braft::PosixFileAdaptor::close();
    }
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_
