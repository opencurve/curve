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
 * Created Date: 2020-06-17
 * Author: charisu
 */

#ifndef TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_
#define TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class MockFileReader : public CurveSnapshotFileReader {
 public:
    MockFileReader(braft::FileSystemAdaptor* fs,
                   braft::SnapshotThrottle* throttle) :
                        CurveSnapshotFileReader(fs, "", throttle) {}
    virtual ~MockFileReader() {}
    MOCK_CONST_METHOD7(read_file, int(butil::IOBuf*, const std::string&,
                          off_t, size_t, bool, size_t*, bool*));
    MOCK_CONST_METHOD0(path, const std::string&());
};

}  // namespace chunkserver
}  // namespace curve

#endif  //  TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_
