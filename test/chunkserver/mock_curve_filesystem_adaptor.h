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
 * Created Date: Tuesday December 24th 2019
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_MOCK_CURVE_FILESYSTEM_ADAPTOR_H_
#define TEST_CHUNKSERVER_MOCK_CURVE_FILESYSTEM_ADAPTOR_H_

#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/chunkserver/filesystem_adaptor/curve_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {

class MockCurveFilesystemAdaptor : public CurveFilesystemAdaptor {
 public:
    MockCurveFilesystemAdaptor() {}
    ~MockCurveFilesystemAdaptor() {}
    MOCK_METHOD4(open, braft::FileAdaptor*(const std::string&,
                                           int,
                                           const ::google::protobuf::Message*,
                                           butil::File::Error*));
    MOCK_METHOD2(delete_file, bool(const std::string&, bool));
    MOCK_METHOD2(rename, bool(const std::string&, const std::string&));
    MOCK_METHOD2(link, bool(const std::string& old_path,
              const std::string& new_path));
    MOCK_METHOD3(create_directory, bool(const std::string& path,
                                  butil::File::Error* error,
                                  bool create_parent_directories));
    MOCK_METHOD1(path_exists, bool(const std::string& path));
    MOCK_METHOD1(directory_exists, bool(const std::string& path));
    MOCK_METHOD1(directory_reader, braft::DirReader*(const std::string& path));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_CURVE_FILESYSTEM_ADAPTOR_H_
