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
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_DATASTORE_MOCK_FILE_POOL_H_
#define TEST_CHUNKSERVER_DATASTORE_MOCK_FILE_POOL_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/chunkserver/datastore/file_pool.h"

namespace curve {
namespace chunkserver {

class MockFilePool : public FilePool {
 public:
    explicit MockFilePool(std::shared_ptr<LocalFileSystem> lfs)
        : FilePool(lfs) {}
    ~MockFilePool() {}
    MOCK_METHOD1(Initialize, bool(FilePoolOptions));
    MOCK_METHOD2(GetFileImpl, int(const std::string&, char*));
    MOCK_METHOD1(RecycleFile, int(const std::string&  chunkpath));
    MOCK_METHOD0(UnInitialize, void());
    MOCK_METHOD0(Size, size_t());
    MOCK_METHOD0(GetFilePoolOpt, FilePoolOptions());

    int GetFile(const std::string& chunkpath,
                char* metapage,
                bool needClean = false) override {
        return GetFileImpl(chunkpath, metapage);
    };
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_DATASTORE_MOCK_FILE_POOL_H_
