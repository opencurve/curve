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
 * File Created: Tuesday, 29th January 2019 11:44:59 am
 * Author: tongguangxun
 */
#ifndef TEST_CHUNKSERVER_DATASTORE_FILEPOOL_HELPER_H_
#define TEST_CHUNKSERVER_DATASTORE_FILEPOOL_HELPER_H_

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <climits>
#include <memory>
#include <string>

#include "src/fs/local_filesystem.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

void allocateChunk(std::shared_ptr<LocalFileSystem> fsptr,
                   uint32_t num,
                   std::string poolDir,
                   uint32_t chunkSize);

#endif  // TEST_CHUNKSERVER_DATASTORE_FILEPOOL_HELPER_H_
