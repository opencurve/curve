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
 * Created Date: 2020-01-19
 * Author: charisu
 */

#ifndef NEBD_SRC_PART2_METAFILE_MANAGER_H_
#define NEBD_SRC_PART2_METAFILE_MANAGER_H_

#include <json/json.h>

#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>

#include "nebd/src/common/crc32.h"
#include "nebd/src/common/posix_wrapper.h"
#include "nebd/src/common/rw_lock.h"
#include "nebd/src/part2/define.h"
#include "nebd/src/part2/util.h"

namespace nebd {
namespace server {

using nebd::common::PosixWrapper;
using nebd::common::ReadLockGuard;
using nebd::common::RWLock;
using nebd::common::WriteLockGuard;
using FileMetaMap = std::unordered_map<std::string, NebdFileMeta>;

const char kVolumes[] = "volumes";
const char kFileName[] = "filename";
const char kFd[] = "fd";
const char kCRC[] = "crc";

class NebdMetaFileParser {
 public:
    int Parse(Json::Value root, FileMetaMap* fileMetas);
    Json::Value ConvertFileMetasToJson(const FileMetaMap& fileMetas);
};

struct NebdMetaFileManagerOption {
    std::string metaFilePath = "";
    std::shared_ptr<PosixWrapper> wrapper = std::make_shared<PosixWrapper>();
    std::shared_ptr<NebdMetaFileParser> parser =
        std::make_shared<NebdMetaFileParser>();
};

class NebdMetaFileManager {
 public:
    NebdMetaFileManager();
    virtual ~NebdMetaFileManager();

    //  Initialization, mainly reading metadata information from files and
    //  loading it into memory
    virtual int Init(const NebdMetaFileManagerOption& option);

    // List file records
    virtual int ListFileMeta(std::vector<NebdFileMeta>* fileMetas);

    // Update file metadata
    virtual int UpdateFileMeta(const std::string& fileName,
                               const NebdFileMeta& fileMeta);

    // Delete file metadata
    virtual int RemoveFileMeta(const std::string& fileName);

 private:
    // Atomic writing file
    int AtomicWriteFile(const Json::Value& root);
    // Update metadata files and update memory cache
    int UpdateMetaFile(const FileMetaMap& fileMetas);
    // Initialize reading from persistent files to memory
    int LoadFileMeta();

 private:
    // Meta Data File Path
    std::string metaFilePath_;
    // File system operation encapsulation
    std::shared_ptr<common::PosixWrapper> wrapper_;
    // Metadata for parsing Json format
    std::shared_ptr<NebdMetaFileParser> parser_;
    // MetaFileManager thread safe read write lock
    RWLock rwLock_;
    // Meta file memory cache
    FileMetaMap metaCache_;
};
using MetaFileManagerPtr = std::shared_ptr<NebdMetaFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_METAFILE_MANAGER_H_
