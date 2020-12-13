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
 * Created Date: Thursday August 29th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/datastore_file_helper.h"

namespace curve {
namespace chunkserver {

int DatastoreFileHelper::ListFiles(const string& baseDir,
                                   vector<string>* chunkFiles,
                                   vector<string>* snapFiles) {
    vector<string> files;
    int rc = fs_->List(baseDir, &files);
    if (rc < 0 && rc != -ENOENT) {
        LOG(ERROR) << "List " << baseDir << " failed.";
        return -1;
    }

    for (auto &file : files) {
        FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(file);
        if (info.type == FileNameOperator::FileType::CHUNK) {
            // If chunkFiles is nullptr, the chunk file name is not returned
            if (chunkFiles != nullptr) {
                chunkFiles->emplace_back(file);
            }
        } else if (info.type == FileNameOperator::FileType::SNAPSHOT) {
            // If snapFiles is nullptr, the snapshot file name is not returned
            if (snapFiles != nullptr) {
                snapFiles->emplace_back(file);
            }
        } else {
            LOG(WARNING) << "Unknown file: " << file;
        }
    }
    return 0;
}

bool DatastoreFileHelper::IsSnapshotFile(const string& fileName) {
    FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(fileName);
    return info.type == FileNameOperator::FileType::SNAPSHOT;
}

bool DatastoreFileHelper::IsChunkFile(const string& fileName) {
    FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(fileName);
    return info.type == FileNameOperator::FileType::CHUNK;
}

}  // namespace chunkserver
}  // namespace curve
