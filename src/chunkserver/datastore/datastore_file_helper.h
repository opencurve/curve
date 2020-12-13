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

#ifndef SRC_CHUNKSERVER_DATASTORE_DATASTORE_FILE_HELPER_H_
#define SRC_CHUNKSERVER_DATASTORE_DATASTORE_FILE_HELPER_H_

#include <glog/logging.h>
#include <vector>
#include <string>
#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/fs/ext4_filesystem_impl.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::Ext4FileSystemImpl;

class DatastoreFileHelper {
 public:
    DatastoreFileHelper() {
        // Use Ext4FileSystemImpl by default
        fs_ = Ext4FileSystemImpl::getInstance();
    }

    explicit DatastoreFileHelper(std::shared_ptr<LocalFileSystem> fs)
        : fs_(fs) {}

    virtual ~DatastoreFileHelper() {}

    /**
     * Used to get the chunk file name and snapshot file name in the specified
     * copyset directory
     * @param baseDir: The directory where the data file of the copyset is
     *                 located
     * @param chunkFiles[out]: Returns the names of all chunk files in the
     *                         directory; it can be nullptr
     * @param snapFiles[out]: Returns the names of all snapshot files in the
     *                        directory; it can be nullptr
     * @return: return 0 on success, return -1 on failure
     */
    int ListFiles(const string& baseDir,
                  vector<string>* chunkFiles,
                  vector<string>* snapFiles);

    /**
     * Determine whether the file is a snapshot file of a chunk
     * @param fileName: file name
     * @return true-is a snapshot file, false-not a snapshot file
     */
    static bool IsSnapshotFile(const string& fileName);

    /**
     * Determine whether the file is a chunk file
     * @param fileName: file name
     * @return true-is a chunk file, false-not a chunk file
     */
    static bool IsChunkFile(const string& fileName);

 private:
    std::shared_ptr<LocalFileSystem> fs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DATASTORE_FILE_HELPER_H_
