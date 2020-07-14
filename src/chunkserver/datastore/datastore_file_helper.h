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
        // 默认使用 Ext4FileSystemImpl
        fs_ = Ext4FileSystemImpl::getInstance();
    }

    explicit DatastoreFileHelper(std::shared_ptr<LocalFileSystem> fs)
        : fs_(fs) {}

    virtual ~DatastoreFileHelper() {}

    /**
     * 用于获取指定copyset目录下的chunk文件名和快照文件名
     * @param baseDir: copyset的数据文件所在目录
     * @param chunkFiles[out]: 返回目录下的所有chunk文件名；可以为nullptr
     * @param snapFiles[out]: 返回目录下的所有快照文件名；可以为nullptr
     * @return：成功返回0，失败返回-1
     */
    int ListFiles(const string& baseDir,
                  vector<string>* chunkFiles,
                  vector<string>* snapFiles);

    /**
     * 判断文件是否为chunk的snapshot文件
     * @param fileName: 文件名
     * @return true-是snapshot文件，false-不是snapshot文件
     */
    static bool IsSnapshotFile(const string& fileName);

    /**
     * 判断文件是否为chunk文件
     * @param fileName: 文件名
     * @return true-是chunk文件，false-不是chunk文件
     */
    static bool IsChunkFile(const string& fileName);

 private:
    std::shared_ptr<LocalFileSystem> fs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DATASTORE_FILE_HELPER_H_
