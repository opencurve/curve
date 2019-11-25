/*
 * Project: curve
 * Created Date: Thursday August 29th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
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
            // 如果chunkFiles为nullptr，则不返回chunk文件名
            if (chunkFiles != nullptr) {
                chunkFiles->emplace_back(file);
            }
        } else if (info.type == FileNameOperator::FileType::SNAPSHOT) {
            // 如果snapFiles为nullptr，则不返回快照文件名
            if (snapFiles != nullptr) {
                snapFiles->emplace_back(file);
            }
        } else {
            LOG(WARNING) << "Unknown file: " << file;
        }
    }
    return 0;
}

}  // namespace chunkserver
}  // namespace curve
