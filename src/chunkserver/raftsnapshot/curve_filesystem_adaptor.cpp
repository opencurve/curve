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
 * File Created: Monday, 10th June 2019 2:20:23 pm
 * Author: tongguangxun
 */

#include <butil/fd_utility.h>
#include <vector>

#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {
CurveFilesystemAdaptor::CurveFilesystemAdaptor(
                                std::shared_ptr<FilePool> chunkFilePool,
                                std::shared_ptr<LocalFileSystem> lfs) {
    lfs_ = lfs;
    chunkFilePool_ = chunkFilePool;
    uint64_t metapageSize = chunkFilePool->GetFilePoolOpt().metaPageSize;
    tempMetaPageContent = new (std::nothrow) char[metapageSize];
    CHECK(tempMetaPageContent != nullptr);
    memset(tempMetaPageContent, 0, metapageSize);
}

CurveFilesystemAdaptor::CurveFilesystemAdaptor()
    : tempMetaPageContent(nullptr) {
}

CurveFilesystemAdaptor::~CurveFilesystemAdaptor() {
    if (tempMetaPageContent != nullptr) {
        delete[] tempMetaPageContent;
        tempMetaPageContent = nullptr;
    }
    LOG(INFO) << "release raftsnapshot filesystem adaptor!";
}

braft::FileAdaptor* CurveFilesystemAdaptor::open(const std::string& path,
                    int oflag, const ::google::protobuf::Message* file_meta,
                    butil::File::Error* e) {
    (void) file_meta;

    static std::once_flag local_s_check_cloexec_once;
    static bool local_s_support_cloexec_on_open = false;
    std::call_once(local_s_check_cloexec_once, [&](){
        int fd = lfs_->Open("/dev/zero", O_RDONLY | O_CLOEXEC);
        local_s_support_cloexec_on_open = (fd != -1);
        if (fd != -1) {
            lfs_->Close(fd);
        }
    });

    bool cloexec = (oflag & O_CLOEXEC);
    if (cloexec && !local_s_support_cloexec_on_open) {
        oflag &= (~O_CLOEXEC);
    }
    // The sync flag is used on open to avoid a one-time，simultaneous sync on close operation,
    // which may cause jitter for 16MB chunk files
    oflag |= O_SYNC;

    // Check whether the current file needs to be filtered,
    // if so, ignore chunkfilepool and go directly to the following logic.
    // If the open operation carries the create flag, then fetch from the chunkfilepool,
    // otherwise maintain the original semantics.
    // If the file to be opened already exists, use the original semantics directly.
    if (!NeedFilter(path) &&
        (oflag & O_CREAT) &&
        false == lfs_->FileExists(path)) {
        // fetch chunk from chunkfilepool, return
        int rc = chunkFilePool_->GetFile(path, tempMetaPageContent);
        // if fail to fetch from Filepool, return error
        if (rc != 0) {
            LOG(ERROR) << "get chunk from chunkfile pool failed!";
            return NULL;
        } else {
            oflag &= (~O_CREAT);
            oflag &= (~O_TRUNC);
        }
    }

    int fd = lfs_->Open(path.c_str(), oflag);
    if (e) {
        *e = (fd < 0) ? butil::File::OSErrorToFileError(errno)
                        : butil::File::FILE_OK;
    }

    if (fd < 0) {
        if (oflag & O_CREAT) {
            LOG(ERROR) << "snapshot create chunkfile failed, filename = "
                << path.c_str() << ", errno = " << errno;
        } else {
            LOG(WARNING) << "snapshot open chunkfile failed,"
                    << "may be deleted by user, filename = "
                    << path.c_str() << ",errno = " << errno;
        }
        return NULL;
    }
    if (cloexec && !local_s_support_cloexec_on_open) {
        butil::make_close_on_exec(fd);
    }

    return new CurveFileAdaptor(fd);
}

bool CurveFilesystemAdaptor::delete_file(const std::string& path,
                                                bool recursive) {
    // 1. If it is a directory and recursive=true,
    //    iterate the contents of the directory to recycle
    // 2. If it is a directory and recursive=false，
    //    check whether the contents of the directory is empty，if not so return false
    // 3. If it is a file, recycle directly
    if (lfs_->DirExists(path)) {
        std::vector<std::string> dircontent;
        lfs_->List(path, &dircontent);
        if (!dircontent.empty() && !recursive) {
            LOG(ERROR) << "delete none empty directory!";
            return false;
        } else {
            return RecycleDirRecursive(path);
        }
    } else {
        if (lfs_->FileExists(path)) {
             // If it is in the filter list, delete it
             if (NeedFilter(path)) {
                 return lfs_->Delete(path) == 0;
             } else {
                // The chunkfilepool will internally check the legality of the file corresponding to path
                // and delete it directly if it is not legal
                return chunkFilePool_->RecycleFile(path) == 0;
             }
        }
    }
    return true;
}

bool CurveFilesystemAdaptor::RecycleDirRecursive(
    const std::string& path) {
    std::vector<std::string> dircontent;
    lfs_->List(path, &dircontent);
    bool rc = true;
    for (auto filepath : dircontent) {
        std::string todeletePath = path + "/" + filepath;
        if (lfs_->DirExists(todeletePath)) {
            RecycleDirRecursive(todeletePath);
        } else {
            // If it is in the filter list, delete it
            if (NeedFilter(todeletePath)) {
                if (lfs_->Delete(todeletePath) != 0) {
                    LOG(ERROR) << "delete " << todeletePath << ", failed!";
                    rc = false;
                    break;
                }
            } else {
                int ret = chunkFilePool_->RecycleFile(todeletePath);
                if (ret < 0) {
                    rc = false;
                    LOG(ERROR) << "recycle " << path + filepath << ", failed!";
                    break;
                }
            }
        }
    }
    return rc && lfs_->Delete(path) == 0;
}

bool CurveFilesystemAdaptor::rename(const std::string& old_path,
                                            const std::string& new_path) {
    if (!NeedFilter(new_path) && lfs_->FileExists(new_path)) {
        // The chunkfilepool will internally check the legality of the file corresponding to path
        // and delete it directly if it is not legal
        chunkFilePool_->RecycleFile(new_path);
    }
    return lfs_->Rename(old_path, new_path) == 0;
}

void CurveFilesystemAdaptor::SetFilterList(
                                    const std::vector<std::string>& filter) {
    filterList_.assign(filter.begin(), filter.end());
}

bool CurveFilesystemAdaptor::NeedFilter(const std::string& filename) {
    bool ret = false;
    for (auto name : filterList_) {
        if (filename.find(name) != filename.npos) {
            ret = true;
            LOG(INFO) << "file " << filename.c_str() << ", need filter!";
            break;
        }
    }
    return ret;
}

}  // namespace chunkserver
}  // namespace curve
