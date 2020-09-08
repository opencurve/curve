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
    // Open就使用sync标志是为了避免集中在close一次性sync，对于16MB的chunk文件可能会造成抖动
    oflag |= O_SYNC;

    // 先判断当前文件是否需要过滤，如果需要过滤，就直接走下面逻辑，不走chunkfilepool
    // 如果open操作携带create标志，则从chunkfilepool取，否则保持原来语意
    // 如果待打开的文件已经存在，则直接使用原有语意
    if (!NeedFilter(path) &&
        (oflag & O_CREAT) &&
        false == lfs_->FileExists(path)) {
        // 从chunkfile pool中取出chunk返回
        int rc = chunkFilePool_->GetFile(path, tempMetaPageContent);
        // 如果从FilePool中取失败，返回错误。
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
    // 1. 如果是目录且recursive=true，那么遍历目录内容回收
    // 2. 如果是目录且recursive=false，那么判断目录内容是否为空，不为空返回false
    // 3. 如果是文件直接回收
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
             // 如果在过滤名单里，就直接删除
             if (NeedFilter(path)) {
                 return lfs_->Delete(path) == 0;
             } else {
                // chunkfilepool内部会检查path对应文件合法性，如果不符合就直接删除
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
            // 如果在过滤名单里，就直接删除
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
        // chunkfilepool内部会检查path对应文件合法性，如果不符合就直接删除
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
