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
#include <list>

#include "src/chunkserver/filesystem_adaptor/curve_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {
CurveFilesystemAdaptor::CurveFilesystemAdaptor(
                                std::shared_ptr<FilePool> filePool,
                                std::shared_ptr<LocalFileSystem> lfs) {
    lfs_ = lfs;
    filePool_ = filePool;
    uint64_t metapageSize = filePool->GetFilePoolOpt().metaPageSize;
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
    LOG(INFO) << "release CurveFilesystemAdaptor!";
}

braft::FileAdaptor* CurveFilesystemAdaptor::open(const std::string& path,
                    int oflag, const ::google::protobuf::Message* file_meta,
                    butil::File::Error* e) {
    (void) file_meta;

    bool cloexec = (oflag & O_CLOEXEC);
    if (cloexec) {
        oflag &= (~O_CLOEXEC);
        LOG(WARNING) << "CurveFilesystemAdaptor not support O_CLOEXEC!";
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
        int rc = filePool_->GetFile(path, tempMetaPageContent);
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
            LOG(ERROR) << "create chunkfile failed, filename = "
                << path.c_str() << ", errno = " << errno;
        } else {
            LOG(WARNING) << "open chunkfile failed,"
                    << "may be deleted by user, filename = "
                    << path.c_str() << ",errno = " << errno;
        }
        return NULL;
    }

    return new CurveFileAdaptor(fd, lfs_);
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
                return filePool_->RecycleFile(path) == 0;
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
                int ret = filePool_->RecycleFile(todeletePath);
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
        filePool_->RecycleFile(new_path);
    }
    return lfs_->Rename(old_path, new_path) == 0;
}

bool CurveFilesystemAdaptor::link(const std::string& old_path,
          const std::string& new_path) {
    return lfs_->Link(old_path, new_path) == 0;
}

bool CurveFilesystemAdaptor::CreateDirectoryAndGetError(
    const FilePath& full_path,
    File::Error* error,
    bool create_parents) {
  if (!create_parents) {
    if (lfs_->DirExists(full_path.value())) {
      return true;
    }
    if (lfs_->Mkdir(full_path.value(), false) == 0) {
      return true;
    }
    const int saved_errno = errno;
    if (lfs_->DirExists(full_path.value())) {
      return true;
    }
    if (error) {
      *error = File::OSErrorToFileError(saved_errno);
    }
    return false;
  }
  std::vector<FilePath> subpaths;

  // Collect a list of all parent directories.
  FilePath last_path = full_path;
  subpaths.push_back(full_path);
  for (FilePath path = full_path.DirName();
       path.value() != last_path.value(); path = path.DirName()) {
    subpaths.push_back(path);
    last_path = path;
  }

  // Iterate through the parents and create the missing ones.
  for (std::vector<FilePath>::reverse_iterator i = subpaths.rbegin();
       i != subpaths.rend(); ++i) {
    if (lfs_->DirExists(i->value()))
      continue;
    // NOTE(gejun): permission bits of dir are different from file's
    // -The write bit allows the affected user to create, rename, or delete
    //  files within the directory, and modify the directory's attributes
    // -The read bit allows the affected user to list the files within the
    //  directory
    // -The execute bit allows the affected user to enter the directory, and
    //  access files and directories inside
    // -The sticky bit states that files and directories within that directory
    //  may only be deleted or renamed by their owner (or root)
    if (lfs_->Mkdir(i->value(), false) == 0)
      continue;
    // Mkdir failed, but it might have failed with EEXIST, or some other error
    // due to the the directory appearing out of thin air. This can occur if
    // two processes are trying to create the same file system tree at the same
    // time. Check to see if it exists and make sure it is a directory.
    int saved_errno = errno;
    if (!lfs_->DirExists(i->value())) {
      if (error)
        *error = File::OSErrorToFileError(saved_errno);
      return false;
    }
  }
  return true;
}

bool CurveFilesystemAdaptor::create_directory(const std::string& path,
                              butil::File::Error* error,
                              bool create_parent_directories) {
    FilePath dir(path);
    return CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool CurveFilesystemAdaptor::path_exists(const std::string& path) {
    return lfs_->PathExists(path);
}

bool CurveFilesystemAdaptor::directory_exists(const std::string& path) {
    return lfs_->DirExists(path);
}

braft::DirReader* CurveFilesystemAdaptor::directory_reader(
    const std::string& path) {
    return new CurveDirReader(path.c_str(), lfs_);
}

void CurveFilesystemAdaptor::SetFilterList(
                                    const std::list<std::string>& filter) {
    filterList_ = filter;
}

void CurveFilesystemAdaptor::AddToFilterList(std::list<std::string> *files) {
    filterList_.splice(filterList_.end(), *files);
}

void CurveFilesystemAdaptor::AddToFilterList(const std::string& file) {
    filterList_.push_back(file);
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

CurveDirReader::CurveDirReader(const std::string& path,
    const std::shared_ptr<LocalFileSystem> &lfs)
    : path_(path), lfs_(lfs), dir_(nullptr), dirIter_(nullptr) {
    dir_ = lfs_->OpenDir(path);
}

CurveDirReader::~CurveDirReader() {
    if (dir_ != nullptr) {
        lfs_->CloseDir(dir_);
    }
    dir_ = nullptr;
}

bool CurveDirReader::is_valid() const {
    return dir_ != nullptr;
}

bool CurveDirReader::next() {
    while ((dirIter_ = lfs_->ReadDir(dir_)) != nullptr) {
        if (strcmp(dirIter_->d_name, ".") == 0
                || strcmp(dirIter_->d_name, "..") == 0) {
            continue;
        } else {
            return true;
        }
    }
    return false;
}

const char* CurveDirReader::name() const {
    if (nullptr == dirIter_) {
        return nullptr;
    }
    return dirIter_->d_name;
}


}  // namespace chunkserver
}  // namespace curve
