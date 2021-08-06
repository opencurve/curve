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
 * Created Date: Mon Apr 27th 2019
 * Author: lixiaocui
 */

#include <time.h>
#include <glog/logging.h>
#include <vector>
#include "src/chunkserver/trash.h"
#include "src/common/string_util.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/copyset_node.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/uri_parser.h"
#include "src/chunkserver/raftlog/define.h"

using ::curve::chunkserver::RAFT_DATA_DIR;
using ::curve::chunkserver::RAFT_META_DIR;
using ::curve::chunkserver::RAFT_SNAP_DIR;
using ::curve::chunkserver::RAFT_LOG_DIR;

namespace curve {
namespace chunkserver {
int Trash::Init(TrashOptions options) {
    isStop_ = true;

    if (curve::common::UriParser::ParseUri(options.trashPath, &trashPath_)
            .empty()) {
        LOG(ERROR) << "not support trash uri's protocol"
                   << " error trashPath is: " << options.trashPath;
        return -1;
    }

    if (trashPath_.empty()) {
        LOG(ERROR) << "trash path is empty, please check!";
        return -1;
    }

    expiredAfterSec_ = options.expiredAfterSec;
    scanPeriodSec_ = options.scanPeriodSec;
    localFileSystem_ = options.localFileSystem;
    chunkFilePool_ = options.chunkFilePool;
    walPool_ = options.walPool;
    chunkNum_.store(0);

     // 读取trash目录下的所有目录
    std::vector<std::string> files;
    localFileSystem_->List(trashPath_, &files);

    // 遍历trash下的文件
    for (auto &file : files) {
        // 如果不是copyset目录，跳过
        if (!IsCopysetInTrash(file)) {
            continue;
        }
        std::string copysetDir = trashPath_ + "/" + file;
        uint32_t chunkNum = CountChunkNumInCopyset(copysetDir);
        chunkNum_.fetch_add(chunkNum);
    }
    LOG(INFO) << "Init trash success. "
              << "Current num of chunks in trash: " << chunkNum_.load();
    return 0;
}

int Trash::Run() {
    if (isStop_.exchange(false)) {
        recycleThread_ =
            Thread(&Trash::DeleteEligibleFileInTrashInterval, this);
        LOG(INFO) << "Start trash thread ok.";
        return 0;
    }

    return -1;
}

int Trash::Fini() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop Trash...";
        sleeper_.interrupt();
        recycleThread_.join();
    }
    LOG(INFO) << "stop trash ok.";
    return 0;
}

int Trash::RecycleCopySet(const std::string &dirPath) {
    // 回收站目录不存在，需要创建
    if (!localFileSystem_->DirExists(trashPath_)) {
        LOG(INFO) << "Copyset recyler directory " << trashPath_
                  << " does not exist, creating it";

        if (0 != localFileSystem_->Mkdir(trashPath_)) {
            LOG(ERROR) << "Failed to create copyset recyler directory: "
                       << trashPath_;
            return -1;
        }
    }

    // 如果回收站已存在该目录，本次删除失败
    std::string dst = trashPath_ + "/" +
        dirPath.substr(dirPath.find_last_of('/', dirPath.length()) + 1) +
        '.' + std::to_string(std::time(nullptr));
    if (localFileSystem_->DirExists(dst)) {
        LOG(WARNING) << "recycle error: " << dst << " already exist in "
                     << trashPath_;
        return -1;
    }
    {
        LockGuard lg(mtx_);
        if (0 != localFileSystem_->Rename(dirPath, dst)) {
            LOG(ERROR) << "rename " << dirPath << " to " << dst << " error";
            return -1;
        }
        uint32_t chunkNum = CountChunkNumInCopyset(dst);
        chunkNum_.fetch_add(chunkNum);
    }
    LOG(INFO) << "Recycle copyset success. Copyset path: " << dst
              << ", current num of chunks in trash: " << chunkNum_.load();
    return 0;
}

void Trash::DeleteEligibleFileInTrashInterval() {
     while (sleeper_.wait_for(std::chrono::seconds(scanPeriodSec_))) {
        // 扫描回收站
         DeleteEligibleFileInTrash();
     }
}

void Trash::DeleteEligibleFileInTrash() {
    // trash目录暂不存在
    if (!localFileSystem_->DirExists(trashPath_)) {
        return;
    }

    // 读取trash目录下的所有目录
    std::vector<std::string> files;
    if (0 != localFileSystem_->List(trashPath_, &files)) {
        LOG(ERROR) << "Trash failed list files in " << trashPath_;
        return;
    }

    // 遍历trash下的文件
    for (auto &file : files) {
        // 如果不是copyset目录，跳过
        if (!IsCopysetInTrash(file)) {
            continue;
        }

        std::string copysetDir = trashPath_ + "/" + file;
        if (!NeedDelete(copysetDir)) {
            continue;
        }

        if (!RecycleChunksAndWALInDir(copysetDir, file)) {
            continue;
        }

        // 删除copyset目录
        if (0 != localFileSystem_->Delete(copysetDir)) {
            LOG(ERROR) << "Trash fail to delete " << copysetDir;
            return;
        }
    }
}

bool Trash::IsCopysetInTrash(const std::string &dirName) {
    // 合法的copyset目录: 高32位PoolId(>0)组成， 低32位由copysetId(>0)组成
    // 目录是十进制形式
    // 例如：2860448220024 (poolId: 666, copysetId: 888)
    uint64_t groupId;
    int n = dirName.find(".");
    if (n == std::string::npos) {
        return false;
    }

    if (!::curve::common::StringToUll(dirName.substr(0, n), &groupId)) {
        return false;
    }
    return GetPoolID(groupId) >= 1 && GetCopysetID(groupId) >= 1;
}

bool Trash::NeedDelete(const std::string &copysetDir) {
    int fd = localFileSystem_->Open(copysetDir, O_RDONLY);
    if (0 > fd) {
        LOG(ERROR) << "Trash fail open " << copysetDir;
        return false;
    }

    struct stat info;
    if (0 != localFileSystem_->Fstat(fd, &info)) {
        localFileSystem_->Close(fd);
        return false;
    }

    time_t now;
    time(&now);
    if (difftime(now, info.st_ctime) < expiredAfterSec_) {
        localFileSystem_->Close(fd);
        return false;
    }
    localFileSystem_->Close(fd);
    return true;
}

bool Trash::IsChunkOrSnapShotFile(const std::string &chunkName) {
    return FileNameOperator::FileType::UNKNOWN !=
        FileNameOperator::ParseFileName(chunkName).type;
}

bool Trash::RecycleChunksAndWALInDir(
    const std::string &copysetPath, const std::string &filename) {
    bool isDir = localFileSystem_->DirExists(copysetPath);
    // 是文件看是否需要回收
    if (!isDir) {
        if (IsChunkOrSnapShotFile(filename)) {
            return RecycleChunkfile(copysetPath, filename);
        } else if (IsWALFile(filename)) {
            return RecycleWAL(copysetPath, filename);
        } else {
            return true;
        }
    }

    // 是目录，继续list
    std::vector<std::string> files;
    if (0 != localFileSystem_->List(copysetPath, &files)) {
        LOG(ERROR) << "Trash failed to list files in " << copysetPath;
        return false;
    }

    // 遍历子文件
    bool ret = true;
    for (auto &file : files) {
        std::string filePath = copysetPath + "/" + file;
        // recycle 失败不应该中断其他文件的recycle
        if (!RecycleChunksAndWALInDir(filePath, file)) {
            ret = false;
        }
    }
    return ret;
}

bool Trash::RecycleChunkfile(
    const std::string &filepath, const std::string &filename) {
    LockGuard lg(mtx_);
    if (0 != chunkFilePool_->RecycleFile(filepath)) {
        LOG(ERROR) << "Trash  failed recycle chunk " << filepath
                    << " to FilePool";
        return false;
    }

    chunkNum_.fetch_sub(1);
    return true;
}

bool Trash::RecycleWAL(
    const std::string &filepath, const std::string &filename) {
    LockGuard lg(mtx_);
    if (walPool_ != nullptr && 0 != walPool_->RecycleFile(filepath)) {
        LOG(ERROR) << "Trash  failed recycle WAL " << filepath
                    << " to WALPool";
        return false;
    }

    chunkNum_.fetch_sub(1);
    return true;
}

bool Trash::IsWALFile(const std::string &fileName) {
    int match = 0;
    int64_t first_index = 0;
    int64_t last_index = 0;
    match = sscanf(fileName.c_str(), CURVE_SEGMENT_CLOSED_PATTERN,
                  &first_index, &last_index);
    if (match == 2) {
        LOG(INFO) << "recycle closed segment wal file, path: " << fileName
                  << " first_index: " << first_index
                  << " last_index: " << last_index;
        return true;
    }

    match = sscanf(fileName.c_str(), CURVE_SEGMENT_OPEN_PATTERN,
                   &first_index);
    if (match == 1) {
        LOG(INFO) << "recycle open segment wal file, path: " << fileName
                  << " first_index: " << first_index;
        return true;
    }
    return false;
}

uint32_t Trash::CountChunkNumInCopyset(const std::string &copysetPath) {
    auto count = [&](const std::string& path) {
        std::vector<std::string> chunks;
        localFileSystem_->List(path, &chunks);

        uint32_t chunkNum = 0;
        for (auto& chunk : chunks) {
            // valid: chunkfile, snapshotfile, walfile
            if (!(IsChunkOrSnapShotFile(chunk) || IsWALFile(chunk))) {
                LOG(WARNING) << "Trash find a illegal file:"
                             << chunk << " in " << path
                             << ", filename: " << chunk;
                continue;
            }
            ++chunkNum;
        }

        return chunkNum;
    };

    return count(copysetPath + "/" + RAFT_DATA_DIR)
        + count(copysetPath + "/" + RAFT_LOG_DIR);
}

}  // namespace chunkserver
}  // namespace curve
