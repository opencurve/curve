/*
 * Project: curve
 * Created Date: Mon Apr 27th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <time.h>
#include <glog/logging.h>
#include <vector>
#include "src/chunkserver/trash.h"
#include "src/common/string_util.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/copyset_node.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/uri_paser.h"

using ::curve::chunkserver::RAFT_DATA_DIR;
using ::curve::chunkserver::RAFT_META_DIR;
using ::curve::chunkserver::RAFT_SNAP_DIR;
using ::curve::chunkserver::RAFT_LOG_DIR;

namespace curve {
namespace chunkserver {
int Trash::Init(TrashOptions options) {
    isStop_ = true;

    if (UriParser::ParseUri(options.trashPath, &trashPath_).empty()) {
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
    chunkfilePool_ = options.chunkfilePool;

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
        LOG(INFO) << "stop trash recycle";
        exitcv_.notify_one();
        recycleThread_.join();
    }
    LOG(INFO) << "stop trash thread ok.";
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

    if (0 != localFileSystem_->Rename(dirPath, dst)) {
        LOG(ERROR) << "rename " << dirPath << " to " << dst << " error";
        return -1;
    }

    return 0;
}

void Trash::DeleteEligibleFileInTrashInterval() {
     while (!isStop_) {
         // 睡眠一段时间
         std::unique_lock<std::mutex> lk(exitmtx_);
         exitcv_.wait_for(lk, std::chrono::seconds(scanPeriodSec_),
                          [&]()->bool{ return isStop_;});
        if (isStop_) {
            return;
        }

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
        if (!IsCopySetDir(file)) {
            continue;
        }

        std::string copysetDir = trashPath_ + "/" + file;
        if (!NeedDelete(copysetDir)) {
            continue;
        }

        // 清理copyset目录
        std::string copysetPath = trashPath_ + "/" + file;
        CleanCopySet(copysetPath);
    }
}

bool Trash::IsCopySetDir(const std::string &dirName) {
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

void Trash::CleanCopySet(const std::string &copysetPath) {
    std::vector<std::string> out;
    if (0 != localFileSystem_->List(copysetPath, &out)) {
        LOG(ERROR) << "Trash failed to list files in " << copysetPath;
        return;
    }

    // 如果copyset目录文件是空的，直接删除
    if (out.empty()) {
        if (0 != localFileSystem_->Delete(copysetPath)) {
            LOG(ERROR) << "Trash failed to delete copyset dir "
                        << copysetPath;
        }
        return;
    }

    // 遍copyset下面所有文件
    for (auto file : out) {
        std::string filePath = copysetPath + "/" + file;

        // log或raft_snapshot文件夹直接删除
        if (file == RAFT_LOG_DIR ||
            file == RAFT_SNAP_DIR ||
            file == RAFT_META_DIR) {
            if (0 != localFileSystem_->Delete(filePath)) {
                LOG(ERROR) << "Trash failed to delete " << file;
            }
        } else if (file == RAFT_DATA_DIR) {
            RecycleChunks(filePath);
        } else {
            LOG(ERROR) << "Trash find unknown file " << filePath;
        }
    }
}

void Trash::RecycleChunks(const std::string &dataPath) {
    std::vector<std::string> chunks;
    if (0 != localFileSystem_->List(dataPath, &chunks)) {
        LOG(ERROR) << "Trash failed to list files in " << dataPath;
        return;
    }

    // data下面没有chunk
    if (chunks.empty()) {
        if (0 != localFileSystem_->Delete(dataPath)) {
            LOG(ERROR) << "Trash failed to delete " << dataPath;
        }
        return;
    }

    // 遍历data下面的chunk
    for (auto &chunk : chunks) {
        // 不是chunkfile或者snapshotfile
        if (!IsChunkOrSnapShotFile(chunk)) {
            LOG(ERROR) << "Trash find a illegal file:"
                       << chunk << " in " << dataPath;
            continue;
        }

        // 是chunkfile, 回收到chunkfilepool中
        std::string chunkPath = dataPath + "/" + chunk;
        if (0 != chunkfilePool_->RecycleChunk(chunkPath)) {
            LOG(ERROR) << "Trash  failed recycle chunk " << chunkPath
                       << " to chunkfilePool";
        }
    }
}

}  // namespace chunkserver
}  // namespace curve

