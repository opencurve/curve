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
 * Created Date: 2023-09-25
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/volume/volume_service_manager.h"

#include <memory>
#include <string>
#include <vector>

#include "src/common/snapshotclone/snapshotclone_define.h"

namespace curve {
namespace snapshotcloneserver {

int VolumeServiceManager::CreateFile(const std::string &file,
    const std::string &user,
    uint64_t size,
    uint64_t stripeUnit,
    uint64_t stripeCount,
    const std::string &poolset) {
    int ret = client_->CreateFile(file, user, size, stripeUnit, stripeCount,
        poolset);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        if (kErrCodeFileExist == ret) {
            LOG(INFO) << "CreateFile found file exist, "
                      << ", file = " << file
                      << ", user = " << user
                      << ", size = " << size
                      << ", stripeUnit = " << stripeUnit
                      << ", stripeCount = " << stripeCount
                      << ", poolset = " << poolset;
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "CreateFile fail, ret = " << ret
                   << ", file = " << file
                   << ", user = " << user
                   << ", size = " << size
                   << ", stripeUnit = " << stripeUnit
                   << ", stripeCount = " << stripeCount
                   << ", poolset = " << poolset;
        return ret;
    }
    LOG(INFO) << "CreateFile success, file = " << file
              << ", user = " << user
              << ", size = " << size
              << ", stripeUnit = " << stripeUnit
              << ", stripeCount = " << stripeCount
              << ", poolset = " << poolset;
    return kErrCodeSuccess;
}

int VolumeServiceManager::DeleteFile(const std::string &file,
    const std::string &user) {
    int ret = client_->DeleteFile(file, user);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        if (kErrCodeFileNotExist == ret) {
            LOG(INFO) << "DeleteFile found file not exist, "
                      << ", file = " << file
                      << ", user = " << user;
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "DeleteFile fail, ret = " << ret
                   << ", file = " << file
                   << ", user = " << user;
        return ret;
    }
    LOG(INFO) << "DeleteFile success, file = " << file
              << ", user = " << user;
    return kErrCodeSuccess;
}

int VolumeServiceManager::GetFile(const std::string &file,
    const std::string &user,
    FileInfo *fileInfo) {
    FileStatInfo statInfo;
    int ret = client_->StatFile(file, user, &statInfo);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        if (kErrCodeFileNotExist == ret) {
            LOG(INFO) << "StatFile found file not exist, "
                      << ", file = " << file
                      << ", user = " << user;
            return kErrCodeFileNotExist;
        }
        LOG(ERROR) << "StatFile fail, ret = " << ret
                   << ", file = " << file
                   << ", user = " << user;
        return ret;
    }
    return BuildFileInfo(file, user, statInfo, fileInfo);
}

int VolumeServiceManager::BuildFileInfo(const std::string &file,
    const std::string &user,
    const FileStatInfo &statInfo,
    FileInfo *fileInfo) {
    fileInfo->SetFileName(file);
    fileInfo->SetFileStatInfo(statInfo);
    if (FileType::INODE_CLONE_PAGEFILE == statInfo.filetype) {
        FileStatus fileStatus;
        uint32_t progress;
        int ret = client_->QueryFlattenStatus(
            file, user, &fileStatus, &progress);
        if (ret != LIBCURVE_ERROR::OK) {
            if (ret == -LIBCURVE_ERROR::NOTEXIST) {
                LOG(INFO) << "QueryFlattenStatus fail, file not exist, ret = "
                          << ret << ", file = " << file
                          << ", user = " << user;
                return kErrCodeFileNotExist;
            }
            LOG(ERROR) << "QueryFlattenStatus fail, ret = " << ret
                       << ", file = " << file
                       << ", user = " << user;
            return kErrCodeInternalError;
        }
        if (FileStatus::Flattening == fileStatus) {
            fileInfo->SetFileInfoStatus(FileInfoStatus::flattening);
        } else {
            fileInfo->SetFileInfoStatus(FileInfoStatus::unflattened);
        }
        fileInfo->SetFileInfoType(FileInfoType::file);
        fileInfo->SetProgress(progress);
    } else if (FileType::INODE_PAGEFILE == statInfo.filetype) {
        fileInfo->SetFileInfoStatus(FileInfoStatus::done);
        fileInfo->SetFileInfoType(FileInfoType::file);
        fileInfo->SetProgress(100);
    } else if (FileType::INODE_DIRECTORY == statInfo.filetype) {
        fileInfo->SetFileInfoStatus(FileInfoStatus::done);
        fileInfo->SetFileInfoType(FileInfoType::directory);
        fileInfo->SetProgress(100);
    } else {
        LOG(ERROR) << "unexpected file type, file = " << file
                   << ", user = " << user
                   << ", filetype = " << statInfo.filetype;
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int VolumeServiceManager::ListFile(const std::string &dir,
    const std::string &user,
    std::vector<FileInfo> *fileInfos) {
    std::vector<FileStatInfo> statInfos;
    int ret = client_->ListDir(dir, user, &statInfos);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        LOG(ERROR) << "ListFile fail, ret = " << ret
                   << ", user = " << user
                   << ", dir = " << dir;
        return ret;
    }

    for (const auto &statInfo : statInfos) {
        FileInfo fileInfo;
        std::string fullPathName;
        if (dir[dir.size() - 1] == '/') {
            fullPathName = dir + statInfo.filename;
        } else {
            fullPathName = dir + "/" + statInfo.filename;
        }
        int ret = BuildFileInfo(fullPathName, user, statInfo,
            &fileInfo);
        if (ret != kErrCodeSuccess) {
            if (ret == kErrCodeFileNotExist) {
                LOG(INFO) << "BuildFileInfo found file not exist, ret = "
                          << ret << ", user = " << user
                          << ", dir = " << dir
                          << ", filename = " << statInfo.filename;
                continue;
            }
            LOG(ERROR) << "BuildFileInfo fail, ret = " << ret
                       << ", user = " << user
                       << ", dir = " << dir
                       << ", filename = " << statInfo.filename;
            return ret;
        }
        fileInfos->push_back(fileInfo);
    }

    return kErrCodeSuccess;
}

}  // namespace snapshotcloneserver
}  // namespace curve
