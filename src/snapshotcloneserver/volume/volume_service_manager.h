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

#ifndef SRC_SNAPSHOTCLONESERVER_VOLUME_VOLUME_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_VOLUME_VOLUME_SERVICE_MANAGER_H_

#include <memory>
#include <vector>
#include <string>

#include "json/json.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"

namespace curve {
namespace snapshotcloneserver {

enum class FileInfoStatus {
    done = 0,
    flattening = 1,
    unflattened = 2,
};

enum class FileInfoType {
    file = 0,
    directory = 1,
    unknown = 2,
};

class FileInfo {
 public:
    FileInfo() = default;

    void SetFileName(const std::string &fileName) {
        fileName_ = fileName;
    }

    std::string GetFileName() const {
        return fileName_;
    }

    void SetFileInfoType(FileInfoType fileType) {
        fileType_ = fileType;
    }

    FileInfoType GetFileInfoType() const {
        return fileType_;
    }

    void SetFileStatInfo(const FileStatInfo &fileStatInfo) {
        user_ = std::string(fileStatInfo.owner);
        ctime_ = fileStatInfo.ctime;
        fileLength_ = fileStatInfo.length;
        stripeUnit_ = fileStatInfo.stripeUnit;
        stripeCount_ = fileStatInfo.stripeCount;
    }

    void SetFileInfoStatus(FileInfoStatus status) {
        status_ = status;
    }

    FileInfoStatus GetFileInfoStatus() const {
        return status_;
    }

    void SetProgress(uint32_t progress) {
        progress_ = progress;
    }

    uint32_t GetProgress() const {
        return progress_;
    }

    Json::Value ToJsonObj() const {
        Json::Value fileInfo;
        fileInfo["File"] = fileName_;
        fileInfo["User"] = user_;
        fileInfo["Type"] = static_cast<int>(fileType_);
        fileInfo["Time"] = ctime_;
        fileInfo["FileLength"] = fileLength_;
        fileInfo["StripeUnit"] = stripeUnit_;
        fileInfo["StripeCount"] = stripeCount_;
        fileInfo["Status"] = static_cast<int>(status_);
        fileInfo["Progress"] = progress_;
        return fileInfo;
    }

 private:
    std::string fileName_;
    std::string user_;
    FileInfoType fileType_;
    uint64_t        ctime_;
    uint64_t        fileLength_;
    uint64_t        stripeUnit_;
    uint64_t        stripeCount_;
    FileInfoStatus      status_;
    uint32_t progress_;
};

class VolumeServiceManager {
 public:
     explicit VolumeServiceManager(
        const std::shared_ptr<CurveFsClient> &client)
       : client_(client) {}

    virtual ~VolumeServiceManager() {}

    virtual int CreateFile(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset);

    virtual int DeleteFile(const std::string &file,
        const std::string &user);

    virtual int GetFile(const std::string &file,
        const std::string &user,
        FileInfo *fileInfo);

    virtual int ListFile(const std::string &dir,
        const std::string &user,
        std::vector<FileInfo> *fileInfos);

 private:
    int BuildFileInfo(const std::string &file,
        const std::string &user,
        const FileStatInfo &statInfo,
        FileInfo *fileInfo);

 private:
    std::shared_ptr<CurveFsClient> client_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_VOLUME_VOLUME_SERVICE_MANAGER_H_
