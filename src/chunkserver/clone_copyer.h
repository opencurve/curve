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
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_COPYER_H_
#define SRC_CHUNKSERVER_CLONE_COPYER_H_

#include <glog/logging.h>
#include <memory>
#include <unordered_map>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/location_operator.h"
#include "src/client/config_info.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "include/client/libcurve.h"
#include "src/common/s3_adapter.h"

namespace curve {
namespace chunkserver {

using curve::common::S3Adapter;
using curve::client::FileClient;
using curve::client::UserInfo;
using curve::common::LocationOperator;
using curve::common::OriginType;
using curve::common::GetObjectAsyncCallBack;
using curve::common::GetObjectAsyncContext;
using std::string;

class DownloadClosure;

struct CopyerOptions {
    // root user information of curvefs
    UserInfo curveUser;
    // configuration file path of curvefs
    std::string curveConf;
    // configuration file path of s3adapter
    std::string s3Conf;
    // object pointer to curve client
    std::shared_ptr<FileClient> curveClient;
    // object pointer to s3 adapter
    std::shared_ptr<S3Adapter> s3Client;
};

struct AsyncDownloadContext {
    // Location information of the source chunk
    string location;
    // Relative offset of the downloaded data in the object
    off_t offset;
    // Length of downloaded data
    size_t size;
    // Buffer for downloaded data
    char* buf;
};

std::ostream& operator<<(std::ostream& out, const AsyncDownloadContext& rhs);

class OriginCopyer {
 public:
    OriginCopyer();
    virtual ~OriginCopyer() = default;

    /**
     * init resources
     * @param options: configuration info
     * @return: Return 0 for success, -1 for failure
     */
    virtual int Init(const CopyerOptions& options);

    /**
     * free resources
     * @return: Return 0 for success, -1 for failure
     */
    virtual int Fini();

    /**
     * Copy data from the source asynchronously
     * @param done：Contain the contexts of the download request，
     * execute this closure for a callback after the data has been downloaded
     */
    virtual void DownloadAsync(DownloadClosure* done);

 private:
    void DownloadFromS3(const string& objectName,
                       off_t off,
                       size_t size,
                       char* buf,
                       DownloadClosure* done);
    void DownloadFromCurve(const string& fileName,
                          off_t off,
                          size_t size,
                          char* buf,
                          DownloadClosure* done);

 private:
    // root user information of curvefs
    UserInfo curveUser_;
    // Responsible for communicating with curves
    std::shared_ptr<FileClient> curveClient_;
    // Responsible for communicating with s3
    std::shared_ptr<S3Adapter>  s3Client_;
    // Mutex lock which protects fdMap_
    std::mutex  mtx_;
    // File name->file fd map
    std::unordered_map<std::string, int> fdMap_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_COPYER_H_
