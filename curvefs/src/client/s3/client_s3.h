/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 21-5-31
 * Author: huyao
 */
#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_H_

#include <string>
#include <memory>
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace client {

using curve::common::GetObjectAsyncContext;
class S3Client {
 public:
    S3Client() {}
    virtual ~S3Client() {}
    virtual void Init(const curve::common::S3AdapterOption& option) = 0;
    virtual int Upload(const std::string& name, const char* buf,
                       uint64_t length) = 0;
    virtual int Download(const std::string& name, char* buf, uint64_t offset,
                         uint64_t length) = 0;
    virtual void DownloadAsync(
        std::shared_ptr<GetObjectAsyncContext> context) = 0;
};

class S3ClientImpl : public S3Client {
 public:
    S3ClientImpl() : S3Client() {
        s3Adapter_ = std::make_shared<curve::common::S3Adapter>();
    }
    virtual ~S3ClientImpl() {}
    void Init(const curve::common::S3AdapterOption& option);
    int Upload(const std::string& name, const char* buf, uint64_t length);
    int Download(const std::string& name, char* buf, uint64_t offset,
                 uint64_t length);
    void DownloadAsync(std::shared_ptr<GetObjectAsyncContext> context);
    void SetAdapter(std::shared_ptr<curve::common::S3Adapter> adapter) {
        s3Adapter_ = adapter;
    }

 private:
    std::shared_ptr<curve::common::S3Adapter> s3Adapter_;
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_H_
