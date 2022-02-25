/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-02-28
 * Author: chengyi
 */

#ifndef CURVEFS_SRC_MDS_S3_MDS_S3_H_
#define CURVEFS_SRC_MDS_S3_MDS_S3_H_

#include <memory>
#include <string>
#include <list>

#include "src/common/s3_adapter.h"

namespace curvefs {
namespace mds {
class S3Client {
 public:
    S3Client() {}
    virtual ~S3Client() {}
    virtual void Init(const curve::common::S3AdapterOption& option) = 0;
    virtual void Reinit(const std::string& ak, const std::string& sk,
                        const std::string& endpoint,
                        const std::string& bucketName) = 0;
    virtual void SetAdaptor(
        std::shared_ptr<curve::common::S3Adapter> s3Adapter) = 0;
    virtual bool BucketExist() = 0;
};

class S3ClientImpl : public S3Client {
 public:
    S3ClientImpl() : S3Client() {}
    virtual ~S3ClientImpl() {}

    void SetAdaptor(std::shared_ptr<curve::common::S3Adapter> s3Adapter);
    void Init(const curve::common::S3AdapterOption& option) override;
    void Reinit(const std::string& ak, const std::string& sk,
      const std::string& endpoint, const std::string& bucketName) override;
    bool BucketExist();

 private:
    std::shared_ptr<curve::common::S3Adapter> s3Adapter_;
    curve::common::S3AdapterOption option_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_S3_MDS_S3_H_
