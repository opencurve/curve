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

#include "curvefs/src/mds/s3/mds_s3.h"

namespace curvefs {
namespace mds {

void S3ClientImpl::SetAdaptor(
    std::shared_ptr<curve::common::S3Adapter> s3Adapter) {
    s3Adapter_ = s3Adapter;
}

void S3ClientImpl::Init(const curve::common::S3AdapterOption& option) {
    s3Adapter_->Init(option);
    option_ = option;
}

void S3ClientImpl::Reinit(const std::string& ak, const std::string& sk,
    const std::string& endpoint, const std::string& bucketName) {
    option_.ak = ak;
    option_.sk = sk;
    option_.s3Address = endpoint;
    option_.bucketName = bucketName;
    s3Adapter_->Reinit(option_);
}

bool S3ClientImpl::BucketExist() {
    return s3Adapter_->BucketExist();
}

}  // namespace mds
}  // namespace curvefs
