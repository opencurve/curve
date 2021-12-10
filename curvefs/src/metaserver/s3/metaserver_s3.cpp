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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#include "curvefs/src/metaserver/s3/metaserver_s3.h"

namespace curvefs {
namespace metaserver {

void S3ClientImpl::SetAdaptor(
    std::shared_ptr<curve::common::S3Adapter> s3Adapter) {
    s3Adapter_ = s3Adapter;
}

void S3ClientImpl::Init(const curve::common::S3AdapterOption& option) {
    s3Adapter_->Init(option);
}

int S3ClientImpl::Delete(const std::string& name) {
    int ret = 0;
    const Aws::String aws_key(name.c_str(), name.length());
    ret = s3Adapter_->DeleteObject(aws_key);
    if (ret < 0) {
        // -1
        LOG(ERROR) << "delete object: " << aws_key << " get error:" << ret;
        if (!s3Adapter_->ObjectExist(aws_key)) {
            // the aws_key is not exist
            // may delete by others
            ret = 0;
        }
    } else {
        // 0
        LOG(INFO) << "delete object: " << aws_key << " end, ret:" << ret;
    }

    return ret;
}
}  // namespace metaserver
}  // namespace curvefs
