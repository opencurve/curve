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
 * Created Date: 21-5-31
 * Author: huyao
 */
#include "curvefs/src/client/s3/client_s3.h"

namespace curvefs {

namespace client {
void S3ClientImpl::Init(const curve::common::S3AdapterOption& option) {
    s3Adapter_.Init(option);
}

int S3ClientImpl::Upload(const std::string& name,
                         const char* buf, uint64_t length) {
    int ret = 0;
    const Aws::String aws_key(name.c_str(), name.size());

    LOG(INFO) << "upload start, aws_key:" << aws_key
              << ",length:" << length;
    ret = s3Adapter_.PutObject(aws_key, (const void*)buf, length);
    LOG(INFO) << "upload end, ret:" << ret;
    return ret;
}

int S3ClientImpl::Download(const std::string& name, char* buf,
                           uint64_t offset, uint64_t length) {
    int ret = 0;

    LOG(INFO) << "download start, name:" << name
              << ",offset:" << offset << ",length:" << length;
    ret = s3Adapter_.GetObject(name, buf, offset, length);
    if (ret < 0) {
        LOG(INFO) << "download error:" << ret;
        return ret;
    }

    LOG(INFO) << "download end, ret:" << ret
              << ",length:" << length;
    return length;
}

int S3ClientImpl::Append(const std::string& name,
                         const char* buf, uint64_t length) {
    std::string data;
    std::string appendData(buf, length);
    int ret = 0;

    const Aws::String aws_key(name.c_str(), name.size());

    LOG(INFO) << "append get object start, aws_key:"
              << aws_key << ",length:" << length;
    ret = s3Adapter_.GetObject(aws_key, &data);
    if (ret < 0) {
        LOG(INFO) << "append get object error:" << ret;
        return ret;
    }

    data += appendData;
    LOG(INFO) << "append put object start, aws_key:"
              << aws_key << ",data len:" << data.length();
    ret = s3Adapter_.PutObject(aws_key, data);
    LOG(INFO) << "append put object end, ret:" << ret;

    return ret;
}

}  // namespace client
}  // namespace curvefs
