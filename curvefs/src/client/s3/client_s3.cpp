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
#include "src/common/crc32.h"
#include "curvefs/src/client/s3/client_s3.h"

namespace curvefs {

namespace client {

using ::curve::common::CRC32;

void S3ClientImpl::Init(const curve::common::S3AdapterOption &option) {
    s3Adapter_->Init(option);
}

void S3ClientImpl::Deinit() {
    s3Adapter_->Deinit();
}

int S3ClientImpl::Upload(const std::string &name, const char *buf,
                         uint64_t length) {
    int ret = 0;
    const Aws::String aws_key(name.c_str(), name.size());

    VLOG(9) << "upload start, aws_key:" << aws_key << ",length:" << length;
    ret = s3Adapter_->PutObject(aws_key, buf, length);
    if (ret < 0) {
        LOG(WARNING) << "upload error:" << ret;
    }
    VLOG(9) << "upload end, ret:" << ret;
    return ret;
}

void S3ClientImpl::UploadAsync(
  std::shared_ptr<PutObjectAsyncContext> context) {
    const Aws::String aws_key(context->key.c_str(),
      context->key.size());
    VLOG(9) << "upload async start, aws_key: "
            << aws_key << ", length: "
            << context->bufferSize;
    s3Adapter_->PutObjectAsync(context);
}

int S3ClientImpl::Download(const std::string &name, char *buf, uint64_t offset,
                           uint64_t length) {
    int ret = 0;
    VLOG(9) << "download start, name:" << name << ",offset:" << offset
            << ",length:" << length;
    ret = s3Adapter_->GetObject(name, buf, offset, length);
    if (ret < 0) {
        const Aws::String aws_key(name.c_str(), name.size());
        if (!s3Adapter_->ObjectExist(aws_key)) {
            LOG(INFO) << "obj " << name << " seems not exist";
            ret = -2;
        }
        LOG(ERROR) << "download error:" << ret;
    }

    if (dataCrc_) {
        uint32_t checkSum = CRC32(buf, length);
        VLOG(3) << "download name: " << name << " ,crc: " << checkSum;
    }

    VLOG(9) << "download end, ret:" << ret << ",length:" << length;
    return ret;
}

void S3ClientImpl::DownloadAsync(
    std::shared_ptr<GetObjectAsyncContext> context) {
    VLOG(9) << "download async start, name:" << context->key
            << ",offset:" << context->offset << ",length:" << context->len;

    s3Adapter_->GetObjectAsync(context);
    return;
}

}  // namespace client
}  // namespace curvefs
