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
    // curvefs上的root用户信息
    UserInfo curveUser;
    // curvefs 的配置文件路径
    std::string curveConf;
    // s3adapter 的配置文件路径
    std::string s3Conf;
    // curve client的对象指针
    std::shared_ptr<FileClient> curveClient;
    // s3 adapter的对象指针
    std::shared_ptr<S3Adapter> s3Client;
};

struct AsyncDownloadContext {
    // 源chunk的位置信息
    string location;
    // 请求下载数据在对象中的相对偏移
    off_t offset;
    // 请求下载数据的的长度
    size_t size;
    // 存放下载数据的缓冲区
    char* buf;
};

std::ostream& operator<<(std::ostream& out, const AsyncDownloadContext& rhs);

class OriginCopyer {
 public:
    OriginCopyer();
    virtual ~OriginCopyer() = default;

    /**
     * 初始化资源
     * @param options: 配置信息
     * @return: 成功返回0，失败返回-1
     */
    virtual int Init(const CopyerOptions& options);

    /**
     * 释放资源
     * @return: 成功返回0，失败返回-1
     */
    virtual int Fini();

    /**
     * 异步地从源端拷贝数据
     * @param done：包含下载请求的上下文信息，
     * 数据下载完成后执行该closure进行回调
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
    // curvefs上的root用户信息
    UserInfo curveUser_;
    // 负责跟curve交互
    std::shared_ptr<FileClient> curveClient_;
    // 负责跟s3交互
    std::shared_ptr<S3Adapter>  s3Client_;
    // 保护fdMap_的互斥锁
    std::mutex  mtx_;
    // 文件名->文件fd 的映射
    std::unordered_map<std::string, int> fdMap_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_COPYER_H_
