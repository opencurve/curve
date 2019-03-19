/*
 * Project: curve
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CLONE_COPYER_H_
#define SRC_CHUNKSERVER_CLONE_COPYER_H_

#include <glog/logging.h>
#include <memory>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/location_operator.h"
#include "src/client/config_info.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "src/client/libcurve_define.h"
#include "src/snapshotcloneserver/common/s3_adapter.h"

namespace curve {
namespace chunkserver {

using curve::snapshotcloneserver::S3Adapter;
using curve::client::FileClient;
using curve::client::UserInfo;
using curve::common::LocationOperator;
using curve::common::OriginType;
using std::string;

struct CopyerOptions {
    // curvefs上的root用户信息
    UserInfo curveUser;
    // curvefs 的配置文件路径
    std::string curveConf;
    // s3adapter 的配置文件路径
    std::string s3Conf;
};

class OriginCopyer {
 public:
    OriginCopyer() = default;
    OriginCopyer(std::shared_ptr<FileClient> curveClient,
                 std::shared_ptr<S3Adapter> s3Client);
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
     * 从源端拷贝数据
     * @param location：源chunk的位置信息
     * @param off：请求下载数据在对象中的相对偏移
     * @param size：请求下载数据的的长度
     * @param buf：存放下载数据的缓冲区
     * @return：成功返回0，失败返回-1
     */
    virtual int Download(const string& location,
                         off_t off,
                         size_t size,
                         char* buf);

 private:
    int DownloadFromS3(const string& objectName,
                       off_t off,
                       size_t size,
                       char* buf);
    int DownloadFromCurve(const string& fileName,
                          off_t off,
                          size_t size,
                          char* buf);

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
