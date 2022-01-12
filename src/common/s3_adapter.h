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

/*************************************************************************
	> File Name: s3_adapter.h
	> Author:
	> Created Time: Mon Dec 10 14:30:12 2018
 ************************************************************************/

#ifndef SRC_COMMON_S3_ADAPTER_H_
#define SRC_COMMON_S3_ADAPTER_H_
#include <map>
#include <list>
#include <string>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <aws/core/utils/memory/AWSMemory.h>  //NOLINT
#include <aws/core/Aws.h>   //NOLINT
#include <aws/s3/S3Client.h>  //NOLINT
#include <aws/core/client/ClientConfiguration.h>  //NOLINT
#include <aws/core/auth/AWSCredentialsProvider.h>  //NOLINT
#include <aws/s3/model/PutObjectRequest.h>  //NOLINT
#include <aws/s3/model/CreateBucketRequest.h>  //NOLINT
#include <aws/s3/model/DeleteBucketRequest.h>  //NOLINT
#include <aws/s3/model/HeadBucketRequest.h>   //NOLINT
#include <aws/s3/model/HeadObjectRequest.h>  //NOLINT
#include <aws/s3/model/GetObjectRequest.h>  //NOLINT
#include <aws/s3/model/DeleteObjectRequest.h>  //NOLINT
#include <aws/s3/model/CreateMultipartUploadRequest.h>  //NOLINT
#include <aws/s3/model/UploadPartRequest.h>  //NOLINT
#include <aws/s3/model/CompleteMultipartUploadRequest.h>  //NOLINT
#include <aws/s3/model/AbortMultipartUploadRequest.h>   //NOLINT
#include <aws/s3/model/ObjectIdentifier.h>   //NOLINT
#include <aws/s3/model/Delete.h>   //NOLINT
#include <aws/s3/model/DeleteObjectsRequest.h>  //NOLINT
#include <aws/core/http/HttpRequest.h>  //NOLINT
#include <aws/s3/model/CompletedPart.h>  //NOLINT
#include <aws/core/http/Scheme.h>  //NOLINT
#include <aws/core/utils/memory/stl/AWSString.h>  //NOLINT
#include <aws/core/utils/memory/stl/AWSStringStream.h>  //NOLINT
#include <aws/s3/model/BucketLocationConstraint.h>  //NOLINT
#include <aws/s3/model/CreateBucketConfiguration.h>  //NOLINT
#include <aws/core/utils/threading/Executor.h> // NOLINT
#include "src/common/configuration.h"
#include "src/common/throttle.h"

namespace curve {
namespace common {

struct GetObjectAsyncContext;
struct PutObjectAsyncContext;
class S3Adapter;

struct S3AdapterOption {
    std::string ak;
    std::string sk;
    std::string s3Address;
    std::string bucketName;
    int loglevel;
    int scheme;
    bool verifySsl;
    int maxConnections;
    int connectTimeout;
    int requestTimeout;
    int asyncThreadNum;
    uint64_t maxAsyncRequestInflightBytes;
    uint64_t iopsTotalLimit;
    uint64_t iopsReadLimit;
    uint64_t iopsWriteLimit;
    uint64_t bpsTotalMB;
    uint64_t bpsReadMB;
    uint64_t bpsWriteMB;
};

void InitS3AdaptorOption(Configuration *conf,
    S3AdapterOption *s3Opt);

typedef std::function<void(const S3Adapter*,
    const std::shared_ptr<GetObjectAsyncContext>&)>
        GetObjectAsyncCallBack;

struct GetObjectAsyncContext : public Aws::Client::AsyncCallerContext {
    std::string key;
    char *buf;
    off_t offset;
    size_t len;
    GetObjectAsyncCallBack cb;
    int retCode;
};

/*
typedef std::function<void(const S3Adapter*,
    const std::shared_ptr<PutObjectAsyncContext>&)>
        PutObjectAsyncCallBack;
*/
typedef std::function<void(const std::shared_ptr<PutObjectAsyncContext>&)>
        PutObjectAsyncCallBack;

struct PutObjectAsyncContext : public Aws::Client::AsyncCallerContext {
    std::string key;
    const char *buffer;
    size_t bufferSize;
    PutObjectAsyncCallBack cb;
    uint64_t startTime;
    int retCode;
};

class S3Adapter {
 public:
    S3Adapter() {}
    virtual ~S3Adapter() {}
    /**
     * 初始化S3Adapter
     */
    virtual void Init(const std::string &path);
    /**
     * 初始化S3Adapter
     */
    virtual void Init(const S3AdapterOption &option);
    /**
     * 释放S3Adapter资源
     */
    virtual void Deinit();
    /**
     *  call aws sdk shutdown api
     */
    virtual void Shutdown();
    /**
     * reinit s3client with new AWSCredentials
     */
    virtual void Reinit(const std::string& ak, const std::string& sk,
                        const std::string& endpoint, S3AdapterOption option);
    /**
     * get s3 ak
     */
    virtual std::string GetS3Ak();
    /**
     * get s3 sk
     */
    virtual std::string GetS3Sk();
    /**
     * get s3 endpoint
     */
    virtual std::string GetS3Endpoint();
    /**
     * 创建存储快照数据的桶（桶名称由配置文件指定，需要全局唯一）
     * @return: 0 创建成功/ -1 创建失败
     */
    virtual int CreateBucket();
    /**
     * 删除桶
     * @return 0 删除成功/-1 删除失败
     */
    virtual int DeleteBucket();
    /**
     * 判断快照数据的桶是否存在
     * @return true 桶存在/ false 桶不存在
     */
    virtual bool BucketExist();
    /**
     * 上传数据到对象存储
     * @param 对象名
     * @param 数据内容
     * @param 数据内容大小
     * @return:0 上传成功/ -1 上传失败
     */
    virtual int PutObject(const Aws::String &key, const char *buffer,
            const size_t bufferSize);
    // Get object to buffer[bufferSize]
    // int GetObject(const Aws::String &key, void *buffer,
    //        const int bufferSize);
    /**
     * 上传数据到对象存储
     * @param 对象名
     * @param 数据内容
     * @return:0 上传成功/ -1 上传失败
     */
    virtual int PutObject(const Aws::String &key, const std::string &data);
    virtual void PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context);
    /**
     * Get object from s3,
     * note：this function is only used for control plane to get small data,
     * it only has qps limit and dosn't have bandwidth limit.
     * For the data plane, please use the following two function.
     *
     * @param object name
     * @param pointer which contain the data
     * @return 0 success / -1 fail
     */
    virtual int GetObject(const Aws::String &key, std::string *data);
    /**
     * 从对象存储读取数据
     * @param 对象名
     * @param[out] 返回读取的数据
     * @param 读取的偏移
     * @param 读取的长度
     */
    virtual int GetObject(const std::string &key, char *buf, off_t offset, size_t len);   //NOLINT

    /**
     * @brief 异步从对象存储读取数据
     *
     * @param context 异步上下文
     */
    virtual void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context);

    /**
     * 删除对象
     * @param 对象名
     * @return: 0 删除成功/ -
     */
    virtual int DeleteObject(const Aws::String &key);

    virtual int DeleteObjects(const std::list<Aws::String>& keyList);
    /**
     * 判断对象是否存在
     * @param 对象名
     * @return: true 对象存在/ false 对象不存在
     */
    virtual bool ObjectExist(const Aws::String &key);
    /*
    // Update object meta content
    // Todo 接口还有问题 need fix
    virtual int UpdateObjectMeta(const Aws::String &key,
                         const Aws::Map<Aws::String, Aws::String> &meta);
    // Get object meta content
    virtual int GetObjectMeta(const Aws::String &key,
                      Aws::Map<Aws::String, Aws::String> *meta);
    */
    /**
     * 初始化对象的分片上传任务
     * @param 对象名
     * @return 任务名
     */
    virtual Aws::String MultiUploadInit(const Aws::String &key);
    /**
     * 增加一个分片到分片上传任务中
     * @param 对象名
     * @param 任务名
     * @param 第几个分片（从1开始）
     * @param 分片大小
     * @param 分片的数据内容
     * @return: 分片任务管理对象
     */
    virtual Aws::S3::Model::CompletedPart UploadOnePart(const Aws::String &key,
            const Aws::String &uploadId,
            int partNum,
            int partSize,
            const char* buf);
    /**
     * 完成分片上传任务
     * @param 对象名
     * @param 分片上传任务id
     * @管理分片上传任务的vector
     * @return 0 任务完成/ -1 任务失败
     */
    virtual int CompleteMultiUpload(const Aws::String &key,
                            const Aws::String &uploadId,
                        const Aws::Vector<Aws::S3::Model::CompletedPart> &cp_v);
    /**
     * 终止一个对象的分片上传任务
     * @param 对象名
     * @param 任务id
     * @return 0 终止成功/ -1 终止失败
     */
    virtual int AbortMultiUpload(const Aws::String &key,
                        const Aws::String &uploadId);
    void SetBucketName(const Aws::String &name) {
        bucketName_ = name;
    }
    Aws::String GetBucketName() {
        return bucketName_;
    }

 private:
    class AsyncRequestInflightBytesThrottle {
     public:
        explicit AsyncRequestInflightBytesThrottle(uint64_t maxInflightBytes)
            : maxInflightBytes_(maxInflightBytes),
              inflightBytes_(0),
              mtx_(),
              cond_() {}

        void OnStart(uint64_t len);
        void OnComplete(uint64_t len);

     private:
        const uint64_t maxInflightBytes_;
        uint64_t inflightBytes_;

        std::mutex mtx_;
        std::condition_variable cond_;
    };

 private:
    // S3服务器地址，由配置文件指定
    Aws::String s3Address_;
    // 用于用户认证的AK/SK，需要从对象存储的用户管理中申请，并在配置文件中指定
    Aws::String s3Ak_;
    Aws::String s3Sk_;
    // 对象的桶名，根据配置文件指定
    Aws::String bucketName_;
    // aws sdk的配置，同样由配置文件指定
    Aws::Client::ClientConfiguration *clientCfg_;
    Aws::S3::S3Client *s3Client_;
    Configuration conf_;

    Throttle *throttle_;

    std::unique_ptr<AsyncRequestInflightBytesThrottle> inflightBytesThrottle_;
};
}  // namespace common
}  // namespace curve
#endif  // SRC_COMMON_S3_ADAPTER_H_
