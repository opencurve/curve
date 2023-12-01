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
#include <aws/core/Aws.h>                                 //NOLINT
#include <aws/core/auth/AWSCredentialsProvider.h>         //NOLINT
#include <aws/core/client/ClientConfiguration.h>          //NOLINT
#include <aws/core/http/HttpRequest.h>                    //NOLINT
#include <aws/core/http/Scheme.h>                         //NOLINT
#include <aws/core/utils/memory/AWSMemory.h>              //NOLINT
#include <aws/core/utils/memory/stl/AWSString.h>          //NOLINT
#include <aws/core/utils/memory/stl/AWSStringStream.h>    //NOLINT
#include <aws/core/utils/threading/Executor.h>            //NOLINT
#include <aws/s3/S3Client.h>                              //NOLINT
#include <aws/s3/model/AbortMultipartUploadRequest.h>     //NOLINT
#include <aws/s3/model/BucketLocationConstraint.h>        //NOLINT
#include <aws/s3/model/CompleteMultipartUploadRequest.h>  //NOLINT
#include <aws/s3/model/CompletedPart.h>                   //NOLINT
#include <aws/s3/model/CreateBucketConfiguration.h>       //NOLINT
#include <aws/s3/model/CreateBucketRequest.h>             //NOLINT
#include <aws/s3/model/CreateMultipartUploadRequest.h>    //NOLINT
#include <aws/s3/model/Delete.h>                          //NOLINT
#include <aws/s3/model/DeleteBucketRequest.h>             //NOLINT
#include <aws/s3/model/DeleteObjectRequest.h>             //NOLINT
#include <aws/s3/model/DeleteObjectsRequest.h>            //NOLINT
#include <aws/s3/model/GetObjectRequest.h>                //NOLINT
#include <aws/s3/model/HeadBucketRequest.h>               //NOLINT
#include <aws/s3/model/HeadObjectRequest.h>               //NOLINT
#include <aws/s3/model/ObjectIdentifier.h>                //NOLINT
#include <aws/s3/model/PutObjectRequest.h>                //NOLINT
#include <aws/s3/model/UploadPartRequest.h>               //NOLINT

#include <condition_variable>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

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
    std::string region;
    int loglevel;
    std::string logPrefix;
    int scheme;
    bool verifySsl;
    std::string userAgent;
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
    bool useVirtualAddressing;
};

struct S3InfoOption {
    // should get from mds
    std::string ak;
    std::string sk;
    std::string s3Address;
    std::string bucketName;
    uint64_t blockSize;
    uint64_t chunkSize;
    uint32_t objectPrefix;
};

void InitS3AdaptorOptionExceptS3InfoOption(Configuration* conf,
                                           S3AdapterOption* s3Opt);

void InitS3AdaptorOption(Configuration* conf, S3AdapterOption* s3Opt);

using GetObjectAsyncCallBack = std::function<void(
    const S3Adapter*, const std::shared_ptr<GetObjectAsyncContext>&)>;

// get/put object context type
enum class ContextType {
    Unkown,  // should be default value
    S3,
    Disk,
};
struct GetObjectAsyncContext : public Aws::Client::AsyncCallerContext {
    std::string key;
    char* buf;
    off_t offset;
    size_t len;
    int retCode;  // >= 0 success, < 0 fail
    uint32_t retry;
    size_t actualLen;
    GetObjectAsyncCallBack cb;
    butil::Timer timer;
    ContextType type = ContextType::Unkown;

    explicit GetObjectAsyncContext(
        std::string key, char* buf, off_t offset, size_t len,
        GetObjectAsyncCallBack cb =
            [](const S3Adapter*,
               const std::shared_ptr<GetObjectAsyncContext>&) {},
        ContextType type = ContextType::Unkown)
        : key(std::move(key)),
          buf(buf),
          offset(offset),
          len(len),
          cb(std::move(cb)),
          type(type),
          timer(butil::Timer::STARTED) {}
};

/*
typedef std::function<void(const S3Adapter*,
    const std::shared_ptr<PutObjectAsyncContext>&)>
        PutObjectAsyncCallBack;
*/
using PutObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<PutObjectAsyncContext>&)>;

struct PutObjectAsyncContext : public Aws::Client::AsyncCallerContext {
    std::string key;
    const char* buffer;
    size_t bufferSize;
    PutObjectAsyncCallBack cb;
    butil::Timer timer;
    int retCode;  // >= 0 success, < 0 fail
    ContextType type;

    explicit PutObjectAsyncContext(
        std::string key, const char* buffer, size_t bufferSize,
        PutObjectAsyncCallBack cb =
            [](const std::shared_ptr<PutObjectAsyncContext>&) {},
        ContextType type = ContextType::Unkown)
        : key(std::move(key)),
          buffer(buffer),
          bufferSize(bufferSize),
          cb(std::move(cb)),
          type(type),
          timer(butil::Timer::STARTED) {}
};

class S3Adapter {
 public:
    S3Adapter() {
        clientCfg_ = nullptr;
        s3Client_ = nullptr;
        throttle_ = nullptr;
    }
    virtual ~S3Adapter() { Deinit(); }
    /**
     * Initialize S3Adapter
     */
    virtual void Init(const std::string& path);
    /**
     * Initialize S3Adapter
     * But not including S3InfoOption
     */
    virtual void InitExceptFsS3Option(const std::string& path);
    /**
     * Initialize S3Adapter
     */
    virtual void Init(const S3AdapterOption& option);
    /**
     * @brief
     *
     * @details
     */
    virtual void SetS3Option(const S3InfoOption& fsS3Opt);

    /**
     * Release S3Adapter resources
     */
    virtual void Deinit();
    /**
     *  call aws sdk shutdown api
     */
    static void Shutdown();
    /**
     * reinit s3client with new AWSCredentials
     */
    virtual void Reinit(const S3AdapterOption& option);
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
     * Create a bucket for storing snapshot data (the bucket name is specified
     * by the configuration file and needs to be globally unique)
     * @return: 0 successfully created/-1 failed to create
     */
    virtual int CreateBucket();
    /**
     * Delete Bucket
     * @return 0 deleted successfully/-1 deleted failed
     */
    virtual int DeleteBucket();
    /**
     * Determine whether the bucket of snapshot data exists
     * @return true bucket exists/false bucket does not exist
     */
    virtual bool BucketExist();
    /**
     * Upload data to object storage
     * @param object name
     * @param data content
     * @param data content size
     * @return: 0 Upload successful/-1 Upload failed
     */
    virtual int PutObject(const Aws::String& key, const char* buffer,
                          const size_t bufferSize);
    // Get object to buffer[bufferSize]
    // int GetObject(const Aws::String &key, void *buffer,
    //        const int bufferSize);
    /**
     * Upload data to object storage
     * @param object name
     * @param data content
     * @return: 0 Upload successful/-1 Upload failed
     */
    virtual int PutObject(const Aws::String& key, const std::string& data);
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
    virtual int GetObject(const Aws::String& key, std::string* data);
    /**
     * Reading data from object storage
     * @param object name
     * @param[out] returns the read data
     * @param read Offset read
     * @param The read length read
     */
    virtual int GetObject(const std::string& key, char* buf, off_t offset,
                          size_t len);  // NOLINT

    /**
     * @brief asynchronously reads data from object storage
     *
     * @param context asynchronous context
     */
    virtual void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context);
    /**
     * Delete Object
     * @param object name
     * @return: 0 successfully deleted/-
     */
    virtual int DeleteObject(const Aws::String& key);

    virtual int DeleteObjects(const std::list<Aws::String>& keyList);
    /**
     * Determine whether the object exists
     * @param object name
     * @return: true object exists/false object does not exist
     */
    virtual bool ObjectExist(const Aws::String& key);
    /*
    // Update object meta content
    // There are still issues with the Todo interface, need fix
    virtual int UpdateObjectMeta(const Aws::String &key,
                         const Aws::Map<Aws::String, Aws::String> &meta);
    // Get object meta content
    virtual int GetObjectMeta(const Aws::String &key,
                      Aws::Map<Aws::String, Aws::String> *meta);
    */
    /**
     * Initialize the sharding upload task of the object
     * @param object name
     * @return Task Name
     */
    virtual Aws::String MultiUploadInit(const Aws::String& key);
    /**
     * Add a shard to the shard upload task
     * @param object name
     * @param Task Name
     * @param Which shard (starting from 1)
     * @param shard size
     * @param sharded data content
     * @return: Fragmented task management object
     */
    virtual Aws::S3::Model::CompletedPart UploadOnePart(
        const Aws::String& key, const Aws::String& uploadId, int partNum,
        int partSize, const char* buf);
    /**
     * Complete the shard upload task
     * @param object name
     * @param Partitioning Upload Task ID
     * @param Manage vector for sharded upload tasks
     * @return 0 task completed/-1 task failed
     */
    virtual int CompleteMultiUpload(
        const Aws::String& key, const Aws::String& uploadId,
        const Aws::Vector<Aws::S3::Model::CompletedPart>& cp_v);
    /**
     * Terminate the sharding upload task of an object
     * @param object name
     * @param Task ID
     * @return 0 Terminated successfully/-1 Terminated failed
     */
    virtual int AbortMultiUpload(const Aws::String& key,
                                 const Aws::String& uploadId);
    void SetBucketName(const Aws::String& name) { bucketName_ = name; }
    Aws::String GetBucketName() { return bucketName_; }

    Aws::Client::ClientConfiguration* GetConfig() { return clientCfg_; }

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
    // S3 server address
    Aws::String s3Address_;
    // AK/SK for user authentication needs to be applied for from user
    // management in object storage
    Aws::String s3Ak_;
    Aws::String s3Sk_;
    // The bucket name of the object
    Aws::String bucketName_;
    // Configuration of AWS SDK
    Aws::Client::ClientConfiguration* clientCfg_;
    Aws::S3::S3Client* s3Client_;
    Configuration conf_;

    Throttle* throttle_;

    std::unique_ptr<AsyncRequestInflightBytesThrottle> inflightBytesThrottle_;
};

class FakeS3Adapter : public S3Adapter {
 public:
    FakeS3Adapter() : S3Adapter() {}
    virtual ~FakeS3Adapter() {}

    bool BucketExist() override { return true; }

    int PutObject(const Aws::String& key, const char* buffer,
                  const size_t bufferSize) override {
        (void)key;
        (void)buffer;
        (void)bufferSize;
        return 0;
    }

    int PutObject(const Aws::String& key, const std::string& data) override {
        (void)key;
        (void)data;
        return 0;
    }

    void PutObjectAsync(
        std::shared_ptr<PutObjectAsyncContext> context) override {
        context->retCode = 0;
        context->timer.stop();
        context->cb(context);
    }

    int GetObject(const Aws::String& key, std::string* data) override {
        (void)key;
        (void)data;
        // just return 4M data
        data->resize(4 * 1024 * 1024, '1');
        return 0;
    }

    int GetObject(const std::string& key, char* buf, off_t offset,
                  size_t len) override {
        (void)key;
        (void)offset;
        // juset return len data
        memset(buf, '1', len);
        return 0;
    }

    void GetObjectAsync(
        std::shared_ptr<GetObjectAsyncContext> context) override {
        memset(context->buf, '1', context->len);
        context->retCode = 0;
        context->cb(this, context);
    }

    int DeleteObject(const Aws::String& key) override {
        (void)key;
        return 0;
    }

    int DeleteObjects(const std::list<Aws::String>& keyList) override {
        (void)keyList;
        return 0;
    }

    bool ObjectExist(const Aws::String& key) override {
        (void)key;
        return true;
    }
};

}  // namespace common
}  // namespace curve
#endif  // SRC_COMMON_S3_ADAPTER_H_
