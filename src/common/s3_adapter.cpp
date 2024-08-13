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
> File Name: s3_adapter.cpp
> Author:
> Created Time: Wed Dec 19 15:19:40 2018
 ************************************************************************/

#include "src/common/s3_adapter.h"

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "src/common/curve_define.h"
#include "src/common/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace curve {
namespace common {

std::once_flag S3INIT_FLAG;
std::once_flag S3SHUTDOWN_FLAG;
Aws::SDKOptions AWS_SDK_OPTIONS;

namespace {

// https://github.com/aws/aws-sdk-cpp/issues/1430
class PreallocatedIOStream : public Aws::IOStream {
 public:
    PreallocatedIOStream(char *buf, size_t size)
        : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
              reinterpret_cast<unsigned char *>(buf), size)) {}

    PreallocatedIOStream(const char *buf, size_t size)
        : PreallocatedIOStream(const_cast<char *>(buf), size) {}

    ~PreallocatedIOStream() {
        // corresponding new in constructor
        delete rdbuf();
    }
};

Aws::String GetObjectRequestRange(uint64_t offset, uint64_t len) {
    // https://www.rfc-editor.org/rfc/rfc9110.html#name-range
    auto range =
        "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + len - 1);
    return {range.data(), range.size()};
}

}  // namespace

void InitS3AdaptorOption(Configuration* conf, S3AdapterOption* s3Opt) {
    InitS3AdaptorOptionExceptS3InfoOption(conf, s3Opt);
    LOG_IF(FATAL, !conf->GetStringValue("s3.endpoint", &s3Opt->s3Address));
    LOG_IF(FATAL, !conf->GetStringValue("s3.ak", &s3Opt->ak));
    LOG_IF(FATAL, !conf->GetStringValue("s3.sk", &s3Opt->sk));
    LOG_IF(FATAL, !conf->GetStringValue("s3.bucket_name", &s3Opt->bucketName));
}

void InitS3AdaptorOptionExceptS3InfoOption(Configuration* conf,
                                           S3AdapterOption* s3Opt) {
    LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &s3Opt->loglevel));
    LOG_IF(FATAL, !conf->GetStringValue("s3.logPrefix", &s3Opt->logPrefix));
    LOG_IF(FATAL, !conf->GetIntValue("s3.http_scheme", &s3Opt->scheme));
    LOG_IF(FATAL, !conf->GetBoolValue("s3.verify_SSL", &s3Opt->verifySsl));
    LOG_IF(FATAL, !conf->GetStringValue("s3.user_agent", &s3Opt->userAgent));
    LOG_IF(FATAL, !conf->GetIntValue("s3.maxConnections",
        &s3Opt->maxConnections));
    LOG_IF(FATAL, !conf->GetIntValue("s3.connectTimeout",
        &s3Opt->connectTimeout));
    LOG_IF(FATAL, !conf->GetIntValue("s3.requestTimeout",
        &s3Opt->requestTimeout));
    LOG_IF(FATAL, !conf->GetIntValue("s3.asyncThreadNum",
        &s3Opt->asyncThreadNum));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsTotalLimit",
        &s3Opt->iopsTotalLimit));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsReadLimit",
        &s3Opt->iopsReadLimit));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsWriteLimit",
        &s3Opt->iopsWriteLimit));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsTotalMB",
        &s3Opt->bpsTotalMB));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsReadMB",
        &s3Opt->bpsReadMB));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsWriteMB",
        &s3Opt->bpsWriteMB));
    LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
        &s3Opt->useVirtualAddressing));
    LOG_IF(FATAL, !conf->GetStringValue("s3.region", &s3Opt->region));

    if (!conf->GetUInt64Value("s3.maxAsyncRequestInflightBytes",
                              &s3Opt->maxAsyncRequestInflightBytes)) {
        LOG(WARNING) << "Not found s3.maxAsyncRequestInflightBytes in conf";
        s3Opt->maxAsyncRequestInflightBytes = 0;
    }
}

void S3Adapter::Init(const std::string& path) {
    LOG(INFO) << "Loading s3 configurations";
    conf_.SetConfigPath(path);
    LOG_IF(FATAL, !conf_.LoadConfig())
        << "Failed to open s3 config file: " << conf_.GetConfigPath();
    S3AdapterOption option;
    InitS3AdaptorOption(&conf_, &option);
    Init(option);
}

void S3Adapter::InitExceptFsS3Option(const std::string& path) {
    LOG(INFO) << "Loading s3 configurations";
    conf_.SetConfigPath(path);
    LOG_IF(FATAL, !conf_.LoadConfig())
        << "Failed to open s3 config file: " << conf_.GetConfigPath();
    S3AdapterOption option;
    InitS3AdaptorOptionExceptS3InfoOption(&conf_, &option);
    Init(option);
}

void S3Adapter::Init(const S3AdapterOption &option) {
    auto initSDK = [&]() {
        AWS_SDK_OPTIONS.loggingOptions.logLevel =
            Aws::Utils::Logging::LogLevel(option.loglevel);
        AWS_SDK_OPTIONS.loggingOptions.defaultLogPrefix =
            option.logPrefix.c_str();
        Aws::InitAPI(AWS_SDK_OPTIONS);
    };
    std::call_once(S3INIT_FLAG, initSDK);
    s3Address_ = option.s3Address.c_str();
    s3Ak_ = option.ak.c_str();
    s3Sk_ = option.sk.c_str();
    bucketName_ = option.bucketName.c_str();
    clientCfg_ = Aws::New<Aws::Client::ClientConfiguration>(AWS_ALLOCATE_TAG);
    clientCfg_->scheme = Aws::Http::Scheme(option.scheme);
    clientCfg_->verifySSL = option.verifySsl;
    clientCfg_->userAgent = option.userAgent.c_str();
    clientCfg_->region = option.region.c_str();
    clientCfg_->maxConnections = option.maxConnections;
    clientCfg_->connectTimeoutMs = option.connectTimeout;
    clientCfg_->requestTimeoutMs = option.requestTimeout;
    clientCfg_->endpointOverride = s3Address_;
    auto asyncThreadNum = option.asyncThreadNum;
    LOG(INFO) << "S3Adapter init thread num = " << asyncThreadNum << std::endl;
    clientCfg_->executor =
        Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            "S3Adapter.S3Client", asyncThreadNum);
    s3Client_ = Aws::New<Aws::S3::S3Client>(AWS_ALLOCATE_TAG,
            Aws::Auth::AWSCredentials(s3Ak_, s3Sk_),
            *clientCfg_,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            option.useVirtualAddressing);

    ReadWriteThrottleParams params;
    params.iopsTotal.limit = option.iopsTotalLimit;
    params.iopsRead.limit = option.iopsReadLimit;
    params.iopsWrite.limit = option.iopsWriteLimit;
    params.bpsTotal.limit = option.bpsTotalMB * kMB;
    params.bpsRead.limit = option.bpsReadMB * kMB;
    params.bpsWrite.limit = option.bpsWriteMB * kMB;

    throttle_ = new Throttle();
    throttle_->UpdateThrottleParams(params);

    inflightBytesThrottle_.reset(new AsyncRequestInflightBytesThrottle(
        option.maxAsyncRequestInflightBytes == 0
            ? UINT64_MAX
            : option.maxAsyncRequestInflightBytes));
}

void S3Adapter::Deinit() {
    // delete s3client in s3adapter
    if (clientCfg_ != nullptr) {
        Aws::Delete<Aws::Client::ClientConfiguration>(clientCfg_);
        clientCfg_ = nullptr;
    }
    if (s3Client_ != nullptr) {
        Aws::Delete<Aws::S3::S3Client>(s3Client_);
        s3Client_ = nullptr;
    }
    if (throttle_ != nullptr) {
        delete throttle_;
        throttle_ = nullptr;
    }
    if (inflightBytesThrottle_ != nullptr)
        inflightBytesThrottle_.release();
}

void S3Adapter::Shutdown() {
    // one program should only call once
    auto shutdownSDK = [&]() {
        // Aws::ShutdownAPI(AWS_SDK_OPTIONS);
    };
    shutdownSDK();
}

void S3Adapter::Reinit(const S3AdapterOption& option) {
    Deinit();
    Init(option);
}

std::string S3Adapter::GetS3Ak() {
    return std::string(s3Ak_.c_str(), s3Ak_.size());
}

std::string S3Adapter::GetS3Sk() {
    return std::string(s3Sk_.c_str(), s3Sk_.size());
}

std::string S3Adapter::GetS3Endpoint() {
    return std::string(s3Address_.c_str(), s3Address_.size());
}

int S3Adapter::CreateBucket() {
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(bucketName_);
    Aws::S3::Model::CreateBucketConfiguration conf;
    conf.SetLocationConstraint(
            Aws::S3::Model::BucketLocationConstraint::us_east_1);
    request.SetCreateBucketConfiguration(conf);
    auto response = s3Client_->CreateBucket(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "CreateBucket error:"
                << bucketName_
                << "--"
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}

int S3Adapter::DeleteBucket() {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucketName_);
    auto response = s3Client_->DeleteBucket(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "DeleteBucket error:"
                << bucketName_
                << "--"
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}

bool S3Adapter::BucketExist() {
    Aws::S3::Model::HeadBucketRequest request;
    request.SetBucket(bucketName_);
    auto response = s3Client_->HeadBucket(request);
    if (response.IsSuccess()) {
        return true;
    } else {
        LOG(ERROR) << "HeadBucket error:"
                << bucketName_
                << "--"
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return false;
    }
}

int S3Adapter::PutObject(const Aws::String &key, const char *buffer,
                         const size_t bufferSize) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);

    request.SetBody(Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG,
                                                          buffer, bufferSize));

    if (throttle_) {
        throttle_->Add(false, bufferSize);
    }

    auto response = s3Client_->PutObject(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "PutObject error, bucket: " << bucketName_
                   << ", key: " << key << response.GetError().GetExceptionName()
                   << response.GetError().GetMessage();
        return -1;
    }
}

int S3Adapter::PutObject(const Aws::String &key, const std::string &data) {
    return PutObject(key, data.data(), data.size());
}
/*
    int S3Adapter::GetObject(const Aws::String &key,
                  void *buffer,
                  const int bufferSize) {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucketName_);
        request.SetKey(key);
        request.SetResponseStreamFactory(
            [buffer, bufferSize](){
                std::unique_ptr<Aws::StringStream>
                        stream(Aws::New<Aws::StringStream>("stream"));
                stream->rdbuf()->pubsetbuf(buffer,
                        bufferSize);
                return stream.release();
            });
        auto response = s3Client_->GetObject(request);
        if (response.IsSuccess()) {
            *buffer << response.GetResult().GetBody().rdbuf();
        } else {
            LOG(ERROR) << "GetObject error: "
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
            return -1;
        }
    }
*/

void S3Adapter::PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(Aws::String{context->key.c_str(), context->key.size()});

    request.SetBody(Aws::MakeShared<PreallocatedIOStream>(
        AWS_ALLOCATE_TAG, context->buffer, context->bufferSize));

    auto originCallback = context->cb;
    auto wrapperCallback =
        [this,
         originCallback](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
            inflightBytesThrottle_->OnComplete(ctx->bufferSize);
            ctx->cb = originCallback;
            ctx->cb(ctx);
        };

    Aws::S3::PutObjectResponseReceivedHandler handler =
        [context](
            const Aws::S3::S3Client * /*client*/,
            const Aws::S3::Model::PutObjectRequest & /*request*/,
            const Aws::S3::Model::PutObjectOutcome &response,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                &awsCtx) {
            std::shared_ptr<PutObjectAsyncContext> ctx =
                std::const_pointer_cast<PutObjectAsyncContext>(
                    std::dynamic_pointer_cast<const PutObjectAsyncContext>(
                        awsCtx));

            LOG_IF(ERROR, !response.IsSuccess())
                << "PutObjectAsync error: "
                << response.GetError().GetExceptionName()
                << "message: " << response.GetError().GetMessage()
                << "resend: " << ctx->key;

            ctx->retCode = (response.IsSuccess() ? 0 : -1);
            ctx->timer.stop();
            ctx->cb(ctx);
        };

    if (throttle_) {
        throttle_->Add(false, context->bufferSize);
    }

    inflightBytesThrottle_->OnStart(context->bufferSize);
    context->cb = std::move(wrapperCallback);
    s3Client_->PutObjectAsync(request, handler, context);
}

int S3Adapter::GetObject(const Aws::String &key,
                  std::string *data) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    std::stringstream ss;
    if (throttle_) {
        throttle_->Add(true,  1);
    }
    auto response = s3Client_->GetObject(request);
    if (response.IsSuccess()) {
        ss << response.GetResult().GetBody().rdbuf();
        *data = ss.str();
        return 0;
    } else {
        LOG(ERROR) << "GetObject error: "
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}

int S3Adapter::GetObject(const std::string &key,
                         char *buf,
                         off_t offset,
                         size_t len) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(Aws::String{key.c_str(), key.size()});
    request.SetRange(GetObjectRequestRange(offset, len));

    request.SetResponseStreamFactory([buf, len]() {
        return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, len);
    });

    if (throttle_) {
        throttle_->Add(true, len);
    }
    auto response = s3Client_->GetObject(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "GetObject error: "
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}

void S3Adapter::GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(Aws::String{context->key.c_str(), context->key.size()});
    request.SetRange(GetObjectRequestRange(context->offset, context->len));

    request.SetResponseStreamFactory([context]() {
        return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, context->buf,
                                              context->len);
    });

    auto originCallback = context->cb;
    auto wrapperCallback =
        [this, originCallback](
            const S3Adapter* /*adapter*/,
            const std::shared_ptr<GetObjectAsyncContext>& ctx) {
            inflightBytesThrottle_->OnComplete(ctx->len);
            ctx->cb = originCallback;
            ctx->cb(this, ctx);
        };

    Aws::S3::GetObjectResponseReceivedHandler handler =
        [this](const Aws::S3::S3Client * /*client*/,
               const Aws::S3::Model::GetObjectRequest & /*request*/,
               const Aws::S3::Model::GetObjectOutcome &response,
               const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                   &awsCtx) {
            std::shared_ptr<GetObjectAsyncContext> ctx =
                std::const_pointer_cast<GetObjectAsyncContext>(
                    std::dynamic_pointer_cast<const GetObjectAsyncContext>(
                        awsCtx));

            LOG_IF(ERROR, !response.IsSuccess())
                << "GetObjectAsync error: "
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
            ctx->actualLen = response.GetResult().GetContentLength();
            ctx->retCode = (response.IsSuccess() ? 0 : -1);
            ctx->cb(this, ctx);
        };

    if (throttle_) {
        throttle_->Add(true, context->len);
    }

    inflightBytesThrottle_->OnStart(context->len);
    context->cb = std::move(wrapperCallback);
    s3Client_->GetObjectAsync(request, handler, context);
}

bool S3Adapter::ObjectExist(const Aws::String &key) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    auto response = s3Client_->HeadObject(request);
    if (response.IsSuccess()) {
        return true;
    } else {
        LOG(WARNING) << "HeadObject error:" << bucketName_ << "--" << key
                     << "--" << response.GetError().GetExceptionName()
                     << response.GetError().GetMessage();
        return false;
    }
}

int S3Adapter::DeleteObject(const Aws::String &key) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    auto response = s3Client_->DeleteObject(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(WARNING) << "DeleteObject error:" << bucketName_ << "--" << key
                     << "--" << response.GetError().GetExceptionName()
                     << response.GetError().GetMessage();
        return -1;
    }
}

int S3Adapter::DeleteObjects(const std::list<Aws::String>& keyList) {
    Aws::S3::Model::DeleteObjectsRequest deleteObjectsRequest;
    Aws::S3::Model::Delete deleteObjects;
    for (const auto& key : keyList) {
        Aws::S3::Model::ObjectIdentifier ObjIdent;
        ObjIdent.SetKey(key);
        deleteObjects.AddObjects(ObjIdent);
    }

    deleteObjects.SetQuiet(false);
    deleteObjectsRequest.WithBucket(bucketName_).WithDelete(deleteObjects);
    auto response = s3Client_->DeleteObjects(deleteObjectsRequest);
    if (response.IsSuccess()) {
        for (auto del : response.GetResult().GetDeleted()) {
            LOG(INFO) << "delete ok : " << del.GetKey();
        }

        for (auto err : response.GetResult().GetErrors()) {
            LOG(WARNING) << "delete err : " << err.GetKey() << " --> "
                         << err.GetMessage();
        }

        if (response.GetResult().GetErrors().size() != 0) {
            return -1;
        }

        return 0;
    } else {
        LOG(ERROR) << response.GetError().GetMessage() << " failed, "
                   << deleteObjectsRequest.SerializePayload();
        return -1;
    }
    return 0;
}
/*
    // object元数据单独更新还有问题，需要单独的s3接口来支持
int S3Adapter::UpdateObjectMeta(const Aws::String &key,
                    const Aws::Map<Aws::String, Aws::String> &meta) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    auto input_data =
        Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    request.SetBody(input_data);
    request.SetMetadata(meta);
    auto response = s3Client_->PutObject(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "PutObject error:"
                << bucketName_ << key
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}

int S3Adapter::GetObjectMeta(const Aws::String &key,
                    Aws::Map<Aws::String, Aws::String> *meta) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    auto response = s3Client_->HeadObject(request);
    if (response.IsSuccess()) {
        *meta = response.GetResult().GetMetadata();
        return 0;
    } else {
        LOG(ERROR) << "HeadObject error:"
                << bucketName_ << key
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
}
*/
Aws::String S3Adapter::MultiUploadInit(const Aws::String &key) {
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.WithBucket(bucketName_).WithKey(key);
    auto response = s3Client_->CreateMultipartUpload(request);
    if (response.IsSuccess()) {
        return response.GetResult().GetUploadId();
    } else {
        LOG(ERROR) << "CreateMultipartUploadRequest error: "
                << response.GetError().GetMessage();
        return "";
    }
}

Aws::S3::Model::CompletedPart S3Adapter::UploadOnePart(
    const Aws::String &key,
    const Aws::String &uploadId,
    int partNum,
    int partSize,
    const char* buf) {
    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    request.SetUploadId(uploadId);
    request.SetPartNumber(partNum);
    request.SetContentLength(partSize);

    request.SetBody(
        Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, partSize));

    if (throttle_) {
        throttle_->Add(false, partSize);
    }
    auto result = s3Client_->UploadPart(request);
    if (result.IsSuccess()) {
        return Aws::S3::Model::CompletedPart()
            .WithETag(result.GetResult().GetETag()).WithPartNumber(partNum);
    } else {
        return Aws::S3::Model::CompletedPart()
                .WithETag("errorTag").WithPartNumber(-1);
    }
}

int S3Adapter::CompleteMultiUpload(const Aws::String &key,
                const Aws::String &uploadId,
            const Aws::Vector<Aws::S3::Model::CompletedPart> &cp_v) {
    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.WithBucket(bucketName_);
    request.SetKey(key);
    request.SetUploadId(uploadId);
    request.SetMultipartUpload(
        Aws::S3::Model::CompletedMultipartUpload().WithParts(cp_v));
    auto response = s3Client_->CompleteMultipartUpload(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "CompleteMultiUpload error: "
                << response.GetError().GetMessage();
        this->AbortMultiUpload(key, uploadId);
        return -1;
    }
}

int S3Adapter::AbortMultiUpload(const Aws::String &key,
                                const Aws::String &uploadId) {
    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.WithBucket(bucketName_);
    request.SetKey(key);
    request.SetUploadId(uploadId);
    auto response = s3Client_->AbortMultipartUpload(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "AbortMultiUpload error: "
                   << response.GetError().GetMessage();
        return -1;
    }
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnStart(uint64_t len) {
    std::unique_lock<std::mutex> lock(mtx_);
    while (inflightBytes_ + len > maxInflightBytes_) {
        cond_.wait(lock);
    }

    inflightBytes_ += len;
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnComplete(uint64_t len) {
    std::unique_lock<std::mutex> lock(mtx_);
    inflightBytes_ -= len;
    cond_.notify_all();
}
void S3Adapter::SetS3Option(const S3InfoOption& fsS3Opt) {
    s3Address_ = fsS3Opt.s3Address.c_str();
    s3Ak_ = fsS3Opt.ak.c_str();
    s3Sk_ = fsS3Opt.sk.c_str();
    bucketName_ = fsS3Opt.bucketName.c_str();
}

}  // namespace common
}  // namespace curve
