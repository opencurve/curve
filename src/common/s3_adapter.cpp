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
#include <glog/logging.h>
#include <memory>
#include <sstream>
#include <string>
#include "src/common/curve_define.h"

namespace curve {
namespace common {

void InitS3AdaptorOption(Configuration *conf,
    S3AdapterOption *s3Opt) {
    LOG_IF(FATAL, !conf->GetIntValue("s3.loglevel", &s3Opt->loglevel));
    LOG_IF(FATAL, !conf->GetStringValue("s3.endpoint", &s3Opt->s3Address));
    LOG_IF(FATAL, !conf->GetStringValue("s3.ak", &s3Opt->ak));
    LOG_IF(FATAL, !conf->GetStringValue("s3.sk", &s3Opt->sk));
    LOG_IF(FATAL, !conf->GetStringValue("s3.bucket_name",
        &s3Opt->bucketName));
    LOG_IF(FATAL, !conf->GetIntValue("s3.http_scheme", &s3Opt->scheme));
    LOG_IF(FATAL, !conf->GetBoolValue("s3.verify_SSL", &s3Opt->verifySsl));
    LOG_IF(FATAL, !conf->GetIntValue("s3.max_connections",
        &s3Opt->maxConnections));
    LOG_IF(FATAL, !conf->GetIntValue("s3.connect_timeout",
        &s3Opt->connectTimeout));
    LOG_IF(FATAL, !conf->GetIntValue("s3.request_timeout",
        &s3Opt->requestTimeout));
    LOG_IF(FATAL, !conf->GetIntValue("s3.async_thread_num",
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
}

void S3Adapter::Init(const std::string &path) {
    LOG(INFO) << "Loading s3 configurations";
    conf_.SetConfigPath(path);
    LOG_IF(FATAL, !conf_.LoadConfig())
              << "Failed to open s3 config file: " << conf_.GetConfigPath();
    S3AdapterOption option;
    InitS3AdaptorOption(&conf_, &option);
    Init(option);
}

void S3Adapter::Init(const S3AdapterOption &option) {
    options_ = Aws::New<Aws::SDKOptions>("S3Adapter.SDKOptions");
    options_->loggingOptions.logLevel =
      Aws::Utils::Logging::LogLevel(option.loglevel);

    Aws::InitAPI(*options_);
    s3Address_ = option.s3Address.c_str();
    s3Ak_ = option.ak.c_str();
    s3Sk_ = option.sk.c_str();
    bucketName_ = option.bucketName.c_str();
    clientCfg_ = Aws::New<Aws::Client::ClientConfiguration>(
        "S3Adapter.ClientConfiguration");
    clientCfg_->scheme = Aws::Http::Scheme(option.scheme);
    clientCfg_->verifySSL = option.verifySsl;
    //clientCfg_->userAgent = conf_.GetStringValue("s3.user_agent_conf").c_str();  //NOLINT
    clientCfg_->userAgent = "S3 Browser";
    clientCfg_->maxConnections = option.maxConnections;
    clientCfg_->connectTimeoutMs = option.connectTimeout;
    clientCfg_->requestTimeoutMs = option.requestTimeout;
    clientCfg_->endpointOverride = s3Address_;
    auto asyncThreadNum = option.asyncThreadNum;
    LOG(INFO) << "S3Adapter init thread num = " << asyncThreadNum << std::endl;
    clientCfg_->executor =
        Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            "S3Adapter.S3Client", asyncThreadNum);
    s3Client_ = Aws::New<Aws::S3::S3Client>("S3Adapter.S3Client",
            Aws::Auth::AWSCredentials(s3Ak_, s3Sk_),
            *clientCfg_,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);

    ReadWriteThrottleParams params;
    params.iopsTotal.limit = option.iopsTotalLimit;
    params.iopsRead.limit = option.iopsReadLimit;
    params.iopsWrite.limit = option.iopsWriteLimit;
    params.bpsTotal.limit = option.bpsTotalMB * kMB;
    params.bpsRead.limit = option.bpsReadMB * kMB;
    params.bpsWrite.limit = option.bpsWriteMB * kMB;

    throttle_ = new Throttle();
    throttle_->UpdateThrottleParams(params);
}

void S3Adapter::Deinit() {
    Aws::ShutdownAPI(*options_);
    Aws::Delete<Aws::SDKOptions>(options_);
    Aws::Delete<Aws::Client::ClientConfiguration>(clientCfg_);
    Aws::Delete<Aws::S3::S3Client>(s3Client_);
    delete throttle_;
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

int S3Adapter::PutObject(const Aws::String &key,
                  const void *buffer,
                  const int bufferSize) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    request.SetBody(Aws::MakeShared<Aws::StringStream>(
        "stream", Aws::String(static_cast<const char*>(buffer), bufferSize)));

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

int S3Adapter::PutObject(const Aws::String &key,
                  const std::string &data) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    std::shared_ptr<Aws::IOStream> input_data =
                Aws::MakeShared<Aws::StringStream>("stream");
    *input_data << data;
    request.SetBody(input_data);

    if (throttle_) {
        throttle_->Add(false, data.size());
    }

    auto response = s3Client_->PutObject(request);
    if (response.IsSuccess()) {
        return 0;
    } else {
        LOG(ERROR) << "PutObject error:"
                << bucketName_
                << "--"
                << key
                << "--"
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
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
    request.SetKey(context->key.c_str());
    request.SetBody(Aws::MakeShared<Aws::StringStream>(
        "stream",
        Aws::String(static_cast<char*>(context->buffer), context->bufferSize)));

    Aws::S3::PutObjectResponseReceivedHandler handler = [&] (
        const Aws::S3::S3Client* client,
        const Aws::S3::Model::PutObjectRequest& request,
        const Aws::S3::Model::PutObjectOutcome& response,
        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& awsCtx) {
        std::shared_ptr<const PutObjectAsyncContext> cctx =
            std::dynamic_pointer_cast<const PutObjectAsyncContext>(awsCtx);
        std::shared_ptr<PutObjectAsyncContext> ctx =
            std::const_pointer_cast<PutObjectAsyncContext>(cctx);
        if (response.IsSuccess()) {
            const Aws::S3::Model::PutObjectResult &result =
                response.GetResult();
            ctx->retCode = 0;
        } else {
            LOG(ERROR) << "PutObjectAsync error: "
                       << response.GetError().GetExceptionName()
                       << "message: " << response.GetError().GetMessage()
                       << "resend: " << context->key;
            PutObjectAsync(context);
            return;
        }
        ctx->cb(ctx);
    };
    if (throttle_) {
        throttle_->Add(true, context->bufferSize);
    }
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
    request.SetKey(key.c_str());
    request.SetRange(("bytes=" + std::to_string(offset) + "-" + std::to_string(offset+len)).c_str()); //NOLINT
    if (throttle_) {
        throttle_->Add(true, len);
    }
    auto response = s3Client_->GetObject(request);
    if (response.IsSuccess()) {
        response.GetResult().GetBody().rdbuf()->sgetn(buf, len);
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
    request.SetKey(context->key.c_str());
    request.SetRange(("bytes=" + std::to_string(context->offset) + "-" + std::to_string(context->offset + context->len)).c_str()); //NOLINT

    Aws::S3::GetObjectResponseReceivedHandler handler = [this] (
        const Aws::S3::S3Client* client,
        const Aws::S3::Model::GetObjectRequest& request,
        const Aws::S3::Model::GetObjectOutcome& response,
        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& awsCtx) {
        std::shared_ptr<const GetObjectAsyncContext> cctx =
            std::dynamic_pointer_cast<const GetObjectAsyncContext>(awsCtx);
        std::shared_ptr<GetObjectAsyncContext> ctx =
            std::const_pointer_cast<GetObjectAsyncContext>(cctx);
        if (response.IsSuccess()) {
            const Aws::S3::Model::GetObjectResult &result =
                response.GetResult();
            Aws::S3::Model::GetObjectResult &ret =
                const_cast<Aws::S3::Model::GetObjectResult&>(result);
            ret.GetBody().rdbuf()->sgetn(ctx->buf, ctx->len);  // NOLINT
            ctx->retCode = 0;
        } else {
            LOG(ERROR) << "GetObjectAsync error: "
                    << response.GetError().GetExceptionName()
                    << response.GetError().GetMessage();
            ctx->retCode = -1;
        }
        ctx->cb(this, ctx);
    };
    if (throttle_) {
        throttle_->Add(true, context->len);
    }
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
        LOG(ERROR) << "HeadObject error:"
                << bucketName_
                << "--"
                << key
                << "--"
                << response.GetError().GetExceptionName()
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
            LOG(ERROR) << "DeleteObject error:"
                << bucketName_
                << "--"
                << key
                << "--"
                << response.GetError().GetExceptionName()
                << response.GetError().GetMessage();
        return -1;
    }
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

Aws::S3::Model::CompletedPart S3Adapter:: UploadOnePart(
    const Aws::String &key,
    const Aws::String uploadId,
    int partNum,
    int partSize,
    const char* buf) {
    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    request.SetUploadId(uploadId);
    request.SetPartNumber(partNum);
    request.SetContentLength(partSize);
    auto str = std::make_shared<std::string>(buf, partSize);
    auto input_data =
            Aws::MakeShared<Aws::StringStream>("UploadPartStream");
    *input_data << *str;
    request.SetBody(input_data);
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
}  // namespace common
}  // namespace curve
