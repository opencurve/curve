/*************************************************************************
> File Name: s3_adapter.cpp
> Author:
> Created Time: Wed Dec 19 15:19:40 2018
> Copyright (c) 2018 netease
 ************************************************************************/
#include "src/snapshot/s3_adapter.h"
#include <glog/logging.h>
#include <memory>
#include <sstream>
namespace curve {
namespace snapshotserver {

void S3Adapter::Init() {
    options_ = new Aws::SDKOptions();
    options_->loggingOptions.logLevel =
            Aws::Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(*options_);
    s3Address_ = addr;
    s3Ak_ = ak;
    s3Sk_ = sk;
    bucketName_ = bucket_name;
    clientCfg_ = new Aws::Client::ClientConfiguration();
    clientCfg_->scheme = Aws::Http::Scheme::HTTP;
    clientCfg_->verifySSL = false;
    clientCfg_->userAgent = "S3 Browser";
    // timeout settings
    // clientCfg_.connectTimeoutMs = 60000;
    // clientCfg_.requestTimeoutMs = 10000;
    clientCfg_->endpointOverride = s3Address_;
    s3Client_ = new Aws::S3::S3Client(
            Aws::Auth::AWSCredentials(s3Ak_, s3Sk_),
                                        *clientCfg_);
}

void S3Adapter::Deinit() {
    Aws::ShutdownAPI(*options_);
    delete options_;
    delete clientCfg_;
    delete s3Client_;
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
/*
    int S3Adapter::PutObject(const Aws::String &key,
                  const void *buffer,
                  const int bufferSize) {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucketName_);
        request.SetKey(key);
        std::shared_ptr<Aws::StringStream> input_data =
                    Aws::MakeShared<Aws::StringStream>("stream",buffer);

        input_data->rdbuf()->pubsetbuf(buffer),
                                       bufferSize);
        input_data->rdbuf()->pubseekpos(bufferSize);
        input_data->seekg(0);
        request.SetBody(input_data);
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
*/
int S3Adapter::PutObject(const Aws::String &key,
                  const std::string &data) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    std::shared_ptr<Aws::IOStream> input_data =
                Aws::MakeShared<Aws::StringStream>("stream");
    *input_data << data;
    request.SetBody(input_data);
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

int S3Adapter::GetObject(const Aws::String &key,
                  std::string *data) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName_);
    request.SetKey(key);
    std::stringstream ss;
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
    Aws::String str(buf, partSize);
    auto input_data =
            Aws::MakeShared<Aws::StringStream>("UploadPartStream");
    *input_data << str;
    request.SetBody(input_data);
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
}  // namespace snapshotserver
}  // namespace curve
