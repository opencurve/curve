/*************************************************************************
> File Name: snapshot_data_store.cpp
> Author:
> Created Time: Wed Dec 19 10:46:15 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include <utility>
#include <memory>
#include <glog/logging.h>    //NOLINT
#include <aws/core/utils/memory/stl/AWSString.h>  //NOLINT
#include <aws/core/utils/memory/stl/AWSMap.h>  //NOLINT
#include <aws/core/utils/StringUtils.h>   //NOLINT
namespace curve {
namespace snapshotcloneserver {

// nos conf
int S3SnapshotDataStore::Init(const std::string &path) {
    // Init server conf
    s3Adapter4Meta_->Init(path);
    s3Adapter4Data_->Init(path);
    // create bucket if not exist
    if (!s3Adapter4Meta_->BucketExist()) {
        return s3Adapter4Meta_->CreateBucket();
    } else {
        return 0;
    }
}

int S3SnapshotDataStore::PutChunkIndexData(const ChunkIndexDataName &name,
        const ChunkIndexData &indexData) {
    std::string key = name.ToIndexDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    std::string data;
    if (!indexData.Serialize(&data)) {
        LOG(ERROR) << "Failed to serialize ChunkIndexData";
        return -1;
    }
    return s3Adapter4Meta_->PutObject(aws_key, data);
}

int S3SnapshotDataStore::GetChunkIndexData(const ChunkIndexDataName &name,
        ChunkIndexData *indexData) {
    std::string key = name.ToIndexDataChunkKey();
    std::string *data = new std::string();
    const Aws::String aws_key(key.c_str(), key.size());
    if ((s3Adapter4Meta_->GetObject(aws_key, data) == 0)
            && (indexData->Unserialize(*data))) {
            delete data;
            return 0;
    }
    delete data;
    return -1;
}
bool S3SnapshotDataStore::ChunkIndexDataExist(const ChunkIndexDataName &name) {
    std::string key = name.ToIndexDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    if (s3Adapter4Meta_->ObjectExist(aws_key)) {
        return true;
    }
    return false;
}
/*/
int S3SnapshotDataStore::PutChunkData(const ChunkDataName &name,
        const ChunkData &data) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    std::string tmpdata = "test";
    s3Adapter4Data_->PutObject(aws_key, tmpdata);
}

int S3SnapshotDataStore::GetChunkData(const ChunkDataName &name,
        ChunkData *data) {
    std::string key = name.ToDataChunkKey();
    std::string tmpdata;
    const Aws::String aws_key(key.c_str(), key.size());
    s3Adapter4Data_->GetObject(aws_key, &tmpdata);
    return 0;
}
*/
bool S3SnapshotDataStore::ChunkDataExist(const ChunkDataName &name) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    if (s3Adapter4Meta_->ObjectExist(aws_key)) {
        return true;
    }
    return false;
}

int S3SnapshotDataStore::DeleteChunkIndexData(const ChunkIndexDataName &name) {
    std::string key = name.ToIndexDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    return s3Adapter4Meta_->DeleteObject(aws_key);
}

int S3SnapshotDataStore::DeleteChunkData(const ChunkDataName &name) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    return s3Adapter4Meta_->DeleteObject(aws_key);
}
/*
int S3SnapshotDataStore::SetSnapshotFlag(const ChunkIndexDataName &name,
                                         int flag) {
    std::string key = name.ToIndexDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    Aws::Map<Aws::String, Aws::String> meta;
    Aws::String flagStr = Aws::Utils::StringUtils::to_string(flag);
    meta.insert(std::pair<Aws::String,
                Aws::String>("status", flagStr));
    return s3Adapter4Meta_->UpdateObjectMeta(aws_key, meta);
}

int S3SnapshotDataStore::GetSnapshotFlag(const ChunkIndexDataName &name) {
    std::string key = name.ToIndexDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    Aws::Map<Aws::String, Aws::String> meta;
    if (s3Adapter4Meta_->GetObjectMeta(aws_key, &meta) < 0) {
        return -1;
    }
    auto search = meta.find("status");
    if (search != meta.end()) {
        std::string s(search->second.c_str(), search->second.size());
        return std::stoi(s);
    } else {
        return -1;
    }
}
*/
int S3SnapshotDataStore::DataChunkTranferInit(const ChunkDataName &name,
                                    std::shared_ptr<TransferTask> task) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    Aws::String aws_uploadId = s3Adapter4Data_->MultiUploadInit(aws_key);
    if (aws_uploadId == "") {
        LOG(ERROR) << "Init multiupload failed";
        return -1;
    }
    std::string str(aws_uploadId.c_str(), aws_uploadId.size());
    task->uploadId_ = str;
    return 0;
}

int S3SnapshotDataStore::DataChunkTranferAddPart(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task,
                                        int partNum,
                                        int partSize,
                                        const char *buf) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    const Aws::String uploadId(task->uploadId_.c_str(), task->uploadId_.size());
    Aws::S3::Model::CompletedPart cp =
        s3Adapter4Data_->UploadOnePart(
            aws_key, uploadId, partNum + 1, partSize, buf);
    std::string etag(cp.GetETag().c_str(), cp.GetETag().size());
    int tmp_partnum = cp.GetPartNumber();
    if (etag == "errorTag" && tmp_partnum == -1) {
        LOG(ERROR) << "Failed to UploadOnePart";
        return -1;
    }
    task->AddPartInfo(tmp_partnum, etag);
    return 0;
}

int S3SnapshotDataStore::DataChunkTranferComplete(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    const Aws::String uploadId(task->uploadId_.c_str(), task->uploadId_.size());
    Aws::Vector<Aws::S3::Model::CompletedPart> cp_v;
    for (auto &v : task->GetPartInfo()) {
        Aws::String str(v.second.c_str(), v.second.size());
        cp_v.push_back(Aws::S3::Model::CompletedPart()
                       .WithETag(str)
                       .WithPartNumber(v.first));
    }
    return s3Adapter4Data_->CompleteMultiUpload(aws_key, uploadId, cp_v);
}

int S3SnapshotDataStore::DataChunkTranferAbort(const ChunkDataName &name,
                                    std::shared_ptr<TransferTask> task) {
    std::string key = name.ToDataChunkKey();
    const Aws::String aws_key(key.c_str(), key.size());
    const Aws::String uploadId(task->uploadId_.c_str(), task->uploadId_.size());
    return s3Adapter4Data_->AbortMultiUpload(aws_key, uploadId);
}
}  // namespace snapshotcloneserver
}  // namespace curve

