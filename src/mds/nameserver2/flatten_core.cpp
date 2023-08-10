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
 * Created Date: 2023-07-24
 * Author: xuchaojie
 */

#include "src/mds/nameserver2/flatten_core.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

using curve::mds::chunkserverclient::CopysetClientClosure;
using curve::mds::chunkserverclient::CloneInfos;

namespace curve {
namespace mds {

struct FlattenChunkClosure : public CopysetClientClosure {
    FlattenChunkClosure(const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
        const std::shared_ptr<FlattenChunkContext> &context)
        : tracker_(tracker), context_(context) {}

    void Run() override {
        std::unique_ptr<FlattenChunkClosure> selfGuard(this);
        context_->retCode = GetErrCode();
        if (context_->retCode < 0) {
            LOG(ERROR) << "flatten chunk fail, ret: " << context_->retCode;
        }
        tracker_->PushResultContext(context_);
        tracker_->HandleResponse(context_->retCode);
    }

    std::shared_ptr<FlattenChunkTaskTracker> tracker_;
    std::shared_ptr<FlattenChunkContext> context_;
};

void FlattenCore::DoFlatten(
    const std::string &fileName,
    const FileInfo &fileInfo,
    const FileInfo &snapFileInfo,
    TaskProgress *progress) {
    // 1. 计算克隆的segment数量
    uint64_t segmentSize = fileInfo.segmentsize();
    uint64_t cloneSegmentNum = fileInfo.clonelength() / segmentSize;
    uint64_t chunkNumPerSegment = segmentSize / fileInfo.chunksize();

    int ret = kMdsSuccess;
    uint32_t workingChunkNum = 0;
    auto tracker = std::make_shared<FlattenChunkTaskTracker>();
    for (uint64_t i = 0; i < cloneSegmentNum; i++) {
        progress->SetProgress(100 * i / cloneSegmentNum);

        // 2. 加载克隆的segment
        PageFileSegment segment;
        StoreStatus storeRet = storage_->GetSegment(fileInfo.id(),
            i * segmentSize, &segment);
        if (storeRet == StoreStatus::KeyNotExist) {
            // not exist, mains not clone segment
            continue;
        } else if (storeRet != StoreStatus::OK) {
            LOG(ERROR) << "load clone segment fail, file: " << fileName
                       << ", id: " << fileInfo.id()
                       << ", offset: " << i * segmentSize;
            ret = kMdsFail;
            break;
        }

        if (!segment.has_originfileid()) {
            // not clone segment
            continue;
        }

        // 3. 加载origin segment
        PageFileSegment originSegment;
        bool originSegmentExist = true;
        storeRet = storage_->GetSegment(segment.originfileid(),
            i * segmentSize, &originSegment);
        if (storeRet != StoreStatus::OK) {
            // may be origin segment not exist
            if (storeRet == StoreStatus::KeyNotExist) {
                originSegmentExist = false;
            } else {
                LOG(ERROR) << "load origin segment fail, file: " << fileName
                           << ", id: " << fileInfo.id()
                           << ", originFileId: " << segment.originfileid()
                           << ", offset: " << i * segmentSize;
                ret = kMdsFail;
                break;
            }
        }

        // 4. flatten chunk
        LogicalPoolID logicalPoolId = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            while (workingChunkNum >= option_.flattenChunkConcurrency) {
                uint32_t completeChunkNum = 0;
                ret = WaitAsycnFlattenChunkDoneAndSendNewPart(tracker, &completeChunkNum);
                workingChunkNum -= completeChunkNum;
                if (ret < 0) {
                    break;
                }
            }
            if (ret < 0) {
                break;
            }

            workingChunkNum++;
            auto context = std::make_shared<FlattenChunkContext>();
            context->logicalPoolId = logicalPoolId;
            context->copysetId = segment.chunks(j).copysetid();
            context->chunkId = segment.chunks(j).chunkid();
            context->seqNum = fileInfo.seqnum();
            context->chunkSize = fileInfo.chunksize();
            context->partIndex = 0;
            context->partSize = option_.flattenChunkPartSize;
            context->originSegmentExist = originSegmentExist;
            if (originSegmentExist) {
                context->originChunkId = originSegment.chunks(j).chunkid();
            } else {
                context->originChunkId = 0;
            }
            context->chunkIndex = chunkNumPerSegment * i + j;
            context->cloneNo = fileInfo.cloneno();
            for (int i = 0; i < fileInfo.clones_size(); i++) {
                CloneInfos cloneInfo;
                cloneInfo.cloneNo = fileInfo.clones(i).cloneno();
                cloneInfo.cloneSn = fileInfo.clones(i).clonesn();
                context->clones.emplace_back(cloneInfo);
            }
            ret = StartAsyncFlattenChunkPart(tracker, context);
            if (ret < 0) {
                break;
            }
        }
        if (ret < 0) {
            break;
        }
    }

    while (workingChunkNum > 0 && ret >= 0) {
        uint32_t completeChunkNum = 0;
        ret = WaitAsycnFlattenChunkDoneAndSendNewPart(tracker, &completeChunkNum);
        workingChunkNum -= completeChunkNum;
        if (ret < 0) {
            break;
        }
    }

    if (ret < 0) {
        // still have workingChunkNum when failed, wait all cb return
        if (workingChunkNum > 0) {
            tracker->Wait();
            tracker->PopResultContexts();
        }
        LOG(ERROR) << "flatten file fail, file: " << fileName
                   << ", id: " << fileInfo.id();
        progress->SetStatus(TaskStatus::FAILED);
    } else {
        CHECK(ret >= 0 && workingChunkNum == 0);

        std::string srcFileName = fileInfo.clonesource();
        FileSeqType seq = fileInfo.clonesn();
        FileWriteLockGuard guard(fileLockManager_, fileName, srcFileName);
        // reget FileInfo 
        FileInfo fileInfoNew;
        StoreStatus st = storage_->GetFile(
            fileInfo.parentid(), fileInfo.filename(), 
            &fileInfoNew);
        if (st != StoreStatus::OK) {
            LOG(ERROR) << "get file info fail, file: " << fileName
                       << ", id: " << fileInfo.id();
            progress->SetStatus(TaskStatus::FAILED);
            return;
        }

        // update file info to storage
        fileInfoNew.set_filestatus(FileStatus::kFileCreated);
        fileInfoNew.set_filetype(FileType::INODE_PAGEFILE);

        // reget snapFileInfo
        FileInfo snapFileInfoNew;
        st = storage_->GetSnapFile(
            snapFileInfo.parentid(), snapFileInfo.filename(),
            &snapFileInfoNew);
        if (st != StoreStatus::OK) {
            LOG(ERROR) << "flatten LookUp SnapFile srcfile: " 
                       << snapFileInfo.filename()
                       << ", failed, ret: " << st;
            // not return error, to compatibility 
            // with error scenarios
            st = storage_->PutFile(fileInfoNew);
            if (st != StoreStatus::OK) {
                LOG(ERROR) << "update file info fail, file: " << fileName
                           << ", id: " << fileInfo.id();
                progress->SetStatus(TaskStatus::FAILED);
            } else {
                LOG(INFO) << "flatten file success, file: "  << fileName
                          << ", id: "<< fileInfo.id();
                progress->SetStatus(TaskStatus::SUCCESS);
                progress->SetProgress(100);
            }
        } else {
            for (auto it = snapFileInfoNew.mutable_children()->begin(); 
                it != snapFileInfoNew.mutable_children()->end(); 
                ++it) {
                if (*it == fileName) {
                    snapFileInfoNew.mutable_children()->erase(it);
                    break;
                }
            }
            if (storage_->Put2File(fileInfoNew, snapFileInfoNew) 
                != StoreStatus::OK) {
                LOG(ERROR) << "update file info fail, file: " << fileName
                           << ", id: " << fileInfo.id();
                progress->SetStatus(TaskStatus::FAILED);
            } else {
                LOG(INFO) << "flatten file success, file: "  << fileName
                          << ", id: "<< fileInfo.id();
                progress->SetStatus(TaskStatus::SUCCESS);
                progress->SetProgress(100);
            }
        }
    }
    return;
}

int FlattenCore::StartAsyncFlattenChunkPart(
    const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
    const std::shared_ptr<FlattenChunkContext> &context) {

    std::unique_ptr<FlattenChunkClosure> cb(
        new FlattenChunkClosure(tracker, context));
    int ret = copysetClient_->FlattenChunk(context, cb.get());
    if (ret < 0) {
        LOG(ERROR) << "Start flatten chunk fail, ret: " << ret;
        return ret;
    } 
    cb.release();
    tracker->AddOneTrace();
    return ret;
}

int FlattenCore::WaitAsycnFlattenChunkDoneAndSendNewPart(
    const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
    uint32_t *completeChunkNum) {
    *completeChunkNum = 0;
    tracker->WaitSome(1);
    std::list<std::shared_ptr<FlattenChunkContext>> results = 
        tracker->PopResultContexts();
    for (auto &context : results) {
        if (context->retCode < 0) {
            LOG(ERROR) << "WaitAsycnFlattenChunkDone get result fail"
                       << ", ret: " << context->retCode;
            return context->retCode;
        } else {
            context->partIndex++;
            if (context->partIndex * option_.flattenChunkPartSize >= 
                    context->chunkSize) {
                (*completeChunkNum)++;
                continue;
            }

            if ((context->partIndex + 1) * option_.flattenChunkPartSize > 
                    context->chunkSize) {
                context->partSize = context->chunkSize - 
                    context->partIndex * option_.flattenChunkPartSize;
            } else {
                context->partSize = option_.flattenChunkPartSize;
            }
        
            int ret = StartAsyncFlattenChunkPart(tracker, context);
            if (ret < 0) {
                LOG(ERROR) << "StartAsyncFlattenChunk fail, ret: " << ret;
                return ret;
            }
        }
    }
    return kMdsSuccess;
}


}  // namespace mds
}  // namespace curve

