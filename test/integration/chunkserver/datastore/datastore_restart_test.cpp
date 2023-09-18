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
 * Created Date: Saturday January 5th 2019
 * Author: yangyaokai
 */

#include <vector>

#include "test/integration/chunkserver/datastore/datastore_integration_base.h"

namespace curve {
namespace chunkserver {

const string baseDir = "./data_int_res";    // NOLINT
const string poolDir = "./chunfilepool_int_res";  // NOLINT
const string poolMetaPath = "./chunfilepool_int_res.meta";  // NOLINT
// 以下的测试读写数据都在[0, 32kb]范围内
const uint64_t kMaxSize = 8 * PAGE_SIZE;

struct RangeData {
    char data;
    off_t offset;
    size_t length;
    RangeData() = default;
    RangeData(char ch, off_t off, size_t len)
        : data(ch)
        , offset(off)
        , length(len) {}
};

struct ExpectStatus {
    bool exist;
    CSChunkInfo chunkInfo;
    char* chunkData;
    char* snapshotData;
    ExpectStatus() : exist(false), chunkData(nullptr), snapshotData(nullptr) {}
    ~ExpectStatus() {
        if (chunkData != nullptr) {
            delete [] chunkData;
            chunkData = nullptr;
        }

        if (snapshotData != nullptr) {
            delete [] snapshotData;
            snapshotData = nullptr;
        }
    }
};

class ExecStep {
 public:
    explicit ExecStep(std::shared_ptr<CSDataStore>* datastore, ChunkID id)
        : datastore_(datastore)
        , id_(id)
        , statusAfterExec_(nullptr)
        , snapContext_(nullptr) {}

    explicit ExecStep(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
                      std::shared_ptr<SnapContext> snapContext)
        : datastore_(datastore)
        , id_(id)
        , statusAfterExec_(nullptr)
        , snapContext_(snapContext) {}
    virtual ~ExecStep() {}

    std::shared_ptr<CSDataStore> GetDataStore() {
        return (*datastore_);
    }

    ChunkID GetChunkID() {
        return id_;
    }

    std::shared_ptr<ExpectStatus> GetStatus() {
        return statusAfterExec_;
    }

    std::shared_ptr<SnapContext> GetSnapContext() {
        return snapContext_;
    }

    void ClearStatus() {
        statusAfterExec_ = nullptr;
    }

    virtual void SetExpectStatus() {
        statusAfterExec_ = std::make_shared<ExpectStatus>();
        CSChunkInfo info;
        CSErrorCode err = (*datastore_)->GetChunkInfo(id_, &info);
        if (err == CSErrorCode::Success) {
            statusAfterExec_->exist = true;
            statusAfterExec_->chunkInfo = info;
            char* chunkData = new char[kMaxSize];
            memset(chunkData, 0, kMaxSize);
            if (info.isClone) {
                uint32_t endIndex = kMaxSize / PAGE_SIZE - 1;
                std::vector<BitRange> setRanges;
                info.bitmap->Divide(0, endIndex, nullptr, &setRanges);
                for (auto &range : setRanges) {
                    off_t offset = range.beginIndex * PAGE_SIZE;
                    size_t length =
                        (range.endIndex - range.beginIndex + 1) * PAGE_SIZE;
                    (*datastore_)->ReadChunk(id_,
                                             info.curSn,
                                             (chunkData + offset),
                                             offset,
                                             length);
                }
            } else {
                (*datastore_)->ReadChunk(id_,
                                         info.curSn,
                                         chunkData,
                                         0,
                                         kMaxSize);
            }
            statusAfterExec_->chunkData = chunkData;
            // 快照存在，读取快照数据
            if (info.snapSn > 0) {
                char* snapData = new char[kMaxSize];
                (*datastore_)->ReadSnapshotChunk(
                    id_, info.snapSn, snapData, 0, kMaxSize, snapContext_);
                statusAfterExec_->snapshotData = snapData;
            }
        }  // if (err == CSErrorCode::Success)
    }

    virtual void Exec() = 0;

    virtual void Dump() = 0;

 protected:
    std::shared_ptr<CSDataStore>* datastore_;
    ChunkID id_;
    std::shared_ptr<ExpectStatus> statusAfterExec_;
    std::shared_ptr<SnapContext> snapContext_;
};

// default fileId = 1
// chunkIndex = chunkId
// snapContext needs to be set
class ExecWrite : public ExecStep {
 public:
    ExecWrite(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
              SequenceNum sn, RangeData data)
        : ExecStep(datastore, id)
        , sn_(sn)
        , data_(data)
        , snapContext_(nullptr)
        , fileId_(1)
        , cloneInfo_(nullptr) {}

    ExecWrite(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
              SequenceNum sn, RangeData data,
              std::shared_ptr<SnapContext> snapContext,
              struct CloneContext* cloneInfo = nullptr)
        : ExecStep(datastore, id, snapContext)
        , sn_(sn)
        , data_(data)
        , snapContext_(snapContext)
        , fileId_(1)
        , cloneInfo_(cloneInfo) {}

    ~ExecWrite() {}

    void Exec() override {
        char* buf = new char[data_.length];
        memset(buf, data_.data, data_.length);
        if (nullptr != cloneInfo_) {
            std::unique_ptr<CloneContext> clone(new CloneContext());
            clone->rootId = cloneInfo_->rootId;
            clone->cloneNo = cloneInfo_->cloneNo;
            clone->virtualId = cloneInfo_->virtualId;
            clone->clones = cloneInfo_->clones;
            butil::IOBuf databuf;
            databuf.append_user_data(
                const_cast<char*>(buf), data_.length, TrivialDeleter);
            (*datastore_)->WriteChunk(id_, sn_, databuf,
                                      data_.offset, data_.length,
                                      id_, fileId_, nullptr,
                                      snapContext_, clone);
        } else {
            if (snapContext_ == nullptr) {
                // if the snapContext_ is not set, use the default one
                (*datastore_)->WriteChunk(id_, sn_, buf,
                                          data_.offset, data_.length,
                                          id_, fileId_, nullptr);
            } else {
                (*datastore_)->WriteChunk(id_, sn_, buf,
                                          data_.offset, data_.length,
                                          id_, fileId_, nullptr,
                                          snapContext_);
            }
        }
    }

    void Dump() override {
        printf("WriteChunk, id = %llu, sn = %llu, offset = %llu, "
                "chunkIndex = %llu, fileId = %llu, "
                "size = %llu, data = %c.\n",
                id_, sn_, data_.offset, id_, fileId_, data_.length, data_.data);
    }

 private:
    SequenceNum sn_;
    RangeData data_;
    std::shared_ptr<SnapContext> snapContext_;
    uint64_t fileId_;
    struct CloneContext* cloneInfo_;
};

class ExecPaste : public ExecStep {
 public:
    ExecPaste(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
              RangeData data)
        : ExecStep(datastore, id)
        , data_(data) {}
    ~ExecPaste() {}

    void Exec() override {
        char* buf = new char[data_.length];
        memset(buf, data_.data, data_.length);
        (*datastore_)->PasteChunk(id_, buf, data_.offset, data_.length);
        delete [] buf;
    }

    void Dump() override {
        printf("PasteChunk, id = %llu, offset = %llu, "
                "size = %llu, data = %c.\n",
                id_, data_.offset, data_.length, data_.data);
    }

 private:
    RangeData data_;
};

class ExecDelete : public ExecStep {
 public:
    ExecDelete(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
               SequenceNum sn)
        : ExecStep(datastore, id)
        , sn_(sn)
        , snapContext_(nullptr) {}
    ExecDelete(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
               SequenceNum sn, std::shared_ptr<SnapContext> snapContext)
        : ExecStep(datastore, id, snapContext)
        , sn_(sn)
        , snapContext_(snapContext) {}
    ~ExecDelete() {}

    void Exec() override {
        if (snapContext_ == nullptr) {
            (*datastore_)->DeleteChunk(id_, sn_);
        } else {
            (*datastore_)->DeleteChunk(id_, sn_, snapContext_);
        }
    }

    void Dump() override {
        printf("DeleteChunk, id = %llu, sn = %llu.\n", id_, sn_);
    }

 private:
    SequenceNum sn_;
    std::shared_ptr<SnapContext> snapContext_;
};

class ExecDeleteSnapshot : public ExecStep {
 public:
    ExecDeleteSnapshot(std::shared_ptr<CSDataStore>* datastore,
                        ChunkID id,
                        SequenceNum correctedSn,
                        std::shared_ptr<SnapContext> snapContext)
        : ExecStep(datastore, id, snapContext)
        , correctedSn_(correctedSn)
        , snapContext_(snapContext) {}
    ~ExecDeleteSnapshot() {}

    void Exec() override {
        (*datastore_)->DeleteSnapshotChunk(id_, correctedSn_, snapContext_);
    }

    void Dump() override {
        printf("DeleteSnapshotChunk, "
               "id = %llu, correctedSn = %llu.\n", id_, correctedSn_);
    }

 private:
    SequenceNum correctedSn_;
    std::shared_ptr<SnapContext> snapContext_;
};

class ExecCreateClone : public ExecStep {
 public:
    ExecCreateClone(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
                    SequenceNum sn, SequenceNum correctedSn, ChunkSizeType size,
                    std::string location)
        : ExecStep(datastore, id)
        , sn_(sn)
        , correctedSn_(correctedSn)
        , size_(size)
        , location_(location) {}
    ~ExecCreateClone() {}

    void Exec() override {
        (*datastore_)->CreateCloneChunk(
            id_, sn_, correctedSn_, size_, location_);
    }

    void Dump() override {
        printf("CreateCloneChunk, id = %llu, sn = %llu, correctedSn = %llu, "
               "chunk size = %llu, location = %s.\n",
               id_, sn_, correctedSn_, size_, location_.c_str());
    }

 private:
    SequenceNum sn_;
    SequenceNum correctedSn_;
    ChunkSizeType size_;
    std::string location_;
};

typedef std::function<void(void)> ClearFunc;
class StepList {
 public:
    explicit StepList(ClearFunc clearFunc) : clearFunc_(clearFunc) {}
    ~StepList() {}

    void Add(std::shared_ptr<ExecStep> step) {
        steps.push_back(step);
    }

    int GetStepCount() {
        return steps.size();
    }

    void ClearEnv() {
        clearFunc_();
        // 清理每一步的预期状态，因为清理环境后，读取到的数据内容可能会不一样
        // 因为通过FilePool分配的chunk初始内容是不确定的
        for (auto &step : steps) {
            step->ClearStatus();
        }
    }

    // 重启前，用户最后执行的操作可能为任意步骤，
    // 需要验证每个步骤作为最后执行操作时，
    // 日志从该步骤前任意步骤进行恢复的幂等性
    // 对于未执行的步骤可以不必验证，只要保证已执行步骤的恢复是幂等的
    // 未执行的步骤恢复一定是幂等的
    bool VerifyLogReplay() {
        // 验证每个步骤作为最后执行操作时日志恢复的幂等性
        for (int lastStep = 0; lastStep < steps.size(); ++lastStep) {
            // 重新初始化环境
            ClearEnv();
            printf("==============Verify log replay to step%d==============\n",
                    lastStep + 1);
            // 构造重启前环境
            if (!ConstructEnv(lastStep)) {
                LOG(ERROR) << "Construct env failed.";
                Dump();
                return false;
            }
            // 验证日志恢复后的幂等性
            if (!ReplayLog(lastStep)) {
                LOG(ERROR) << "Replay log failed."
                           << "last step: step" << lastStep + 1;
                Dump();
                return false;
            }
        }
        return true;
    }

    void Dump() {
        for (int i = 0; i < steps.size(); ++i) {
            printf("**************step%d: ", i + 1);
            steps[i]->Dump();
        }
    }

 private:
    // 构造初始状态
    bool ConstructEnv(int lastStep) {
        // 模拟日志恢复前执行，用于构造初始Chunk状态，并初始化每一步的预期状态
        for (int curStep = 0; curStep <= lastStep; ++curStep) {
            std::shared_ptr<ExecStep> step = steps[curStep];
            step->Exec();
            step->SetExpectStatus();
        }
        // 检查构造出来的状态是否符合预期
        if (!CheckStatus(lastStep)) {
            LOG(ERROR) << "Check chunk status failed."
                       << "last step: step" << lastStep + 1;
            return false;
        }
        return true;
    }

    // 从最后步骤前任意一个步骤进行恢复都应该保证幂等性
    bool ReplayLog(int lastStep) {
        // 模拟从不同的起始位置进行日志恢复
        for (int beginStep = 0; beginStep <= lastStep; ++beginStep) {
            // 执行恢复前，chunk的状态保证为预期的状态
            for (int curStep = beginStep; curStep <= lastStep; ++curStep) {
                std::shared_ptr<ExecStep> step = steps[curStep];
                step->Exec();
            }
            // 每次日志恢复完成检查Chunk状态是否符合预期
            if (!CheckStatus(lastStep)) {
                LOG(ERROR) << "Check chunk status failed."
                           << "begin step: step" << beginStep + 1
                           << ", last step: step" << lastStep + 1;
                return false;
            }
        }
        return true;
    }

    bool CheckChunkData(std::shared_ptr<ExecStep> step) {
        std::shared_ptr<ExpectStatus> expectStatus = step->GetStatus();
        std::shared_ptr<CSDataStore> datastore =
            step->GetDataStore();
        ChunkID id = step->GetChunkID();
        CSChunkInfo info;
        datastore->GetChunkInfo(id, &info);

        char* actualData = new char[kMaxSize];
        memset(actualData, 0, kMaxSize);
        if (info.isClone) {
            uint32_t endIndex = kMaxSize / PAGE_SIZE - 1;
            std::vector<BitRange> setRanges;
            info.bitmap->Divide(0, endIndex, nullptr, &setRanges);
            for (auto &range : setRanges) {
                off_t offset = range.beginIndex * PAGE_SIZE;
                size_t length =
                    (range.endIndex - range.beginIndex + 1) * PAGE_SIZE;
                datastore->ReadChunk(id,
                                    info.curSn,
                                    (actualData + offset),
                                    offset,
                                    length);
            }
        } else {
            datastore->ReadChunk(id,
                                info.curSn,
                                actualData,
                                0,
                                kMaxSize);
        }

        int ret = memcmp(expectStatus->chunkData, actualData, kMaxSize);
        if (ret != 0) {
            LOG(ERROR) << "Data readed not as expect."
                       << "chunk id: " << id
                       << ", ret: " << ret;

            for (int i = 0; i < kMaxSize; ++i) {
                if (*(expectStatus->chunkData + i) != *(actualData + i)) {
                    LOG(ERROR) << "diff pos: " << i
                               << ", expect data: "
                               << *(expectStatus->chunkData + i)
                               << ", actual data: " << *(actualData + i);
                    break;
                }
            }
            delete [] actualData;
            return false;
        }
        delete [] actualData;
        return true;
    }

    bool CheckSnapData(std::shared_ptr<ExecStep> step) {
        std::shared_ptr<ExpectStatus> expectStatus = step->GetStatus();
        std::shared_ptr<CSDataStore> datastore =
            step->GetDataStore();
        ChunkID id = step->GetChunkID();
        CSChunkInfo info;
        datastore->GetChunkInfo(id, &info);

        char* actualData = new char[kMaxSize];

        CSErrorCode err;
        err = datastore->ReadSnapshotChunk(
            id, info.snapSn, actualData, 0, kMaxSize, step->GetSnapContext());
        if (err != CSErrorCode::Success) {
            LOG(ERROR) << "Read snapshot failed."
                        << "Error Code: " << err
                        << ", chunk id: " << id;
            delete [] actualData;
            return false;
        }

        if (memcmp(expectStatus->snapshotData, actualData, kMaxSize) != 0) {
            LOG(ERROR) << "Data readed not as expect."
                        << "chunk id: " << id;
            delete [] actualData;
            return false;
        }
        delete [] actualData;
        return true;
    }

    bool CheckStatus(int lastStep) {
        std::shared_ptr<ExecStep> step = steps[lastStep];
        std::shared_ptr<ExpectStatus> expectStatus = step->GetStatus();

        // 获取chunk信息
        std::shared_ptr<CSDataStore> datastore =
            step->GetDataStore();
        ChunkID id = step->GetChunkID();
        CSChunkInfo info;
        CSErrorCode err = datastore->GetChunkInfo(id, &info);

        // 返回Success说明chunk存在
        if (err == CSErrorCode::Success) {
            // 检查chunk的状态
            if (!expectStatus->exist ||
                expectStatus->chunkInfo != info) {
                LOG(ERROR) << "Chunk info is not as expected!";
                LOG(ERROR) << "Expect status("
                           << "chunk exist: " << expectStatus->exist
                           << ", sn: " << expectStatus->chunkInfo.curSn
                           << ", correctedSn: " <<  expectStatus->chunkInfo.correctedSn  // NOLINT
                           << ", snap sn: " << expectStatus->chunkInfo.snapSn
                           << ", isClone: " << expectStatus->chunkInfo.isClone
                           << ", location: " << expectStatus->chunkInfo.location
                           << ").";
                LOG(ERROR) << "Actual status("
                           << "chunk exist: " << true
                           << ", sn: " << info.curSn
                           << ", correctedSn: " <<  info.correctedSn
                           << ", isClone: " << info.isClone
                           << ", location: " << info.location
                           << ").";
                return false;
            }

            // 检查chunk的数据状态
            if (!CheckChunkData(step))
                return false;

            // 检查快照状态
            if (info.snapSn > 0) {
                // 检查快照的数据状态
                if (!CheckSnapData(step))
                    return false;
            }
        } else if (err == CSErrorCode::ChunkNotExistError) {
            // 预期chunk存在，实际却不存在
            if (expectStatus->exist) {
                LOG(ERROR) << "Chunk is expected to exist, but actual not.";
                return false;
            }
        } else {
            LOG(ERROR) << "Get chunk info failed."
                       << "chunk id: " << id
                       << ", error code: " << err;
            return false;
        }
        return true;
    }

 private:
    std::vector<std::shared_ptr<ExecStep>> steps;
    ClearFunc clearFunc_;
};

class RestartTestSuit : public DatastoreIntegrationBase {
 public:
    RestartTestSuit() {}
    ~RestartTestSuit() {}

    void ClearEnv() {
        TearDown();
        SetUp();
    }

 protected:
    ClearFunc clearFunc = std::bind(&RestartTestSuit::ClearEnv, this);
};

TEST_F(RestartTestSuit, BasicTest) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 1;

    // 第一步：WriteChunk,写[0, 8kb]区域
    RangeData step1Data;
    step1Data.offset = 0;
    step1Data.length = 2 * PAGE_SIZE;
    step1Data.data = '1';
    std::shared_ptr<ExecWrite> step1 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step1Data);
    list.Add(step1);

    // 第二步：WriteChunk,写[4kb, 12kb]区域
    RangeData step2Data;
    step2Data.offset = PAGE_SIZE;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step2Data);
    list.Add(step2);

    // 第三步：DeleteChunk
    std::shared_ptr<ExecDelete> step3 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step3);

    ASSERT_TRUE(list.VerifyLogReplay());
}

TEST_F(RestartTestSuit, SnapshotTest) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 1;
    std::vector<SequenceNum> snaps;
    snaps.clear();
    std::shared_ptr<SnapContext> context = nullptr;

    // 第一步：WriteChunk,写[0, 8kb]区域
    RangeData step1Data;
    step1Data.offset = 0;
    step1Data.length = 2 * PAGE_SIZE;
    step1Data.data = '1';
    std::shared_ptr<ExecWrite> step1 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step1Data);
    list.Add(step1);

    // 模拟用户打了快照，此时sn +1
    snaps.push_back(sn);
    ++sn;

    // 第二步：WriteChunk,写[4kb, 12kb]区域
    context = std::make_shared<SnapContext>(snaps);

    RangeData step2Data;
    step2Data.offset = PAGE_SIZE;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step2Data, context);
    list.Add(step2);

    // 第三步：用户请求删除快照
    uint64_t lastSn = snaps.back();
    snaps.pop_back();
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDeleteSnapshot> step3 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, lastSn, context);
    list.Add(step3);

    // 模拟再次打快照 sn +1
    snaps.push_back(sn);
    ++sn;

    // 第四步：此次快照过程中没有数据写入，直接DeleteSnapshotOrCorrectedSn
    lastSn = snaps.back();
    snaps.pop_back();
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDeleteSnapshot> step4 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, lastSn, context);
    list.Add(step4);

    // 第五步：WriteChunk，写[8kb, 16kb]区域
    RangeData step5Data;
    step5Data.offset = 2 * PAGE_SIZE;
    step5Data.length = 2 * PAGE_SIZE;
    step5Data.data = '5';
    std::shared_ptr<ExecWrite> step5 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step5Data, context);
    list.Add(step5);

    // 模拟再次打快照 sn +1
    snaps.push_back(sn);
    ++sn;

    // 第六步：WriteChunk，写[4kb, 12kb]区域
    context = std::make_shared<SnapContext>(snaps);
    RangeData step6Data;
    step6Data.offset = PAGE_SIZE;
    step6Data.length = 2 * PAGE_SIZE;
    step6Data.data = '6';
    std::shared_ptr<ExecWrite> step6 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step6Data, context);
    list.Add(step6);

    // 第七步：用户请求删除快照
    lastSn = snaps.back();
    snaps.pop_back();
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDeleteSnapshot> step7 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, lastSn, context);
    list.Add(step7);

    // 模拟再次打快照 sn +1
    snaps.push_back(sn);
    ++sn;

    // 第八步：用户请求删除快照
    lastSn = snaps.back();
    snaps.pop_back();
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDeleteSnapshot> step8 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, lastSn, context);
    list.Add(step8);

    // 第九步：用户请求删除chunk
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDelete> step9 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn, context);
    list.Add(step9);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// 测试克隆场景，以及克隆后打快照的组合场景
TEST_F(RestartTestSuit, CloneTest) {
    StepList list(clearFunc);

    // build origin chunk, which is the clone source,
    // does not exist, and snapshot 1, also not exist
    // the fileId = 1 and originfileId = 1
    // chunkIndex = id
    // the clone fileId/cloneno = 2

    uint64_t fileId = 2;
    uint64_t originFileId = 1;
    ChunkID id = 1;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    // std::string location("test@s3");
    // no need location in clone chunk
    std::string location("");

    // build the cloneInfo for write clone chunk
    struct CloneContext cloneInfo;
    struct CloneInfos infos;
    infos.cloneNo = fileId;
    infos.cloneSn = 1;

    cloneInfo.rootId = originFileId;
    cloneInfo.cloneNo = fileId;
    cloneInfo.virtualId = id;
    cloneInfo.clones.clear();
    cloneInfo.clones.push_back(infos);

    std::vector<SequenceNum> snaps;
    snaps.clear();
    std::shared_ptr<SnapContext> context = nullptr;

    id = 2;  // change the id to clone chunk id

    // 第二步：WriteChunk，写[0kb, 8kb]区域
    RangeData step2Data;
    step2Data.offset = 0;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step2Data, nullptr, &cloneInfo);
    list.Add(step2);

    // 第三步：WriteChunk，写[4kb, 12kb]区域
    RangeData step3Data;
    step3Data.offset = PAGE_SIZE;
    step3Data.length = 2 * PAGE_SIZE;
    step3Data.data = '3';
    std::shared_ptr<ExecWrite> step3 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step3Data, nullptr, &cloneInfo);
    list.Add(step3);

    // 第四步：通过WriteChunk 遍写chunk
    RangeData step4Data;
    step4Data.offset = 0;
    step4Data.length = CHUNK_SIZE;
    step4Data.data = '4';
    std::shared_ptr<ExecWrite> step4 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step4Data, nullptr, &cloneInfo);
    list.Add(step4);

    // 模拟打快照
    snaps.push_back(sn);
    ++sn;

    // 第五步：WriteChunk，写[4kb, 12kb]区域
    RangeData step5Data;
    step5Data.offset = PAGE_SIZE;
    step5Data.length = 2 * PAGE_SIZE;
    step5Data.data = '5';
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecWrite> step5 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step5Data, context, &cloneInfo);
    list.Add(step5);

    // 第六步：用户请求删除快照
    uint64_t lastSn = snaps.back();
    snaps.pop_back();
    context = std::make_shared<SnapContext>(snaps);
    std::shared_ptr<ExecDeleteSnapshot> step6 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, lastSn, context);
    list.Add(step6);

    // 第七步：DeleteChunk
    std::shared_ptr<ExecDelete> step7 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn, context);
    list.Add(step7);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// 测试恢复场景
TEST_F(RestartTestSuit, RecoverTest) {
    StepList list(clearFunc);

    // build origin chunk, which is the clone source,
    // does not exist, and snapshot 1, also not exist
    // the fileId = 1 and originfileId = 1
    // chunkIndex = id
    // the clone fileId/cloneno = 2

    uint64_t fileId = 2;
    uint64_t originFileId = 1;
    ChunkID id = 1;
    SequenceNum sn = 3;
    SequenceNum correctedSn = 0;
    // std::string location("test@s3");
    // no need location in clone chunk
    std::string location("");

    // build the cloneInfo for write clone chunk
    struct CloneContext cloneInfo;
    struct CloneInfos infos;
    infos.cloneNo = fileId;
    infos.cloneSn = 1;

    cloneInfo.rootId = originFileId;
    cloneInfo.cloneNo = fileId;
    cloneInfo.virtualId = id;
    cloneInfo.clones.clear();
    cloneInfo.clones.push_back(infos);

    std::vector<SequenceNum> snaps;
    snaps.clear();
    std::shared_ptr<SnapContext> context = nullptr;

#if 0
    // 第一步：通过CreateCloneChunk创建clone chunk
    std::shared_ptr<ExecCreateClone> step1 =
        std::make_shared<ExecCreateClone>(&dataStore_,
                                          id,
                                          sn,
                                          correctedSn,
                                          CHUNK_SIZE,
                                          location);
    list.Add(step1);
#endif
    // 数据写入的版本应为最新的版本
    sn = 5;
    id = 2;  // change the id to clone chunk id

    // 第二步：PasteChunk，写[0kb, 8kb]区域
    RangeData step2Data;
    step2Data.offset = 0;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step2Data, nullptr, &cloneInfo);
    list.Add(step2);

    // 第三步：PasteChunk，写[4kb, 12kb]区域
    RangeData step3Data;
    step3Data.offset = PAGE_SIZE;
    step3Data.length = 2 * PAGE_SIZE;
    step3Data.data = '3';
    std::shared_ptr<ExecWrite> step3 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step3Data, nullptr, &cloneInfo);
    list.Add(step3);

    // 第四步：通过PasteChunk 遍写chunk
    RangeData step4Data;
    step4Data.offset = 0;
    step4Data.length = CHUNK_SIZE;
    step4Data.data = '4';
    std::shared_ptr<ExecWrite> step4 =
        std::make_shared<ExecWrite>(
            &dataStore_, id, sn, step4Data, nullptr, &cloneInfo);
    list.Add(step4);

    // 第五步：DeleteChunk
    std::shared_ptr<ExecDelete> step5 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step5);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// 按照实际用户使用从场景随机产生每一步的操作，校验一定操作个数下都能保证幂等性
TEST_F(RestartTestSuit, RandomCombine) {
    StepList list(clearFunc);

    // build origin chunk, which is the clone source,
    // does not exist, and snapshot 1, also not exist
    // the fileId = 1 and originfileId = 1
    // chunkIndex = id
    // the clone fileId/cloneno = 2

    uint64_t fileId = 2;
    uint64_t originFileId = 1;
    ChunkID id = 1;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    // std::string location("test@s3");
    // no need location in clone chunk
    std::string location("");

    // build the cloneInfo for write clone chunk
    struct CloneContext cloneInfo;
    struct CloneInfos infos;
    infos.cloneNo = fileId;
    infos.cloneSn = 1;

    cloneInfo.rootId = originFileId;
    cloneInfo.cloneNo = fileId;
    cloneInfo.virtualId = id;
    cloneInfo.clones.clear();
    cloneInfo.clones.push_back(infos);

    std::vector<SequenceNum> snaps;
    snaps.clear();
    std::shared_ptr<SnapContext> context = nullptr;
    context = SnapContext::build_empty();

    std::srand(std::time(nullptr));

    id = 2;  // change the id to clone chunk id

    // 写随机地址的数据,在[0, kMaxSize]范围内写
    auto randWriteOrPaste = [&](bool isPaste) {
        int pageCount = kMaxSize / PAGE_SIZE;
        RangeData stepData;
        stepData.offset = std::rand() % (pageCount - 2) * PAGE_SIZE;
        stepData.length = 2 * PAGE_SIZE;
        stepData.data = std::rand() % 256;
        if (isPaste) {
            std::cout << "ExecWrite "
                       << ", id = " << id
                       << ", sn = " << sn
                       << ", offset = " << stepData.offset
                       << ", length = " << stepData.length
                       << ", context = " << context
                       << ", step count = " << list.GetStepCount()
                       << std::endl;

            std::shared_ptr<ExecWrite> step =
                std::make_shared<ExecWrite>(
                    &dataStore_, id, sn, stepData, context, &cloneInfo);
            list.Add(step);
        } else {
            std::cout << "ExecWrite "
                       << ", id = " << id
                       << ", sn = " << sn
                       << ", offset = " << stepData.offset
                       << ", length = " << stepData.length
                       << ", context = " << context
                       << ", step count = " << list.GetStepCount()
                       << std::endl;

            std::shared_ptr<ExecWrite> step =
                std::make_shared<ExecWrite>(
                    &dataStore_, id, sn, stepData, context, &cloneInfo);
            list.Add(step);
        }
    };

    // 随机的克隆过程
    auto randClone = [&]() {
        // 二分之一概率，模拟恢复过程
        // build the clone context for the clone chunk
#if 0
        if (std::rand() % 2 == 0)
            correctedSn = 2;
        std::shared_ptr<ExecCreateClone> createStep =
            std::make_shared<ExecCreateClone>(&dataStore_,
                                              id,
                                              sn,
                                              correctedSn,
                                              CHUNK_SIZE,
                                              location);
        list.Add(createStep);
#endif
        // 克隆过程模拟5个操作，Write或者Paste，三分之一概率Write
        for (int i = 0; i < 5; ++i) {
            if (std::rand() % 3 == 0) {
                randWriteOrPaste(false);
            } else {
                randWriteOrPaste(true);
            }
        }

        // 遍写一遍chunk，可以用于模拟后续写入创建快照
        RangeData pasteData;
        pasteData.offset = 0;
        pasteData.length = CHUNK_SIZE;
        pasteData.data = 'x';
        std::shared_ptr<ExecWrite> pasteStep =
            std::make_shared<ExecWrite>(
                &dataStore_, id, sn, pasteData, context, &cloneInfo);
        list.Add(pasteStep);
    };

    // 随机的快照过程
    auto randSnapshot = [&](int* stepCount) {
        // 快照需要将版本+1
        snaps.push_back(sn);
        ++sn;
        // 三分之一的概率调DeleteSnapshot，一旦调了DeleteSnapshot就退出快照
        while (true) {
            if (std::rand() % 3 == 0) {
                uint64_t lastSn = snaps.back();
                snaps.pop_back();
                context = std::make_shared<SnapContext>(snaps);
                std::shared_ptr<ExecDeleteSnapshot> step =
                    std::make_shared<ExecDeleteSnapshot>(
                        &dataStore_, id, lastSn, context);
                list.Add(step);
                break;
            } else {
                context = std::make_shared<SnapContext>(snaps);
                randWriteOrPaste(false);
            }
            ++(*stepCount);
        }
    };

    // 创建clone chunk，
    randClone();

    // 设置最长执行步数
    int maxSteps = 30;
    int stepCount = 0;
    while (stepCount < maxSteps) {
        // 三分之一的概率会模拟快照过程
        if (std::rand() % 3 == 0) {
            randSnapshot(&stepCount);
        } else {
            randWriteOrPaste(false);
            ++stepCount;
        }
    }

    // 最后删除chunk
    std::shared_ptr<ExecDelete> lastStep =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(lastStep);
    list.Dump();
    ASSERT_TRUE(list.VerifyLogReplay());
}

}  // namespace chunkserver
}  // namespace curve
