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
// The following test read and write data are within the range of [0, 32kb]
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
        , statusAfterExec_(nullptr) {}
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
            // Snapshot exists, reading snapshot data
            if (info.snapSn > 0) {
                char* snapData = new char[kMaxSize];
                (*datastore_)->ReadSnapshotChunk(
                    id_, info.snapSn, snapData, 0, kMaxSize);
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
};

class ExecWrite : public ExecStep {
 public:
    ExecWrite(std::shared_ptr<CSDataStore>* datastore, ChunkID id,
              SequenceNum sn, RangeData data)
        : ExecStep(datastore, id)
        , sn_(sn)
        , data_(data) {}
    ~ExecWrite() {}

    void Exec() override {
        char* buf = new char[data_.length];
        memset(buf, data_.data, data_.length);

        (*datastore_)->WriteChunk(id_, sn_, buf,
                                  data_.offset, data_.length, nullptr);
    }

    void Dump() override {
        printf("WriteChunk, id = %lu, sn = %lu, offset = %lu, "
                "size = %lu, data = %c.\n",
                id_, sn_, data_.offset, data_.length, data_.data);
    }

 private:
    SequenceNum sn_;
    RangeData data_;
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
        printf("PasteChunk, id = %lu, offset = %lu, "
                "size = %lu, data = %c.\n",
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
        , sn_(sn) {}
    ~ExecDelete() {}

    void Exec() override {
        (*datastore_)->DeleteChunk(id_, sn_);
    }

    void Dump() override {
        printf("DeleteChunk, id = %lu, sn = %lu.\n", id_, sn_);
    }

 private:
    SequenceNum sn_;
};

class ExecDeleteSnapshot : public ExecStep {
 public:
    ExecDeleteSnapshot(std::shared_ptr<CSDataStore>* datastore,
                        ChunkID id,
                        SequenceNum correctedSn)
        : ExecStep(datastore, id)
        , correctedSn_(correctedSn) {}
    ~ExecDeleteSnapshot() {}

    void Exec() override {
        (*datastore_)->DeleteSnapshotChunkOrCorrectSn(id_, correctedSn_);
    }

    void Dump() override {
        printf("DeleteSnapshotChunkOrCorrectSn, "
               "id = %lu, correctedSn = %lu.\n", id_, correctedSn_);
    }

 private:
    SequenceNum correctedSn_;
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
        printf("CreateCloneChunk, id = %lu, sn = %lu, correctedSn = %lu, "
               "chunk size = %u, location = %s.\n",
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
        // Clean up the expected state of each step, as the data content read after cleaning up the environment may differ
        // Because the initial content of the chunk allocated through FilePool is uncertain
        for (auto &step : steps) {
            step->ClearStatus();
        }
    }

    // Before restarting, the last action performed by the user may be any step,
    // It is necessary to verify the idempotence of the log recovery from any step before each step as the final execution operation
    // For steps that have not been executed, there is no need to verify as long as the recovery of the executed steps is idempotent
    // Unexecuted step recovery must be idempotent
    bool VerifyLogReplay() {
        // Verify the idempotence of log recovery at each step as the final operation
        for (int lastStep = 0; lastStep < steps.size(); ++lastStep) {
            // Reinitialize the environment
            ClearEnv();
            printf("==============Verify log replay to step%d==============\n",
                    lastStep + 1);
            // Construct a pre restart environment
            if (!ConstructEnv(lastStep)) {
                LOG(ERROR) << "Construct env failed.";
                Dump();
                return false;
            }
            // Verify the idempotence of log recovery
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
    // Construction initial state
    bool ConstructEnv(int lastStep) {
        // Execute before simulating log recovery to construct the initial Chunk state and initialize the expected state for each step
        for (int curStep = 0; curStep <= lastStep; ++curStep) {
            std::shared_ptr<ExecStep> step = steps[curStep];
            step->Exec();
            step->SetExpectStatus();
        }
        // Check if the constructed state meets expectations
        if (!CheckStatus(lastStep)) {
            LOG(ERROR) << "Check chunk status failed."
                       << "last step: step" << lastStep + 1;
            return false;
        }
        return true;
    }

    // Restoring from any step before the final step should ensure idempotence
    bool ReplayLog(int lastStep) {
        // Simulate log recovery from different starting locations
        for (int beginStep = 0; beginStep <= lastStep; ++beginStep) {
            // Before performing the recovery, the state of the chunk is guaranteed to be the expected state
            for (int curStep = beginStep; curStep <= lastStep; ++curStep) {
                std::shared_ptr<ExecStep> step = steps[curStep];
                step->Exec();
            }
            // Check if the Chunk status meets expectations after each log recovery is completed
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
            id, info.snapSn, actualData, 0, kMaxSize);
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

        // Obtain chunk information
        std::shared_ptr<CSDataStore> datastore =
            step->GetDataStore();
        ChunkID id = step->GetChunkID();
        CSChunkInfo info;
        CSErrorCode err = datastore->GetChunkInfo(id, &info);

        // Returning Success indicates that the chunk exists
        if (err == CSErrorCode::Success) {
            // Check the status of the chunk
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

            // Check the data status of the chunk
            if (!CheckChunkData(step))
                return false;

            // Check snapshot status
            if (info.snapSn > 0) {
                // Check the data status of the snapshot
                if (!CheckSnapData(step))
                    return false;
            }
        } else if (err == CSErrorCode::ChunkNotExistError) {
            // The expected chunk exists, but it does not actually exist
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

    // Step 1: WriteChunk, write the [0, 8kb] area
    RangeData step1Data;
    step1Data.offset = 0;
    step1Data.length = 2 * PAGE_SIZE;
    step1Data.data = '1';
    std::shared_ptr<ExecWrite> step1 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step1Data);
    list.Add(step1);

    // Step 2: WriteChunk, write the [4kb, 12kb] area
    RangeData step2Data;
    step2Data.offset = PAGE_SIZE;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step2Data);
    list.Add(step2);

    // Step 3: DeleteChunk
    std::shared_ptr<ExecDelete> step3 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step3);

    ASSERT_TRUE(list.VerifyLogReplay());
}

TEST_F(RestartTestSuit, SnapshotTest) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 1;

    // Step 1: WriteChunk, write the [0, 8kb] area
    RangeData step1Data;
    step1Data.offset = 0;
    step1Data.length = 2 * PAGE_SIZE;
    step1Data.data = '1';
    std::shared_ptr<ExecWrite> step1 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step1Data);
    list.Add(step1);

    // Simulated user took a snapshot, at which point sn+1
    ++sn;

    // Step 2: WriteChunk, write the [4kb, 12kb] area
    RangeData step2Data;
    step2Data.offset = PAGE_SIZE;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step2Data);
    list.Add(step2);

    // Step 3: User requests to delete the snapshot
    std::shared_ptr<ExecDeleteSnapshot> step3 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
    list.Add(step3);

    // Simulate taking a snapshot again sn+1
    ++sn;

    // Step 4: No data was written during this snapshot process, directly delete SnapshotOrCorrectedSn
    std::shared_ptr<ExecDeleteSnapshot> step4 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
    list.Add(step4);

    // Step 5: WriteChunk, write the [8kb, 16kb] area
    RangeData step5Data;
    step5Data.offset = 2 * PAGE_SIZE;
    step5Data.length = 2 * PAGE_SIZE;
    step5Data.data = '5';
    std::shared_ptr<ExecWrite> step5 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step5Data);
    list.Add(step5);

    // Simulate taking a snapshot again sn+1
    ++sn;

    // Step 6: WriteChunk, write the [4kb, 12kb] area
    RangeData step6Data;
    step6Data.offset = PAGE_SIZE;
    step6Data.length = 2 * PAGE_SIZE;
    step6Data.data = '6';
    std::shared_ptr<ExecWrite> step6 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step6Data);
    list.Add(step6);

    // Step 7: User requests to delete the snapshot
    std::shared_ptr<ExecDeleteSnapshot> step7 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
    list.Add(step7);

    // Simulate taking a snapshot again sn+1
    ++sn;

    // Step 8: User requests to delete the snapshot
    std::shared_ptr<ExecDeleteSnapshot> step8 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
    list.Add(step8);

    // Step 9: User requests to delete chunk
    std::shared_ptr<ExecDelete> step9 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step9);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// Test the cloning scenario and the combination scenario of taking a snapshot after cloning
TEST_F(RestartTestSuit, CloneTest) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    std::string location("test@s3");

    // Step 1: Create a clone chunk through CreateCloneChunk
    std::shared_ptr<ExecCreateClone> step1 =
        std::make_shared<ExecCreateClone>(&dataStore_,
                                          id,
                                          sn,
                                          correctedSn,
                                          CHUNK_SIZE,
                                          location);
    list.Add(step1);

    // Step 2: WriteChunk, write the [0kb, 8kb] area
    RangeData step2Data;
    step2Data.offset = 0;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecWrite> step2 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step2Data);
    list.Add(step2);

    // Step 3: PasteChunk, write the [4kb, 12kb] area
    RangeData step3Data;
    step3Data.offset = PAGE_SIZE;
    step3Data.length = 2 * PAGE_SIZE;
    step3Data.data = '3';
    std::shared_ptr<ExecPaste> step3 =
        std::make_shared<ExecPaste>(&dataStore_, id, step3Data);
    list.Add(step3);

    // Step 4: Write the chunk through PasteChunk
    RangeData step4Data;
    step4Data.offset = 0;
    step4Data.length = CHUNK_SIZE;
    step4Data.data = '4';
    std::shared_ptr<ExecPaste> step4 =
        std::make_shared<ExecPaste>(&dataStore_, id, step4Data);
    list.Add(step4);

    // Simulate taking a snapshot
    ++sn;

    // Step 5: WriteChunk, write the [4kb, 12kb] area
    RangeData step5Data;
    step5Data.offset = PAGE_SIZE;
    step5Data.length = 2 * PAGE_SIZE;
    step5Data.data = '5';
    std::shared_ptr<ExecWrite> step5 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step5Data);
    list.Add(step5);

    // Step 6: User requests to delete the snapshot
    std::shared_ptr<ExecDeleteSnapshot> step6 =
        std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
    list.Add(step6);

    // Step 7: DeleteChunk
    std::shared_ptr<ExecDelete> step7 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step7);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// Testing Recovery Scenarios
TEST_F(RestartTestSuit, RecoverTest) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 3;
    SequenceNum correctedSn = 5;
    std::string location("test@s3");

    // Step 1: Create a clone chunk through CreateCloneChunk
    std::shared_ptr<ExecCreateClone> step1 =
        std::make_shared<ExecCreateClone>(&dataStore_,
                                          id,
                                          sn,
                                          correctedSn,
                                          CHUNK_SIZE,
                                          location);
    list.Add(step1);

    // The version of data writing should be the latest version
    sn = correctedSn;

    // Step 2: PasteChunk, write the [0kb, 8kb] area
    RangeData step2Data;
    step2Data.offset = 0;
    step2Data.length = 2 * PAGE_SIZE;
    step2Data.data = '2';
    std::shared_ptr<ExecPaste> step2 =
        std::make_shared<ExecPaste>(&dataStore_, id, step2Data);
    list.Add(step2);

    // Step 3: PasteChunk, write the [4kb, 12kb] area
    RangeData step3Data;
    step3Data.offset = PAGE_SIZE;
    step3Data.length = 2 * PAGE_SIZE;
    step3Data.data = '3';
    std::shared_ptr<ExecWrite> step3 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step3Data);
    list.Add(step3);

    // Step 4: Write the chunk through PasteChunk
    RangeData step4Data;
    step4Data.offset = 0;
    step4Data.length = CHUNK_SIZE;
    step4Data.data = '4';
    std::shared_ptr<ExecWrite> step4 =
        std::make_shared<ExecWrite>(&dataStore_, id, sn, step4Data);
    list.Add(step4);

    // Step 5: DeleteChunk
    std::shared_ptr<ExecDelete> step5 =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(step5);

    ASSERT_TRUE(list.VerifyLogReplay());
}

// Randomly generate each step of the operation from the scene based on actual user usage, and verify that a certain number of operations can ensure idempotence
TEST_F(RestartTestSuit, RandomCombine) {
    StepList list(clearFunc);

    ChunkID id = 1;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    std::string location("test@s3");
    std::srand(std::time(nullptr));

    // Write random address data within the range of [0, kMaxSize]
    auto randWriteOrPaste = [&](bool isPaste) {
        int pageCount = kMaxSize / PAGE_SIZE;
        RangeData stepData;
        stepData.offset = std::rand() % (pageCount - 2) * PAGE_SIZE;
        stepData.length = 2 * PAGE_SIZE;
        stepData.data = std::rand() % 256;
        if (isPaste) {
            std::shared_ptr<ExecPaste> step =
                std::make_shared<ExecPaste>(&dataStore_, id, stepData);
            list.Add(step);
        } else {
            std::shared_ptr<ExecWrite> step =
                std::make_shared<ExecWrite>(&dataStore_, id, sn, stepData);
            list.Add(step);
        }
    };

    // Random cloning process
    auto randClone = [&]() {
        //Half probability, simulating the recovery process
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

        // The cloning process simulates 5 operations, Write or Paste, with a one-third probability of Write
        for (int i = 0; i < 5; ++i) {
            if (std::rand() % 3 == 0) {
                randWriteOrPaste(false);
            } else {
                randWriteOrPaste(true);
            }
        }

        // Write the chunk over and over again, which can be used to simulate subsequent writes and create snapshots
        RangeData pasteData;
        pasteData.offset = 0;
        pasteData.length = CHUNK_SIZE;
        pasteData.data = 'x';
        std::shared_ptr<ExecPaste> pasteStep =
            std::make_shared<ExecPaste>(&dataStore_, id, pasteData);
        list.Add(pasteStep);
    };

    // Random snapshot process
    auto randSnapshot = [&](int* stepCount) {
        // Snapshots require version+1
        ++sn;
        // One third of the probability is to call DeleteSnapshot, and once DeleteSnapshot is called, it exits the snapshot
        while (true) {
            if (std::rand() % 3 == 0) {
                std::shared_ptr<ExecDeleteSnapshot> step =
                    std::make_shared<ExecDeleteSnapshot>(&dataStore_, id, sn);
                list.Add(step);
                break;
            } else {
                randWriteOrPaste(false);
            }
            ++(*stepCount);
        }
    };

    // Create a clone chunk
    randClone();

    // Set the maximum number of execution steps
    int maxSteps = 30;
    int stepCount = 0;
    while (stepCount < maxSteps) {
        // One-third of the probability will simulate the snapshot process
        if (std::rand() % 3 == 0) {
            randSnapshot(&stepCount);
        } else {
            randWriteOrPaste(false);
            ++stepCount;
        }
    }

    // Finally, delete the chunk
    std::shared_ptr<ExecDelete> lastStep =
        std::make_shared<ExecDelete>(&dataStore_, id, sn);
    list.Add(lastStep);
    list.Dump();
    ASSERT_TRUE(list.VerifyLogReplay());
}

}  // namespace chunkserver
}  // namespace curve
