/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2021-05-25
 * Author: Jingli Chen (Wine93)
 */

#include "test/mds/schedule/common.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "src/common/timeutility.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"
#include "src/mds/schedule/scheduleMetrics.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::SetArgPointee;
using ::curve::common::TimeUtility;
using ::curve::mds::topology::MockTopology;
using ::curve::mds::topology::LogicalPool;

namespace curve {
namespace mds {
namespace schedule {

class TestScanSchedule : public ::testing::Test {
 protected:
    using GetLogicalPoolCb = std::function<bool(PoolIdType, LogicalPool*)>;

    void SetUp() override {
        // topoAdapter_
        topoAdapter_ = std::make_shared<MockTopoAdapter>();

        // opController_
        auto topo = std::make_shared<MockTopology>();
        auto metric = std::make_shared<ScheduleMetrics>(topo);
        opController_ = std::make_shared<OperatorController>(2, metric);

        // scanScheduler_
        opt_.scanSchedulerIntervalSec = 1;
        opt_.scanPeerTimeLimitSec = 10;
        opt_.scanStartHour = 0;
        opt_.scanEndHour = 23;
        opt_.scanIntervalSec = 86400;
        opt_.scanConcurrentPerPool = 2;
        opt_.scanConcurrentPerChunkserver = 1;
        scanScheduler_ = std::make_shared<ScanScheduler>(
            opt_, topoAdapter_, opController_);
    }

    void ReInitScheduler() {
        scanScheduler_ = std::make_shared<ScanScheduler>(
            opt_, topoAdapter_, opController_);
    }

    void InsertOperator(CopySetInfo copysetInfo, bool start) {
        auto opType = start ? ConfigChangeType::START_SCAN_PEER
                            : ConfigChangeType::CANCEL_SCAN_PEER;
        auto priority = start ? OperatorPriority::LowPriority
                              : OperatorPriority::HighPriority;
        auto op = operatorFactory.CreateScanPeerOperator(
            copysetInfo, copysetInfo.leader, priority, opType);

        ASSERT_TRUE(opController_->AddOperator(op));
    }

    void CheckOperators(size_t n, int nStartExpect, int nCancelExpect) {
        auto nStart = 0, nCancel = 0;
        auto operators = opController_->GetOperators();
        ASSERT_EQ(operators.size(), n);

        for (auto& op : operators) {
            auto step = dynamic_cast<ScanPeer*>(op.step.get());
            ASSERT_TRUE(nullptr != step);
            if (step->IsStartScan()) {
                nStart += 1;
            } else {
                nCancel += 1;
            }
        }

        ASSERT_EQ(nStart, nStartExpect);
        ASSERT_EQ(nCancel, nCancelExpect);
    }

    GetLogicalPoolCb GenGetLogicalPoolCb(bool scanEnable) {
        return [scanEnable](PoolIdType lpid, LogicalPool* lpool) -> bool {
            lpool->SetScanEnable(scanEnable);
            return true;
        };
    }

    static std::vector<PoolIdType> GetLogicalpools() {
        return std::vector<PoolIdType>{ 1, 2, 3 };
    }

    static std::vector<CopySetInfo> GetCopySetInfosInLogicalPool(
        PoolIdType lpid) {
        std::vector<CopySetInfo> copysetInfos;

        // chunkserver ids
        using csId = std::vector<ChunkServerIdType>;
        auto csIds = std::vector<csId>{
            csId{ 1, 2, 3 },
            csId{ 4, 5, 6 },
            csId{ 1, 4, 7 },
        };

        for (auto i = 0; i < 3; i++) {
            std::vector<PeerInfo> peers({
                PeerInfo(csIds[i][0], 1, 1, "0.0.0.0", 9000),
                PeerInfo(csIds[i][1], 1, 1, "0.0.0.0", 9000),
                PeerInfo(csIds[i][2], 1, 1, "0.0.0.0", 9000),
            });

            auto copysetInfo = ::curve::mds::schedule::CopySetInfo(
                CopySetKey(lpid, i + 1),  // (logical pool id, copyset id)
                1,                        // epoch
                1,                        // leader (chunkserver id)
                peers,                    // peers
                ConfigChangeInfo{},
                CopysetStatistics{});
            copysetInfos.push_back(copysetInfo);
        }

        return copysetInfos;
    }

 protected:
    ScheduleOption opt_;
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<ScanScheduler> scanScheduler_;
};

TEST_F(TestScanSchedule, TestScanBasic) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(3)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    scanScheduler_->Schedule();
    CheckOperators(6, 6, 0);
}

TEST_F(TestScanSchedule, TestNotDuringScanTime) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(3)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    opt_.scanStartHour = TimeUtility::GetCurrentHour() + 1;
    ReInitScheduler();
    scanScheduler_->Schedule();
    CheckOperators(0, 0, 0);
}

TEST_F(TestScanSchedule, TestCancelScanForNotDuringScanTime) {
    auto copysetInfos1 = GetCopySetInfosInLogicalPool(1);
    copysetInfos1[0].scaning = true;
    auto copysetInfos2 = GetCopySetInfosInLogicalPool(2);
    copysetInfos2[0].scaning = true;
    auto copysetInfos3 = GetCopySetInfosInLogicalPool(3);
    copysetInfos3[0].scaning = true;

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos1))
        .WillOnce(Return(copysetInfos2))
        .WillOnce(Return(copysetInfos3));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    opt_.scanStartHour = TimeUtility::GetCurrentHour() + 1;
    ReInitScheduler();
    scanScheduler_->Schedule();
    CheckOperators(3, 0, 3);
}

TEST_F(TestScanSchedule, TestDisableScanForLogicalPool) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(3)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillOnce(Invoke(GenGetLogicalPoolCb(false)))
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    scanScheduler_->Schedule();
    CheckOperators(4, 4, 0);  // 2 operators per logical pool
}

TEST_F(TestScanSchedule, TestCancelScanWhenDisableScan) {
    auto copysetInfos = GetCopySetInfosInLogicalPool(1);
    for (auto i = 0; i < 3; i++) {
        copysetInfos[i].scaning = true;
    }

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos))
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillOnce(Invoke(GenGetLogicalPoolCb(false)))
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    scanScheduler_->Schedule();
    CheckOperators(7, 4, 3);
}

TEST_F(TestScanSchedule, TestScanConcurrentPerPool) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(3)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    opt_.scanConcurrentPerPool = 3;
    opt_.scanConcurrentPerChunkserver = 1;
    ReInitScheduler();
    scanScheduler_->Schedule();
    CheckOperators(6, 6, 0);
}

TEST_F(TestScanSchedule, TestScanConcurrentPerChunkserver) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(3)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    opt_.scanConcurrentPerPool = 3;
    opt_.scanConcurrentPerChunkserver = 2;
    ReInitScheduler();
    scanScheduler_->Schedule();
    CheckOperators(9, 9, 0);
}

TEST_F(TestScanSchedule, TestCancelScanForConcurrent) {
    auto copysetInfos1 = GetCopySetInfosInLogicalPool(1);
    for (auto i = 0; i < 3; i++) {
        copysetInfos1[i].scaning = true;
    }

    auto copysetInfos2 = GetCopySetInfosInLogicalPool(2);
    InsertOperator(copysetInfos2[0], true);
    InsertOperator(copysetInfos2[1], true);
    InsertOperator(copysetInfos2[2], true);

    auto copysetInfos3 = GetCopySetInfosInLogicalPool(3);
    copysetInfos3[0].scaning = true;

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos1))
        .WillOnce(Return(copysetInfos2))
        .WillOnce(Return(copysetInfos3));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    opt_.scanConcurrentPerPool = 2;
    opt_.scanConcurrentPerChunkserver = 1;
    ReInitScheduler();
    scanScheduler_->Schedule();
    // orginal: (3 * start)
    // pool1: +(1 * cancel)
    // pool2: +(1 * cancel) - (1 * start)
    // pool3: +(1 * start)
    CheckOperators(5, 3, 2);
}

TEST_F(TestScanSchedule, TestScanInterval) {
    auto lastScanTime = TimeUtility::GetTimeofDaySec()
                        - opt_.scanIntervalSec + 10;
    auto copysetInfos1 = GetCopySetInfosInLogicalPool(1);
    copysetInfos1[0].lastScanSec = lastScanTime;
    auto copysetInfos2 = GetCopySetInfosInLogicalPool(2);
    copysetInfos2[0].lastScanSec = lastScanTime;
    auto copysetInfos3 = GetCopySetInfosInLogicalPool(3);
    copysetInfos3[0].lastScanSec = lastScanTime;

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos1))
        .WillOnce(Return(copysetInfos2))
        .WillOnce(Return(copysetInfos3));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    scanScheduler_->Schedule();
    CheckOperators(3, 3, 0);  // one operator per pool
}

TEST_F(TestScanSchedule, TestScanSelectStartPriority) {
    auto lastScanTime = TimeUtility::GetTimeofDaySec() - opt_.scanIntervalSec;
    auto copysetInfos = GetCopySetInfosInLogicalPool(1);
    copysetInfos[0].lastScanSec = lastScanTime - 10;
    copysetInfos[1].lastScanSec = lastScanTime - 20;
    copysetInfos[2].lastScanSec = lastScanTime - 30;

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos))
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillOnce(Invoke(GenGetLogicalPoolCb(true)))
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(false)));

    scanScheduler_->Schedule();
    // select copyset 2, so copyset 0 and 1 will not selected because
    // the concurrent for every chunkserver is 1.
    CheckOperators(1, 1, 0);
}

TEST_F(TestScanSchedule, TestScanSelectCancelPriority) {
    auto copysetInfos = GetCopySetInfosInLogicalPool(1);
    InsertOperator(copysetInfos[0], true);
    copysetInfos[1].scaning = true;
    InsertOperator(copysetInfos[2], true);

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos))
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillOnce(Invoke(GenGetLogicalPoolCb(true)))
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(false)));

    scanScheduler_->Schedule();
    CheckOperators(2, 1, 1);
    auto op = opController_->GetOperators()[0];
    ASSERT_EQ(op.copysetID, CopySetKey(1, 1));
    ASSERT_EQ(op.priority, OperatorPriority::HighPriority);  // cancel scan
}

TEST_F(TestScanSchedule, TestScanSelectCancelWithCandidate) {
    auto copysetInfos = GetCopySetInfosInLogicalPool(1);
    copysetInfos[0].candidatePeerInfo = copysetInfos[0].peers[0];
    copysetInfos[0].configChangeInfo
                   .set_type(ConfigChangeType::START_SCAN_PEER);
    copysetInfos[1].scaning = true;
    copysetInfos[2].scaning = true;

    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .WillOnce(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .WillOnce(Return(copysetInfos))
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(3)
        .WillOnce(Invoke(GenGetLogicalPoolCb(true)))
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(false)));

    scanScheduler_->Schedule();
    CheckOperators(1, 0, 1);
}

TEST_F(TestScanSchedule, TestScanScheduleRun) {
    EXPECT_CALL(*topoAdapter_, GetLogicalpools())
        .Times(3)
        .WillRepeatedly(Invoke(GetLogicalpools));
    EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
        .Times(9)
        .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
    EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
        .Times(9)
        .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

    // STEP 1: generate operators
    {
        scanScheduler_->Schedule();
        CheckOperators(6, 6, 0);

        scanScheduler_->Schedule();
        CheckOperators(6, 6, 0);

        opController_->RemoveOperator(CopySetKey(1, 1));
        CheckOperators(5, 5, 0);
        scanScheduler_->Schedule();
        CheckOperators(6, 6, 0);
    }

    // STEP 2: chunkserver start scan success
    {
        auto copysetInfos = GetCopySetInfosInLogicalPool(1);
        copysetInfos[0].scaning = true;
        copysetInfos[1].scaning = true;

        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Invoke(GetLogicalpools));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
            .Times(3)
            .WillOnce(Return(copysetInfos))
            .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
        EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
            .Times(3)
            .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

        opController_->RemoveOperator(CopySetKey(1, 1));
        opController_->RemoveOperator(CopySetKey(1, 2));
        scanScheduler_->Schedule();
        CheckOperators(4, 4, 0);
    }

    // STEP 3: update copysets from heartbeat
    {
        auto copysetInfos = GetCopySetInfosInLogicalPool(1);
        for (auto i = 0; i < 3; i++) {
            copysetInfos[i].scaning = true;
        }

        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Invoke(GetLogicalpools));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
            .Times(3)
            .WillOnce(Return(copysetInfos))
            .WillRepeatedly(Invoke(GetCopySetInfosInLogicalPool));
        EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
            .Times(3)
            .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

        scanScheduler_->Schedule();
        CheckOperators(5, 4, 1);
    }

    // STEP 4: all operators done
    {
        auto copysetInfos1 = GetCopySetInfosInLogicalPool(1);
        auto copysetInfos2 = GetCopySetInfosInLogicalPool(1);
        auto copysetInfos3 = GetCopySetInfosInLogicalPool(1);
        copysetInfos1[0].scaning = copysetInfos1[1].scaning = true;
        copysetInfos2[0].scaning = copysetInfos2[1].scaning = true;
        copysetInfos3[0].scaning = copysetInfos3[1].scaning = true;

        EXPECT_CALL(*topoAdapter_, GetLogicalpools())
            .WillOnce(Invoke(GetLogicalpools));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInLogicalPool(_))
            .Times(3)
            .WillOnce(Return(copysetInfos1))
            .WillOnce(Return(copysetInfos2))
            .WillOnce(Return(copysetInfos3));
        EXPECT_CALL(*topoAdapter_, GetLogicalPool(_, _))
            .Times(3)
            .WillRepeatedly(Invoke(GenGetLogicalPoolCb(true)));

        opController_->RemoveOperator(CopySetKey(1, 1));
        opController_->RemoveOperator(CopySetKey(2, 1));
        opController_->RemoveOperator(CopySetKey(2, 2));
        opController_->RemoveOperator(CopySetKey(3, 1));
        opController_->RemoveOperator(CopySetKey(3, 2));
        scanScheduler_->Schedule();
        CheckOperators(0, 0, 0);
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
