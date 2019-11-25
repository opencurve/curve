/*
 * Project: curve
 * File Created: 2019-11-1
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/copyset_check.h"
#include "test/tools/mock_copyset_check_core.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;

DECLARE_bool(detail);
DECLARE_uint32(logicalPoolId);
DECLARE_uint32(copysetId);
DECLARE_uint32(chunkserverId);
DECLARE_string(chunkserverAddr);
DECLARE_uint32(serverId);
DECLARE_string(serverIp);
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");

class CopysetCheckTest : public ::testing::Test {
 protected:
    void SetUp() {
        core_ = std::make_shared<curve::tool::MockCopysetCheckCore>();
        FLAGS_detail = true;
    }
    void TearDown() {
        core_ = nullptr;
    }

    void GetIoBufForTest(butil::IOBuf* buf, const std::string& gId,
                                            bool isLeader = false,
                                            bool noLeader = false,
                                            bool installingSnapshot = false,
                                            bool peersLess = false,
                                            bool gapBig = false,
                                            bool parseErr = false,
                                            bool peerOffline = false) {
        butil::IOBufBuilder os;
        os << "[" << gId <<  "]\r\n";
        if (peersLess) {
            os << "peers: \r\n";
        } else if (peerOffline) {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0 127.0.0.1:9194:0\r\n";  // NOLINT
        } else {
            os << "peers: 127.0.0.1:9191:0 127.0.0.1:9192:0 127.0.0.1:9193:0\r\n";  // NOLINT
        }
        os << "storage: [2581, 2580]\n";
        if (parseErr) {
            os << "\n";
        } else {
            os << "last_log_id: (index=2580,term=4)\n";
        }
        os << "state_machine: Idle\r\n";
        if (isLeader) {
            os << "state: " << "LEADER" << "\r\n";
            os << "replicator_123: next_index=";
            if (gapBig) {
                os << "1000";
            } else {
                os << "2581";
            }
            os << " flying_append_entries_size=0 ";
            if (installingSnapshot) {
                os << "installing snapshot {1234, 3} ";
            } else {
                os << "idle ";
            }
            os << "hc=4211759 ac=1089 ic=0\r\n";
        } else {
            os << "state: " << "FOLLOWER" << "\r\n";
            if (noLeader) {
                os << "leader: " << "0.0.0.0:0:0\r\n";
            } else {
                os << "leader: " << "127.0.0.1:9192:0\r\n";
            }
        }
        os.move_to(*buf);
    }

    std::map<std::string, std::set<std::string>> res1 =
                    {{"total", {"4294967396", "4294967397"}}};
    std::map<std::string, std::set<std::string>> res2 =
                    {{"total", {"4294967396", "4294967397", "4294967398",
                               "4294967399", "4294967400", "4294967401"}},
                     {"installing snapshot", {"4294967397"}},
                     {"no leader", {"4294967398"}},
                     {"index gap too big", {"4294967399"}},
                     {"peers not sufficient", {"4294967400"}},
                     {"peer not online", {"4294967401"}}};
    std::set<std::string> serviceExcepCs = {"127.0.0.1:9092"};
    std::set<std::string> emptySet;

    std::shared_ptr<curve::tool::MockCopysetCheckCore> core_;
};

TEST_F(CopysetCheckTest, CheckOneCopyset) {
    curve::tool::CopysetCheck copysetCheck(core_);
    butil::IOBuf iobuf;
    GetIoBufForTest(&iobuf, "4294967396", true);
    std::vector<std::string> peersInCopyset =
            {"127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093"};
    std::string copysetDetail = iobuf.to_string();

    // 不支持的命令
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-nothings"));
    copysetCheck.PrintHelp("check-nothins");
    // 没有指定逻辑池和copyset的话返回失败
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));
    FLAGS_logicalPoolId = 1;
    FLAGS_copysetId = 100;
    copysetCheck.PrintHelp("check-copyset");

    // 健康的情况
    EXPECT_CALL(*core_, CheckOneCopyset(_, _))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*core_, GetCopysetDetail())
        .Times(1)
        .WillOnce(ReturnRef(copysetDetail));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-copyset"));

    // copyset不健康的情况
    EXPECT_CALL(*core_, CheckOneCopyset(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*core_, GetCopysetDetail())
        .Times(1)
        .WillOnce(ReturnRef(copysetDetail));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(serviceExcepCs));
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-copyset"));
}

TEST_F(CopysetCheckTest, testCheckChunkServer) {
    curve::tool::CopysetCheck copysetCheck(core_);

    // 没有指定chunkserver的话报错
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
    copysetCheck.PrintHelp("check-chunkserver");

    // 健康的情况
    // 通过id查询
    FLAGS_chunkserverId = 1;
    EXPECT_CALL(*core_, CheckCopysetsOnChunkServer(FLAGS_chunkserverId))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res1));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-chunkserver"));
    // id和地址同时指定，报错
    FLAGS_chunkserverAddr = "127.0.0.1:8200";
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
    FLAGS_chunkserverId = 0;
    // 通过地址查询
    EXPECT_CALL(*core_, CheckCopysetsOnChunkServer(FLAGS_chunkserverAddr))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res1));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-chunkserver"));

    // 不健康的情况
    EXPECT_CALL(*core_, CheckCopysetsOnChunkServer(FLAGS_chunkserverAddr))
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res2));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(serviceExcepCs));
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-chunkserver"));
}

TEST_F(CopysetCheckTest, testCheckServer) {
    curve::tool::CopysetCheck copysetCheck(core_);
    std::vector<std::string> chunkservers =
            {"127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093"};

    // 没有指定server的话报错
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-server"));
    copysetCheck.PrintHelp("check-server");

    // 健康的情况
    // 通过id查询
    FLAGS_serverId = 1;
    EXPECT_CALL(*core_, CheckCopysetsOnServer(FLAGS_serverId, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res1));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-server"));
    // id和ip同时指定，报错
    FLAGS_serverIp = "127.0.0.1";
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-server"));
    FLAGS_serverId = 0;
    // 通过ip查询
    EXPECT_CALL(*core_, CheckCopysetsOnServer(FLAGS_serverIp, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res1));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-server"));

    // 不健康的情况
    EXPECT_CALL(*core_, CheckCopysetsOnServer(FLAGS_serverIp, _))
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res2));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(serviceExcepCs));
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-server"));
}

TEST_F(CopysetCheckTest, testCheckCluster) {
    curve::tool::CopysetCheck copysetCheck(core_);
    // 健康的情况
    EXPECT_CALL(*core_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res1));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(emptySet));
    ASSERT_EQ(0, copysetCheck.RunCommand("check-cluster"));

    // 不健康的情况
    EXPECT_CALL(*core_, CheckCopysetsInCluster())
        .Times(1)
        .WillOnce(Return(-1));
    EXPECT_CALL(*core_, GetCopysetsRes())
        .Times(2)
        .WillRepeatedly(ReturnRef(res2));
    EXPECT_CALL(*core_, GetServiceExceptionChunkServer())
        .Times(1)
        .WillOnce(ReturnRef(serviceExcepCs));
    ASSERT_EQ(-1, copysetCheck.RunCommand("check-cluster"));
}
