/*
 * Project: curve
 * Created Date: Saturday March 13th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include <cstdlib>
#include <memory>
#include "src/mds/kvstorageclient/etcd_client.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/timeutility.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/common/mds_define.h"
#include "proto/nameserver2.pb.h"

namespace curve {
namespace mds {
// 接口测试
class TestEtcdClinetImp : public ::testing::Test {
 protected:
    TestEtcdClinetImp() {}
    ~TestEtcdClinetImp() {}

    void SetUp() override {
        client_ = std::make_shared<EtcdClientImp>();
        char endpoints[] = "127.0.0.1:2377";
        EtcdConf conf = {endpoints, strlen(endpoints), 1000};
        ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Init(conf, 200, 3));
        std::string runEtcd =
            std::string("etcd --listen-client-urls 'http://localhost:2377'") +
            std::string(" --advertise-client-urls 'http://localhost:2377'") +
            std::string(" --listen-peer-urls 'http://localhost:2376'&");
        system(runEtcd.c_str());
        // 一定时间内尝试init直到etcd完全起来
        int now = ::curve::common::TimeUtility::GetTimeofDaySec();
        bool initSuccess = false;
        while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 5) {
            if (0 == client_->Init(conf, 0, 3)) {
                initSuccess = true;
                break;
            }
        }
        ASSERT_TRUE(initSuccess);
        ASSERT_EQ(
            EtcdErrCode::DeadlineExceeded, client_->Put("05", "hello word"));
        ASSERT_EQ(EtcdErrCode::DeadlineExceeded,
            client_->CompareAndSwap("04", "10", "110"));

        client_->SetTimeout(20000);
    }

    void TearDown() override {
        client_ = nullptr;
        system("killall etcd");
        system("rm -fr default.etcd");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

 protected:
    std::shared_ptr<EtcdClientImp> client_;
};

TEST_F(TestEtcdClinetImp, test_EtcdClientInterface) {
    // 1. put file
    // - file0~file9 put到etcd中
    // - file6有快照
    std::map<int, std::string> keyMap;
    std::map<int, std::string> fileName;
    FileInfo fileInfo7, fileInfo8;
    std::string fileInfo9, fileKey10, fileInfo10, fileName10;
    std::string fileInfo6, snapshotKey6, snapshotInfo6, snapshotName6;
    uint64_t DefaultChunkSize = 16 * kMB;
    for (int i = 0; i < 11; i++) {
        FileInfo fileinfo;
        std::string filename = "helloword-" + std::to_string(i) + ".log";
        fileinfo.set_id(i);
        fileinfo.set_filename(filename);
        fileinfo.set_parentid(i << 8);
        fileinfo.set_filetype(FileType::INODE_PAGEFILE);
        fileinfo.set_chunksize(DefaultChunkSize);
        fileinfo.set_length(10 << 20);
        fileinfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
        fileinfo.set_seqnum(1);
        std::string encodeFileInfo;
        ASSERT_TRUE(fileinfo.SerializeToString(&encodeFileInfo));
        std::string encodeKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(i << 8, filename);
        if (i <= 9) {
            ASSERT_EQ(EtcdErrCode::OK,
                client_->Put(encodeKey, encodeFileInfo));
            keyMap[i] = encodeKey;
            fileName[i] = filename;
        }

        if (i == 6) {
            fileinfo.set_seqnum(2);
            ASSERT_TRUE(fileinfo.SerializeToString(&encodeFileInfo));
            fileInfo6 = encodeFileInfo;

            fileinfo.set_seqnum(1);
            snapshotName6 = "helloword-" + std::to_string(i) + ".log.snap";
            fileinfo.set_filename(snapshotName6);
            ASSERT_TRUE(fileinfo.SerializeToString(&snapshotInfo6));
            snapshotKey6 = NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(
                                                                i << 8,
                                                                snapshotName6);
        }

        if (i == 7) {
            fileInfo7.CopyFrom(fileinfo);
        }

        if (i == 8) {
            fileInfo8.CopyFrom(fileinfo);
        }

        if (i == 9) {
            fileInfo9 = encodeFileInfo;
        }

        if (i == 10) {
            fileKey10 = encodeKey;
            fileInfo10 = encodeFileInfo;
            fileName10 = filename;
        }
    }

    // 2. get file, 可以正确获取并解码file0~file9
    for (int i = 0; i < keyMap.size(); i++) {
        std::string out;
        int errCode = client_->Get(keyMap[i], &out);
        ASSERT_EQ(EtcdErrCode::OK, errCode);
        FileInfo fileinfo;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
        ASSERT_EQ(fileName[i], fileinfo.filename());
    }

    // 3. list file, 可以list到file0~file9
    std::vector<std::string> listRes;
    int errCode = client_->List("01", "02", &listRes);
    ASSERT_EQ(EtcdErrCode::OK, errCode);
    ASSERT_EQ(keyMap.size(), listRes.size());
    for (int i = 0; i < listRes.size(); i++) {
        FileInfo finfo;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(listRes[i], &finfo));
        ASSERT_EQ(fileName[i], finfo.filename());
    }

    // 4. delete file, 删除file0~file4，这部分文件不能再获取到
    for (int i = 0; i < keyMap.size()/2; i++) {
        ASSERT_EQ(EtcdErrCode::OK, client_->Delete(keyMap[i]));
        // can not get delete file
        std::string out;
        ASSERT_EQ(
            EtcdErrCode::KeyNotExist, client_->Get(keyMap[i], &out));
    }

    // 5. rename file: rename file9 ~ file10, file10本来不存在
    Operation op1{
        OpType::OpDelete,
        const_cast<char*>(keyMap[9].c_str()),
        const_cast<char*>(fileInfo9.c_str()),
        keyMap[9].size(), fileInfo9.size()};
    Operation op2{
        OpType::OpPut,
         const_cast<char*>(fileKey10.c_str()),
        const_cast<char*>(fileInfo10.c_str()),
        fileKey10.size(), fileInfo10.size()};
    std::vector<Operation> ops{op1, op2};
    ASSERT_EQ(EtcdErrCode::OK, client_->TxnN(ops));
    // cannot get file9
    std::string out;
    ASSERT_EQ(EtcdErrCode::KeyNotExist,
        client_->Get(keyMap[9], &out));
    // get file10 ok
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(fileKey10, &out));
    FileInfo fileinfo;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(fileName10, fileinfo.filename());

    // 6. snapshot of keyMap[6]
    Operation op3{
        OpType::OpPut,
        const_cast<char*>(keyMap[6].c_str()),
        const_cast<char*>(fileInfo6.c_str()),
        keyMap[6].size(), fileInfo6.size()};
    Operation op4{
        OpType::OpPut,
        const_cast<char*>(snapshotKey6.c_str()),
        const_cast<char*>(snapshotInfo6.c_str()),
        snapshotKey6.size(), snapshotInfo6.size()};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op4);
    ASSERT_EQ(EtcdErrCode::OK, client_->TxnN(ops));
    // get file6
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(keyMap[6], &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(2, fileinfo.seqnum());
    ASSERT_EQ(fileName[6], fileinfo.filename());
    // get snapshot6
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(snapshotKey6, &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(1, fileinfo.seqnum());
    ASSERT_EQ(snapshotName6, fileinfo.filename());
    // list snapshotfile
    ASSERT_EQ(EtcdErrCode::OK, client_->List("03", "04", &listRes));
    ASSERT_EQ(1, listRes.size());

    // 7. test CompareAndSwap
    ASSERT_EQ(EtcdErrCode::OK,
        client_->CompareAndSwap("04", "", "100"));
    ASSERT_EQ(EtcdErrCode::OK, client_->Get("04", &out));
    ASSERT_EQ("100", out);

    ASSERT_EQ(EtcdErrCode::OK,
        client_->CompareAndSwap("04", "100", "200"));
    ASSERT_EQ(EtcdErrCode::OK, client_->Get("04", &out));
    ASSERT_EQ("200", out);

    // 8. rename file: rename file7 ~ file8
    Operation op8{
        OpType::OpDelete,
        const_cast<char*>(keyMap[7].c_str()), "",
        keyMap[7].size(), 0};
    FileInfo newFileInfo7;
    newFileInfo7.CopyFrom(fileInfo7);
    newFileInfo7.set_parentid(fileInfo8.parentid());
    newFileInfo7.set_filename(fileInfo8.filename());
    std::string encodeNewFileInfo7Key =
            NameSpaceStorageCodec::EncodeFileStoreKey(
                newFileInfo7.parentid(), newFileInfo7.filename());
    std::string encodeNewFileInfo7;
    ASSERT_TRUE(newFileInfo7.SerializeToString(&encodeNewFileInfo7));
    Operation op9{
        OpType::OpPut,
        const_cast<char*>(encodeNewFileInfo7Key.c_str()),
        const_cast<char*>(encodeNewFileInfo7.c_str()),
        encodeNewFileInfo7Key.size(), encodeNewFileInfo7.size()};
    ops.clear();
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(EtcdErrCode::OK, client_->TxnN(ops));
    // 不能获取 file7
    ASSERT_EQ(EtcdErrCode::KeyNotExist,
        client_->Get(keyMap[7], &out));
    // 成功获取rename以后的file7
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(keyMap[8], &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(newFileInfo7.filename(), fileinfo.filename());
    ASSERT_EQ(newFileInfo7.filetype(), fileinfo.filetype());

    // 9. test more Txn err
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(EtcdErrCode::InvalidArgument, client_->TxnN(ops));

    // 10. abnormal
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op4);
    client_->SetTimeout(0);
    ASSERT_EQ(
        EtcdErrCode::DeadlineExceeded, client_->Put(fileKey10, fileInfo10));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Delete("05"));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Get(fileKey10, &out));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->TxnN(ops));

    client_->SetTimeout(5000);
    Operation op5{
        OpType(5), const_cast<char*>(snapshotKey6.c_str()),
        const_cast<char*>(snapshotInfo6.c_str()),
        snapshotKey6.size(), snapshotInfo6.size()};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op5);
    ASSERT_EQ(EtcdErrCode::TxnUnkownOp, client_->TxnN(ops));

    // close client
    client_->CloseClient();
    ASSERT_EQ(EtcdErrCode::Canceled, client_->Put(fileKey10, fileInfo10));
    ASSERT_FALSE(
        EtcdErrCode::OK == client_->CompareAndSwap("04", "300", "400"));
}

TEST_F(TestEtcdClinetImp, test_ListWithLimitAndRevision) {
    // 准备一批数据
    // "011" "013" "015" "017" "019"
    for (int i = 1; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(EtcdErrCode::OK, client_->Put(key, value));
    }

    // "012" "014" "016" "018"
    for (int i = 2; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(EtcdErrCode::OK, client_->Put(key, value));
    }

    // 获取当前revision
    // 通过GetCurrentRevision获取
    int64_t curRevision;
    ASSERT_EQ(EtcdErrCode::OK, client_->GetCurrentRevision(&curRevision));
    LOG(INFO) << "get current revision: " << curRevision;

    // 根据当前revision获取前5个key-value
    std::vector<std::string> out;
    std::string lastKey;
    int res = client_->ListWithLimitAndRevision(
        "01", "", 5, curRevision, &out, &lastKey);
    ASSERT_EQ(EtcdErrCode::OK, res);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ("015", lastKey);
    for (int i = 1; i <= 5; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 1]);
    }

    // 根据当前revision获取后5个key-value
    out.clear();
    res = client_->ListWithLimitAndRevision(
        lastKey, "", 5, curRevision, &out, &lastKey);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ(EtcdErrCode::OK, res);
    ASSERT_EQ("019", lastKey);
    for (int i = 5; i <= 9; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 5]);
    }
}

TEST_F(TestEtcdClinetImp, test_CampaignLeader) {
    std::string pfx("/leadere-election/");
    int sessionnInterSec = 1;
    int dialtTimeout = 10000;
    int retryTimes = 3;
    char endpoints[] = "127.0.0.1:2377";
    EtcdConf conf = {endpoints, strlen(endpoints), 20000};
    std::string leaderName1("leader1");
    std::string leaderName2("leader2");
    uint64_t leaderOid;

    {
        // 1. leader1竞选成功，client退出后leader2竞选成功
        LOG(INFO) << "test case1 start...";
        // 启动一个线程竞选leader
        int electionTimeoutMs = 0;
        uint64_t targetOid;
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client_, pfx,
            leaderName1, sessionnInterSec, electionTimeoutMs, &targetOid);
        int now = ::curve::common::TimeUtility::GetTimeofDaySec();
        bool leaderKeyExist = false;
        while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 5) {
            if (targetOid == 0) {
                continue;
            }
            leaderKeyExist = client_->LeaderKeyExist(
                targetOid, sessionnInterSec * 1000);
        }
        ASSERT_TRUE(leaderKeyExist);
        // 等待线程1执行完成, 线程1执行完成就说明竞选成功，
        // 否则electionTimeoutMs为0的情况下会一直hung在里面
        thread1.join();
        LOG(INFO) << "thread 1 exit.";
        client_->CloseClient();

        // 启动第二个线程竞选leader
        auto client2 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client2->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client2, pfx,
            leaderName2, sessionnInterSec, electionTimeoutMs, &leaderOid);
        // 线程1退出后，leader2会当选
        thread2.join();
        LOG(INFO) << "thread 2 exit.";
        // leader2为leader的情况下此时观察leader1的key应该发现not exist
        ASSERT_EQ(EtcdErrCode::ObserverLeaderNotExist,
            client2->LeaderObserve(targetOid, 1000, leaderName1));
        client2->CloseClient();
    }

    {
        // 2. leader1竞选成功后，不退出; leader2竞选超时
        LOG(INFO) << "test case2 start...";
        int electionTimeoutMs = 1000;
        auto client1 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client1->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client1, pfx,
            leaderName1, sessionnInterSec, electionTimeoutMs, &leaderOid);
        thread1.join();
        LOG(INFO) << "thread 1 exit.";

        // leader2再次竞选
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client1, pfx,
            leaderName2, sessionnInterSec, electionTimeoutMs, &leaderOid);
        thread2.join();
        client1->CloseClient();
        LOG(INFO) << "thread 2 exit.";
    }

    {
        // 3. leader1竞选成功后，删除key; leader2竞选成功; observe leader1改变;
        //  observer leader2的过程中etcd挂掉
        LOG(INFO) << "test case3 start...";
        uint64_t targetOid;
        int electionTimeoutMs = 0;
        auto client1 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client1->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client1, pfx,
            leaderName1, sessionnInterSec, electionTimeoutMs, &targetOid);
        thread1.join();
        LOG(INFO) << "thread 1 exit.";
        // leader1卸任leader
        ASSERT_EQ(EtcdErrCode::LeaderResiginSuccess,
            client1->LeaderResign(targetOid, 1000));

        // leader2当选
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client1, pfx,
            leaderName2, sessionnInterSec, electionTimeoutMs, &leaderOid);
        thread2.join();
        // leader1观察到leader改变
        ASSERT_EQ(EtcdErrCode::ObserverLeaderChange,
            client1->LeaderObserve(targetOid, 1000, leaderName1));

        // leader2启动线程observe
        common::Thread thread3(&EtcdClientImp::LeaderObserve, client1,
            targetOid, 1000, leaderName2);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        system("killall etcd");
        thread3.join();
        client1->CloseClient();
        LOG(INFO) << "thread 2 exit.";

        // 使得etcd完全停掉
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

}  // namespace mds
}  // namespace curve
