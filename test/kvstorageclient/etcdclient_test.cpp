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
 * Created Date: Saturday March 13th 2019
 * Author: lixiaocui1
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>  //NOLINT
#include <cstdlib>
#include <memory>
#include <thread>  //NOLINT

#include "proto/nameserver2.pb.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

namespace curve {
namespace kvstorage {

using ::curve::kvstorage::EtcdClientImp;
using ::curve::mds::FileInfo;
using ::curve::mds::FileType;
using ::curve::mds::NameSpaceStorageCodec;
using ::curve::mds::PageFileChunkInfo;
using ::curve::mds::PageFileSegment;

// Interface testing
class TestEtcdClinetImp : public ::testing::Test {
 protected:
    TestEtcdClinetImp() {}
    ~TestEtcdClinetImp() {}

    void SetUp() override {
        system("rm -fr testEtcdClinetImp.etcd");

        client_ = std::make_shared<EtcdClientImp>();
        char endpoints[] = "127.0.0.1:2377";
        EtcdConf conf = {endpoints, static_cast<int>(strlen(endpoints)), 1000};
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client_->Init(conf, 200, 3));

        etcdPid = ::fork();
        if (0 > etcdPid) {
            ASSERT_TRUE(false);
        } else if (0 == etcdPid) {
            /**
             * Important reminder!!!!
             * After forking, try not to use LOG() printing for child processes,
             * as it may cause deadlock!!!
             */
            ASSERT_EQ(0,
                      execlp("etcd", "etcd", "--listen-client-urls",
                             "http://localhost:2377", "--advertise-client-urls",
                             "http://localhost:2377", "--listen-peer-urls",
                             "http://localhost:2376", "--name",
                             "testEtcdClinetImp", nullptr));
            exit(0);
        }

        // Try init for a certain period of time until etcd is fully recovered
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        bool initSuccess = false;
        while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 50) {
            if (0 == client_->Init(conf, 0, 3)) {
                initSuccess = true;
                break;
            }
        }
        ASSERT_TRUE(initSuccess);
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client_->Put("05", "hello word"));
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client_->CompareAndSwap("04", "10", "110"));

        client_->SetTimeout(20000);
    }

    void TearDown() override {
        client_ = nullptr;
        system(("kill " + std::to_string(etcdPid)).c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));

        system("rm -fr testEtcdClinetImp.etcd");
    }

 protected:
    std::shared_ptr<EtcdClientImp> client_;
    pid_t etcdPid;
};

TEST_F(TestEtcdClinetImp, test_EtcdClientInterface) {
    // 1. put file
    // - file0~file9 put into etcd
    // - file6 has a snapshot
    std::map<int, std::string> keyMap;
    std::map<int, std::string> fileName;
    FileInfo fileInfo7, fileInfo8;
    std::string fileInfo9, fileKey10, fileInfo10, fileName10;
    std::string fileInfo6, snapshotKey6, snapshotInfo6, snapshotName6;
    uint64_t DefaultChunkSize = 16 * 1024 * 1024;
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
            ASSERT_EQ(EtcdErrCode::EtcdOK,
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
                i << 8, snapshotName6);
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

    // 2. get file, which can correctly obtain and decode file0~file9
    for (int i = 0; i < keyMap.size(); i++) {
        std::string out;
        int errCode = client_->Get(keyMap[i], &out);
        ASSERT_EQ(EtcdErrCode::EtcdOK, errCode);
        FileInfo fileinfo;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
        ASSERT_EQ(fileName[i], fileinfo.filename());
    }

    // 3. list file, which can be listed to file0~file9
    std::vector<std::string> listRes;
    std::vector<std::pair<std::string, std::string>> listRes2;
    int errCode = client_->List("01", "02", &listRes2);
    ASSERT_EQ(EtcdErrCode::EtcdOK, errCode);
    ASSERT_EQ(keyMap.size(), listRes2.size());
    for (int i = 0; i < listRes2.size(); i++) {
        FileInfo finfo;
        ASSERT_TRUE(
            NameSpaceStorageCodec::DecodeFileInfo(listRes2[i].second, &finfo));
        ASSERT_EQ(fileName[i], finfo.filename());
    }

    // 4. Delete file, delete file0~file4, these files cannot be retrieved
    // anymore
    for (int i = 0; i < keyMap.size() / 2; i++) {
        ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Delete(keyMap[i]));
        // can not get delete file
        std::string out;
        ASSERT_EQ(EtcdErrCode::EtcdKeyNotExist, client_->Get(keyMap[i], &out));
    }

    // 5. Rename file: rename file9~file10, file10 does not originally exist
    Operation op1{OpType::OpDelete, const_cast<char*>(keyMap[9].c_str()),
                  const_cast<char*>(fileInfo9.c_str()),
                  static_cast<int>(keyMap[9].size()),
                  static_cast<int>(fileInfo9.size())};
    Operation op2{OpType::OpPut, const_cast<char*>(fileKey10.c_str()),
                  const_cast<char*>(fileInfo10.c_str()),
                  static_cast<int>(fileKey10.size()),
                  static_cast<int>(fileInfo10.size())};
    std::vector<Operation> ops{op1, op2};
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));
    // cannot get file9
    std::string out;
    ASSERT_EQ(EtcdErrCode::EtcdKeyNotExist, client_->Get(keyMap[9], &out));
    // get file10 ok
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get(fileKey10, &out));
    FileInfo fileinfo;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(fileName10, fileinfo.filename());

    // 6. snapshot of keyMap[6]
    Operation op3{OpType::OpPut, const_cast<char*>(keyMap[6].c_str()),
                  const_cast<char*>(fileInfo6.c_str()),
                  static_cast<int>(keyMap[6].size()),
                  static_cast<int>(fileInfo6.size())};
    Operation op4{OpType::OpPut, const_cast<char*>(snapshotKey6.c_str()),
                  const_cast<char*>(snapshotInfo6.c_str()),
                  static_cast<int>(snapshotKey6.size()),
                  static_cast<int>(snapshotInfo6.size())};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op4);
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));
    // get file6
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get(keyMap[6], &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(2, fileinfo.seqnum());
    ASSERT_EQ(fileName[6], fileinfo.filename());
    // get snapshot6
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get(snapshotKey6, &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(1, fileinfo.seqnum());
    ASSERT_EQ(snapshotName6, fileinfo.filename());
    // list snapshotfile
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->List("03", "04", &listRes));
    ASSERT_EQ(1, listRes.size());

    // 7. test CompareAndSwap
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->CompareAndSwap("04", "", "100"));
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get("04", &out));
    ASSERT_EQ("100", out);

    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->CompareAndSwap("04", "100", "200"));
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get("04", &out));
    ASSERT_EQ("200", out);

    // 8. rename file: rename file7 ~ file8
    Operation op8{OpType::OpDelete, const_cast<char*>(keyMap[7].c_str()),
                  const_cast<char*>(""), static_cast<int>(keyMap[7].size()), 0};
    FileInfo newFileInfo7;
    newFileInfo7.CopyFrom(fileInfo7);
    newFileInfo7.set_parentid(fileInfo8.parentid());
    newFileInfo7.set_filename(fileInfo8.filename());
    std::string encodeNewFileInfo7Key =
        NameSpaceStorageCodec::EncodeFileStoreKey(newFileInfo7.parentid(),
                                                  newFileInfo7.filename());
    std::string encodeNewFileInfo7;
    ASSERT_TRUE(newFileInfo7.SerializeToString(&encodeNewFileInfo7));
    Operation op9{OpType::OpPut,
                  const_cast<char*>(encodeNewFileInfo7Key.c_str()),
                  const_cast<char*>(encodeNewFileInfo7.c_str()),
                  static_cast<int>(encodeNewFileInfo7Key.size()),
                  static_cast<int>(encodeNewFileInfo7.size())};
    ops.clear();
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));
    // Unable to obtain file7
    ASSERT_EQ(EtcdErrCode::EtcdKeyNotExist, client_->Get(keyMap[7], &out));
    // Successfully obtained file7 after renam
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Get(keyMap[8], &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(newFileInfo7.filename(), fileinfo.filename());
    ASSERT_EQ(newFileInfo7.filetype(), fileinfo.filetype());

    // 9. test more Txn err
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(EtcdErrCode::EtcdInvalidArgument, client_->TxnN(ops));

    // 10. abnormal
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op4);
    client_->SetTimeout(0);
    ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
              client_->Put(fileKey10, fileInfo10));
    ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded, client_->Delete("05"));
    ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded, client_->Get(fileKey10, &out));
    ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded, client_->TxnN(ops));

    client_->SetTimeout(5000);
    Operation op5{OpType(5), const_cast<char*>(snapshotKey6.c_str()),
                  const_cast<char*>(snapshotInfo6.c_str()),
                  static_cast<int>(snapshotKey6.size()),
                  static_cast<int>(snapshotInfo6.size())};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op5);
    ASSERT_EQ(EtcdErrCode::EtcdTxnUnkownOp, client_->TxnN(ops));

    // close client
    client_->CloseClient();
    ASSERT_EQ(EtcdErrCode::EtcdCanceled, client_->Put(fileKey10, fileInfo10));
    ASSERT_FALSE(EtcdErrCode::EtcdOK ==
                 client_->CompareAndSwap("04", "300", "400"));
}

TEST_F(TestEtcdClinetImp, test_ListWithLimitAndRevision) {
    // Prepare a batch of data
    // "011" "013" "015" "017" "019"
    for (int i = 1; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Put(key, value));
    }

    // "012" "014" "016" "018"
    for (int i = 2; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Put(key, value));
    }

    // Obtain the current revision
    // Obtained through GetCurrentRevision
    int64_t curRevision;
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->GetCurrentRevision(&curRevision));
    LOG(INFO) << "get current revision: " << curRevision;

    // Obtain the top 5 key values based on the current revision
    std::vector<std::string> out;
    std::string lastKey;
    int res = client_->ListWithLimitAndRevision("01", "", 5, curRevision, &out,
                                                &lastKey);
    ASSERT_EQ(EtcdErrCode::EtcdOK, res);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ("015", lastKey);
    for (int i = 1; i <= 5; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 1]);
    }

    // Obtain the last 5 key values based on the current revision
    out.clear();
    res = client_->ListWithLimitAndRevision(lastKey, "", 5, curRevision, &out,
                                            &lastKey);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ(EtcdErrCode::EtcdOK, res);
    ASSERT_EQ("019", lastKey);
    for (int i = 5; i <= 9; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 5]);
    }
}

TEST_F(TestEtcdClinetImp, test_return_with_revision) {
    int64_t startRevision;
    int res = client_->GetCurrentRevision(&startRevision);
    ASSERT_EQ(EtcdErrCode::EtcdOK, res);

    int64_t revision;
    res = client_->PutRewithRevision("hello", "everyOne", &revision);
    ASSERT_EQ(EtcdErrCode::EtcdOK, res);
    ASSERT_EQ(startRevision + 1, revision);

    res = client_->DeleteRewithRevision("hello", &revision);
    ASSERT_EQ(EtcdErrCode::EtcdOK, res);
    ASSERT_EQ(startRevision + 2, revision);
}

TEST_F(TestEtcdClinetImp, test_CampaignLeader) {
    std::string pfx("/leadere-election/");
    int sessionnInterSec = 1;
    int dialtTimeout = 10000;
    int retryTimes = 3;
    char endpoints[] = "127.0.0.1:2377";
    EtcdConf conf = {endpoints, static_cast<int>(strlen(endpoints)), 20000};
    std::string leaderName1("leader1");
    std::string leaderName2("leader2");
    uint64_t leaderOid;

    {
        // 1. leader1 successfully ran, but leader2 successfully ran after
        // client exited
        LOG(INFO) << "test case1 start...";
        // Start a thread to run for the leader
        int electionTimeoutMs = 0;
        uint64_t targetOid;
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client_, pfx,
                               leaderName1, sessionnInterSec, electionTimeoutMs,
                               &targetOid);
        // Waiting for thread 1 to complete execution indicates a successful
        // election, Otherwise, if electionTimeoutMs is 0, they will remain in
        // it all the time
        thread1.join();
        LOG(INFO) << "thread 1 exit.";
        client_->CloseClient();

        // Start the second thread to run for the leader
        auto client2 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client2->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client2, pfx,
                               leaderName2, sessionnInterSec, electionTimeoutMs,
                               &leaderOid);
        // After thread1 exits, leader2 will be elected
        thread2.join();
        LOG(INFO) << "thread 2 exit.";
        // If leader2 is the leader, observing the key of leader1 at this time
        // should reveal that the session has expired
        ASSERT_EQ(EtcdErrCode::EtcdObserverLeaderInternal,
                  client2->LeaderObserve(targetOid, leaderName1));
        client2->CloseClient();
    }

    {
        // 2. After the successful election of leader1, do not withdraw; leader2
        // campaign timeout
        LOG(INFO) << "test case2 start...";
        int electionTimeoutMs = 1000;
        auto client1 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client1->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client1, pfx,
                               leaderName1, sessionnInterSec, electionTimeoutMs,
                               &leaderOid);
        thread1.join();
        LOG(INFO) << "thread 1 exit.";

        // leader2 is running again
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client1, pfx,
                               leaderName2, sessionnInterSec, electionTimeoutMs,
                               &leaderOid);
        thread2.join();
        client1->CloseClient();
        LOG(INFO) << "thread 2 exit.";
    }

    {
        // 3. After the successful election of leader1, delete the key; The
        // leader2 campaign was successful; Observe leader1 changed;
        //   During the process of observer leader2, etcd crashes
        LOG(INFO) << "test case3 start...";
        uint64_t targetOid;
        int electionTimeoutMs = 0;
        auto client1 = std::make_shared<EtcdClientImp>();
        ASSERT_EQ(0, client1->Init(conf, dialtTimeout, retryTimes));
        common::Thread thread1(&EtcdClientImp::CampaignLeader, client1, pfx,
                               leaderName1, sessionnInterSec, electionTimeoutMs,
                               &targetOid);
        thread1.join();
        LOG(INFO) << "thread 1 exit.";
        // leader1 Resignation Leader
        ASSERT_EQ(EtcdErrCode::EtcdLeaderResiginSuccess,
                  client1->LeaderResign(targetOid, 1000));

        // leader2 elected
        common::Thread thread2(&EtcdClientImp::CampaignLeader, client1, pfx,
                               leaderName2, sessionnInterSec, electionTimeoutMs,
                               &leaderOid);
        thread2.join();

        // leader2 starts thread observe
        common::Thread thread3(&EtcdClientImp::LeaderObserve, client1,
                               targetOid, leaderName2);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        system(("kill " + std::to_string(etcdPid)).c_str());
        thread3.join();
        client1->CloseClient();
        LOG(INFO) << "thread 2 exit.";

        // Make the ETCD completely stop
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

TEST_F(TestEtcdClinetImp, test_ListSegment) {
    PageFileSegment segment;
    segment.set_chunksize(16 << 20);
    segment.set_segmentsize(1 << 30);
    segment.set_startoffset(0);
    segment.set_logicalpoolid(11);
    int size = segment.segmentsize() / segment.chunksize();
    for (uint32_t i = 0; i < size; i++) {
        PageFileChunkInfo* chunkinfo = segment.add_chunks();
        chunkinfo->set_chunkid(i + 1);
        chunkinfo->set_copysetid(i + 1);
    }

    // Place the segment, with the first three belonging to file1 and the last
    // four belonging to file2
    uint64_t id1 = 101;
    uint64_t id2 = 100001;
    for (uint32_t i = 0; i < 7; ++i) {
        uint64_t offset = i * 1024;
        segment.set_startoffset(offset);
        std::string key;
        if (i < 3) {
            key = NameSpaceStorageCodec::EncodeSegmentStoreKey(id1, offset);
        } else {
            key = NameSpaceStorageCodec::EncodeSegmentStoreKey(id2, offset);
        }
        std::string encodeSegment;
        ASSERT_TRUE(segment.SerializeToString(&encodeSegment));
        ASSERT_EQ(EtcdErrCode::EtcdOK, client_->Put(key, encodeSegment));
        LOG(INFO) << "offset: " << offset;
        LOG(INFO) << segment.startoffset();
    }

    // Obtain the segment of file1
    std::string startKey = NameSpaceStorageCodec::EncodeSegmentStoreKey(id1, 0);
    std::string endKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id1 + 1, 0);
    std::vector<std::string> out;
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->List(startKey, endKey, &out));
    ASSERT_EQ(3, out.size());
    for (int i = 0; i < out.size(); i++) {
        PageFileSegment segment2;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeSegment(out[i], &segment2));
        ASSERT_EQ(i * 1024, segment2.startoffset());
    }

    // Obtain the segment of file2
    startKey = NameSpaceStorageCodec::EncodeSegmentStoreKey(id2, 0);
    endKey = NameSpaceStorageCodec::EncodeSegmentStoreKey(id2 + 1, 0);
    out.clear();
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->List(startKey, endKey, &out));
    ASSERT_EQ(4, out.size());
    for (int i = 0; i < out.size(); i++) {
        PageFileSegment segment2;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeSegment(out[i], &segment2));
        ASSERT_EQ((i + 3) * 1024, segment2.startoffset());
    }
}
}  // namespace kvstorage
}  // namespace curve
