/*
 * Project: curve
 * Created Date: Saturday March 13th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <cstdlib>
#include <memory>
#include "src/mds/nameserver2/etcd_client.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/common/timeutility.h"
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
     char endpoints[] = "127.0.0.1:2379";
     EtcdConf conf = {endpoints, strlen(endpoints), 20000};
     ASSERT_EQ(0, client_->Init(conf, 200, 3));
     ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Put("05", "hello word"));
     client_->SetTimeout(20000);
     system("etcd&");
  }

  void TearDown() override {
      client_ = nullptr;
      system("rm -fr default.etcd");
  }

 protected:
  std::shared_ptr<EtcdClientImp> client_;
};

TEST_F(TestEtcdClinetImp, test_EtcdClientInterface) {
    // 1. put file
    std::map<int, std::string> keyMap;
    std::map<int, std::string> fileName;
    std::map<int, std::string> fullPathname;
    std::string fileInfo9, fileKey10, fileInfo10, fileName10;
    std::string fileInfo6, snapshotKey6, snapshotInfo6, snapshotName6;
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
        std::string fullpathname = "/A/B/" + std::to_string(i) + "/" + filename;
        fileinfo.set_fullpathname(fullpathname);
        fileinfo.set_seqnum(1);
        std::string encodeFileInfo;
        ASSERT_TRUE(fileinfo.SerializeToString(&encodeFileInfo));
        std::string encodeKey = EncodeFileStoreKey(i << 8, filename);
        if (i <= 9) {
            ASSERT_EQ(EtcdErrCode::OK,
                client_->Put(encodeKey, encodeFileInfo));
            keyMap[i] = encodeKey;
            fileName[i] = filename;
            fullPathname[i] = fullpathname;
        }

        if (i == 6) {
            fileinfo.set_seqnum(2);
            ASSERT_TRUE(fileinfo.SerializeToString(&encodeFileInfo));
            fileInfo6 = encodeFileInfo;

            fileinfo.set_seqnum(1);
            snapshotName6 = "helloword-" + std::to_string(i) + ".log.snap";
            fullpathname = fullpathname + "/" + snapshotName6;
            fileinfo.set_filename(snapshotName6);
            fileinfo.set_fullpathname(fullpathname);
            ASSERT_TRUE(fileinfo.SerializeToString(&snapshotInfo6));
            snapshotKey6 = EncodeSnapShotFileStoreKey(i << 8, snapshotName6);
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

    // 2. get file
    for (int i = 0; i < keyMap.size(); i++) {
        std::string out;
        int errCode = client_->Get(keyMap[i], &out);
        ASSERT_EQ(EtcdErrCode::OK, errCode);
        FileInfo fileinfo;
        ASSERT_TRUE(DecodeFileInfo(out, &fileinfo));
        ASSERT_EQ(fileName[i], fileinfo.filename());
        ASSERT_EQ(fullPathname[i], fileinfo.fullpathname());
    }

    // 3. list file
    std::vector<std::string> listRes;
    int errCode = client_->List("01", "02", &listRes);
    ASSERT_EQ(EtcdErrCode::OK, errCode);
    ASSERT_EQ(keyMap.size(), listRes.size());
    for (int i = 0; i < listRes.size(); i++) {
        FileInfo fileinfo;
        ASSERT_TRUE(DecodeFileInfo(listRes[i], &fileinfo));
        ASSERT_EQ(fileName[i], fileinfo.filename());
        ASSERT_EQ(fullPathname[i], fileinfo.fullpathname());
    }

    // 4. delete file
    for (int i = 0; i < keyMap.size()/2; i++) {
        ASSERT_EQ(EtcdErrCode::OK, client_->Delete(keyMap[i]));
        // can not get delete file
        std::string out;
        ASSERT_EQ(
            EtcdErrCode::KeyNotExist, client_->Get(keyMap[i], &out));
    }

    // 5. rename file: rename file9 ~ file10
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
    ASSERT_EQ(EtcdErrCode::OK, client_->Txn2(op1, op2));

    // cannot get file9
    std::string out;
    ASSERT_EQ(EtcdErrCode::KeyNotExist,
        client_->Get(keyMap[9], &out));
    // get file10 ok
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(fileKey10, &out));
    FileInfo fileinfo;
    ASSERT_TRUE(DecodeFileInfo(out, &fileinfo));
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
    ASSERT_EQ(EtcdErrCode::OK, client_->Txn2(op3, op4));
    // get file6
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(keyMap[6], &out));
    ASSERT_TRUE(DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(2, fileinfo.seqnum());
    ASSERT_EQ(fileName[6], fileinfo.filename());
    // get snapshot6
    ASSERT_EQ(EtcdErrCode::OK, client_->Get(snapshotKey6, &out));
    ASSERT_TRUE(DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(1, fileinfo.seqnum());
    ASSERT_EQ(snapshotName6, fileinfo.filename());
    // list snapshotfile
    ASSERT_EQ(EtcdErrCode::OK, client_->List("03", "04", &listRes));
    ASSERT_EQ(1, listRes.size());

    // 7. abnormal
    client_->SetTimeout(0);
    ASSERT_EQ(
        EtcdErrCode::DeadlineExceeded, client_->Put(fileKey10, fileInfo10));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Delete("05"));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Get(fileKey10, &out));
    ASSERT_EQ(EtcdErrCode::DeadlineExceeded, client_->Txn2(op3, op4));

    client_->SetTimeout(5000);
    Operation op5{
        OpType(5), const_cast<char*>(snapshotKey6.c_str()),
        const_cast<char*>(snapshotInfo6.c_str()),
        snapshotKey6.size(), snapshotInfo6.size()};
    ASSERT_EQ(EtcdErrCode::TxnUnkownOp, client_->Txn2(op3, op5));

    // close client
    client_->CloseClient();
    ASSERT_EQ(EtcdErrCode::Canceled, client_->Put(fileKey10, fileInfo10));
}
}  // namespace mds
}  // namespace curve
