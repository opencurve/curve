#include <gtest/gtest.h>
#include <glog/logging.h>
#include <cassert>
#include <thread>  //NOLINT
#include <chrono>  //NOLINT
#include <cstdlib>
#include <memory>
#include "src/mysqlstorageclient/mysql_client.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/timeutility.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/common/mds_define.h"
#include "proto/nameserver2.pb.h"

namespace curve {
namespace mysqlstorage {

using ::curve::mysqlstorage::MysqlClientImp;
using ::curve::mysqlstorage::MysqlConf;
using ::curve::mds::FileInfo;
using ::curve::mds::FileType;
using ::curve::mds::NameSpaceStorageCodec;
using ::curve::mds::PageFileChunkInfo;
using ::curve::mds::PageFileSegment;

class TestMysqlClinetImp : public ::testing::Test {
 protected:
    TestMysqlClinetImp() {}
    ~TestMysqlClinetImp() {}
    void SetUp() {
        system("rm -fr testMysqlClinetImp.etcd");

        MysqlConf conf;
        client_ = std::make_shared<MysqlClientImp>();
        
        ASSERT_EQ(0, client_->Init(conf, 1000, 3));
        ASSERT_EQ(0, client_->DropTable("curvebs_kv"));
        ASSERT_EQ(0, client_->CreateTable("curvebs_kv"));

        LOG(INFO) << "SetUp";   
       
    }
    void TearDown() override {
        client_ = nullptr;

        system("rm -fr testMysqlClinetImp.etcd");
    }

    protected:
        std::shared_ptr<MysqlClientImp> client_;
};

TEST_F(TestMysqlClinetImp,test_MysqlClientInterface) {   
    LOG(INFO) << "test_MysqlClientInterface";
    // 1. put file
    // - file0~file9 put到etcd中
    // - file6有快照
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
            ASSERT_EQ(0,
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

    // 2. get file, 可以正确获取并解码file0~file9
    for (int i = 0; i < keyMap.size(); i++) {
        std::string out;
        int errCode = client_->Get(keyMap[i], &out);
        ASSERT_EQ(0, errCode);
        FileInfo fileinfo;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
        ASSERT_EQ(fileName[i], fileinfo.filename());
    }

    // 3. list file, 可以list到file0~file9
    std::vector<std::string> listRes;
    std::vector<std::pair<std::string, std::string>> listRes2;
    int errCode = client_->List("01", "02", &listRes2);
    ASSERT_EQ(0, errCode);
    ASSERT_EQ(keyMap.size(), listRes2.size());
    for (int i = 0; i < listRes2.size(); i++) {
        FileInfo finfo;
        ASSERT_TRUE(
            NameSpaceStorageCodec::DecodeFileInfo(listRes2[i].second, &finfo));
        ASSERT_EQ(fileName[i], finfo.filename());
    }

    // 4. delete file, 删除file0~file4，and the fileinfo will be "" 
    for (int i = 0; i < keyMap.size() / 2; i++) {
        ASSERT_EQ(0, client_->Delete(keyMap[i]));
        // can not get delete file
        std::string out;
        ASSERT_EQ(-1, client_->Get(keyMap[i], &out));
        client_->Get(keyMap[i], &out);
        FileInfo fileinfo;
        ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
        ASSERT_EQ("", fileinfo.filename());
    }

    // 5. rename file: rename file9 ~ file10, file10本来不存在
    Operation op1{OpType::OpDelete, const_cast<char *>(keyMap[9].c_str()),
                  const_cast<char *>(fileInfo9.c_str()),
                  static_cast<int>(keyMap[9].size()),
                  static_cast<int>(fileInfo9.size())};
    Operation op2{OpType::OpPut, const_cast<char *>(fileKey10.c_str()),
                  const_cast<char *>(fileInfo10.c_str()),
                  static_cast<int>(fileKey10.size()),
                  static_cast<int>(fileInfo10.size())};
    std::vector<Operation> ops{op1, op2};
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));

    std::string out;
    // cannot get file9 
    client_->Get(keyMap[9], &out);
    ASSERT_EQ(-1, client_->Get(keyMap[9], &out));
    
    // get file10 ok
    ASSERT_EQ(0, client_->Get(fileKey10, &out));
    FileInfo fileinfo;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(fileName10, fileinfo.filename());
    
    // 6. snapshot of keyMap[6]
    // 事务开始
    Operation op3{OpType::OpPut, const_cast<char *>(keyMap[6].c_str()),
                  const_cast<char *>(fileInfo6.c_str()),
                  static_cast<int>(keyMap[6].size()),
                  static_cast<int>(fileInfo6.size())};
    Operation op4{OpType::OpPut, const_cast<char *>(snapshotKey6.c_str()),
                  const_cast<char *>(snapshotInfo6.c_str()),
                  static_cast<int>(snapshotKey6.size()),
                  static_cast<int>(snapshotInfo6.size())};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op4);
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));

    // get file6 ok
    ASSERT_EQ(0, client_->Get(keyMap[6], &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(2, fileinfo.seqnum());
    ASSERT_EQ(fileName[6], fileinfo.filename());
    // get snapshot6
    ASSERT_EQ(0, client_->Get(snapshotKey6, &out));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfo));
    ASSERT_EQ(1, fileinfo.seqnum());
    ASSERT_EQ(snapshotName6, fileinfo.filename());
    // list snapshotfile
    ASSERT_EQ(0, client_->List("03", "04", &listRes));
    ASSERT_EQ(1, listRes.size());
    LOG(INFO)<<listRes[0];



    // 7. compare and swap
    std::string outforCAS;
    ASSERT_EQ(0, client_->CompareAndSwap("04", "", "100"));
    ASSERT_EQ(0, client_->Get("04",&outforCAS));
    LOG(INFO) << "outforCAS: " << outforCAS;
    ASSERT_EQ("100",  outforCAS);

    ASSERT_EQ(0, client_->CompareAndSwap("04", "100", "200"));
    ASSERT_EQ(0, client_->Get("04",&outforCAS));
    ASSERT_EQ("200",  outforCAS);
    LOG(INFO) << "test_CAS";

    // 8. rename file: rename file7 ~ file8
    Operation op8{OpType::OpDelete, const_cast<char *>(keyMap[7].c_str()),
                  const_cast<char *>(""), static_cast<int>(keyMap[7].size()),
                  0};
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
                  const_cast<char *>(encodeNewFileInfo7Key.c_str()),
                  const_cast<char *>(encodeNewFileInfo7.c_str()),
                  static_cast<int>(encodeNewFileInfo7Key.size()),
                  static_cast<int>(encodeNewFileInfo7.size())};
    ops.clear();
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(EtcdErrCode::EtcdOK, client_->TxnN(ops));

    std:: string outforRename;
    // 不能获取 file7
    ASSERT_EQ(-1, client_->Get(keyMap[7], &outforRename));
    LOG(INFO) << "out: " << outforRename;
    // 成功获取rename以后的file7
    ASSERT_EQ(0, client_->Get(keyMap[8], &outforRename));
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(outforRename, &fileinfo));
    ASSERT_EQ(newFileInfo7.filename(), fileinfo.filename());
    ASSERT_EQ(newFileInfo7.filetype(), fileinfo.filetype());

    // 9. test more Txn err
    ops.emplace_back(op8);
    ops.emplace_back(op9);
    ASSERT_EQ(-1, client_->TxnN(ops));

     Operation op5{OpType(5), const_cast<char *>(snapshotKey6.c_str()),
                  const_cast<char *>(snapshotInfo6.c_str()),
                  static_cast<int>(snapshotKey6.size()),
                  static_cast<int>(snapshotInfo6.size())};
    ops.clear();
    ops.emplace_back(op3);
    ops.emplace_back(op5);
    ASSERT_EQ(-1, client_->TxnN(ops));

    client_->CloseClient();

}

TEST_F(TestMysqlClinetImp, test_ListWithLimitAndRevision) {
    LOG(INFO) << "test_ListWithLimitAndRevision";
    // 准备一批数据
    // "011" "013" "015" "017" "019"
    for (int i = 1; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(0, client_->Put(key, value));
    }

    // "012" "014" "016" "018"
    for (int i = 2; i <= 9; i += 2) {
        std::string key = std::string("01") + std::to_string(i);
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(0, client_->Put(key, value));
    }

    // 获取当前revision
    // 通过GetCurrentRevision获取
    int64_t curRevision;
    ASSERT_EQ(0, client_->GetCurrentRevision(&curRevision));
    LOG(INFO) << "get current revision: " << curRevision;

    // 根据当前revision获取前5个key-value
    std::vector<std::string> out;
    std::string lastKey;
    int res = client_->ListWithLimitAndRevision("01", "02", 5, curRevision, &out,
                                                &lastKey);
    ASSERT_EQ(0, res);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ("015", lastKey);
    for (int i = 1; i <= 5; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 1]);
    }

    // 根据当前revision获取后5个key-value
    out.clear();
    res = client_->ListWithLimitAndRevision(lastKey, "02", 5, curRevision, &out,
                                            &lastKey);
    ASSERT_EQ(5, out.size());
    ASSERT_EQ(0, res);
    ASSERT_EQ("019", lastKey);
    for (int i = 5; i <= 9; i++) {
        std::string value = std::string("test") + std::to_string(i);
        ASSERT_EQ(value, out[i - 5]);
    }
}

TEST_F(TestMysqlClinetImp, test_return_with_revision) {
    int64_t startRevision;
    int res = client_->GetCurrentRevision(&startRevision);
    ASSERT_EQ(0, res);

    int64_t revision;
    res = client_->PutRewithRevision("hello", "everyOne", &revision);
    ASSERT_EQ(0, res);
    ASSERT_EQ(startRevision + 1, revision);
    std::string out;
    client_->Get("hello", &out);
    ASSERT_EQ("everyOne", out);
    res = client_->DeleteRewithRevision("hello", &revision);
    ASSERT_EQ(0, res);
    ASSERT_EQ(startRevision + 2, revision);
}

}  // namespace mysqlstorage
}  // namespace curve