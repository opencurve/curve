/*
 * Project: curve
 * Created Date: Wednesday June 19th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>

#include "src/common/configuration.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/fs/local_filesystem.h"
#include "test/chunkserver/datastore/chunkfilepool_helper.h"

namespace curve {
namespace chunkserver {

butil::AtExitManager atExitManager;

#define IP "127.0.0.1"
#define PORT 9401

const uint64_t kMB = 1024 * 1024;
const ChunkSizeType CHUNK_SIZE = 4 * kMB;
const PageSizeType PAGE_SIZE = 4 * 1024;
const int chunkNum = 10;
const LogicPoolID logicId = 1;

const string baseDir = "./data_csmetric";    // NOLINT
const string copysetDir = "local://./data_csmetric";  // NOLINT
const string poolDir = "./chunkfilepool_csmetric";  // NOLINT
const string poolMetaPath = "./chunkfilepool_csmetric.meta";  // NOLINT

class CSMetricTest : public ::testing::Test {
 public:
    CSMetricTest() {}
    ~CSMetricTest() {}

    void InitChunkFilePool() {
        ChunkfilePoolHelper::PersistEnCodeMetaInfo(lfs_,
                                                   CHUNK_SIZE,
                                                   PAGE_SIZE,
                                                   poolDir,
                                                   poolMetaPath);
        ChunkfilePoolOptions cfop;
        cfop.chunkSize = CHUNK_SIZE;
        cfop.metaPageSize = PAGE_SIZE;
        memcpy(cfop.metaPath, poolMetaPath.c_str(), poolMetaPath.size());

        if (lfs_->DirExists(poolDir))
            lfs_->Delete(poolDir);
        allocateChunk(lfs_, chunkNum, poolDir, CHUNK_SIZE);
        ASSERT_TRUE(chunkfilePool_->Initialize(cfop));
        ASSERT_EQ(chunkNum, chunkfilePool_->Size());
    }

    void InitCopysetManager() {
        CopysetNodeOptions copysetNodeOptions;
        copysetNodeOptions.ip = IP;
        copysetNodeOptions.port = PORT;
        copysetNodeOptions.snapshotIntervalS = 30;
        copysetNodeOptions.catchupMargin = 50;
        copysetNodeOptions.chunkDataUri = "local://./data_csmetric";
        copysetNodeOptions.chunkSnapshotUri = "local://./data_csmetric";
        copysetNodeOptions.logUri = "local://./data_csmetric";
        copysetNodeOptions.raftMetaUri = "local://./data_csmetric";
        copysetNodeOptions.raftSnapshotUri = "local://./data_csmetric";
        copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
        copysetNodeOptions.localFileSystem = lfs_;
        copysetNodeOptions.chunkfilePool = chunkfilePool_;
        copysetNodeOptions.maxChunkSize = CHUNK_SIZE;
        ASSERT_EQ(0, copysetMgr_->Init(copysetNodeOptions));
        ASSERT_EQ(0, copysetMgr_->Run());

        butil::EndPoint addr(butil::IP_ANY, PORT);
        ASSERT_EQ(0, copysetMgr_->AddService(&server_, addr));
        if (server_.Start(PORT, NULL) != 0) {
            LOG(FATAL) << "Fail to start Server";
        }
    }

    void InitChunkServerMetric() {
        ChunkServerMetricOptions metricOptions;
        metricOptions.port = PORT;
        metricOptions.ip = IP;
        metricOptions.collectMetric = true;
        metric_ = ChunkServerMetric::GetInstance();
        metric_->Init(metricOptions);
        metric_->MonitorChunkFilePool(chunkfilePool_.get());
    }

    void CreateConfigFile() {
        confFile_  = "csmetric.conf";
       // 创建配置文件
        std::string confItem;
        std::ofstream cFile(confFile_);
        ASSERT_TRUE(cFile.is_open());
        confItem = "chunksize=1234\n";
        cFile << confItem;
        confItem = "timeout=100\n";
        cFile << confItem;
    }

    void SetUp() {
        copysetMgr_ = &CopysetNodeManager::GetInstance();
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ASSERT_NE(lfs_, nullptr);
        chunkfilePool_ = std::make_shared<ChunkfilePool>(lfs_);
        ASSERT_NE(chunkfilePool_, nullptr);

        InitChunkFilePool();
        InitCopysetManager();
        InitChunkServerMetric();
        CreateConfigFile();
    }

    void TearDown() {
        ASSERT_EQ(0, metric_->Fini());
        lfs_->Delete(poolDir);
        lfs_->Delete(baseDir);
        lfs_->Delete(poolMetaPath);
        lfs_->Delete(confFile_);
        chunkfilePool_->UnInitialize();
        ASSERT_EQ(0, copysetMgr_->Fini());
        ASSERT_EQ(0, server_.Stop(0));
        ASSERT_EQ(0, server_.Join());
    }

 protected:
    brpc::Server server_;
    CopysetNodeManager* copysetMgr_;
    std::shared_ptr<ChunkfilePool> chunkfilePool_;
    std::shared_ptr<LocalFileSystem> lfs_;
    ChunkServerMetric* metric_;
    std::string confFile_;
};

TEST_F(CSMetricTest, CopysetMetricTest) {
    CopysetID copysetId = 1;
    int rc = metric_->CreateCopysetMetric(logicId, copysetId);
    ASSERT_EQ(rc, 0);

    // 如果copyset的metric已经存在，返回-1
    rc = metric_->CreateCopysetMetric(logicId, copysetId);
    ASSERT_EQ(rc, -1);

    // 获取不存在的copyset metric，返回nullptr
    CopysetMetricPtr copysetMetric = metric_->GetCopysetMetric(logicId, 2);
    ASSERT_EQ(copysetMetric, nullptr);

    copysetMetric = metric_->GetCopysetMetric(logicId, copysetId);
    ASSERT_NE(copysetMetric, nullptr);

    // 删除copyset metric后，再去获取返回nullptr
    rc = metric_->RemoveCopysetMetric(logicId, copysetId);
    ASSERT_EQ(rc, 0);
    copysetMetric = metric_->GetCopysetMetric(logicId, copysetId);
    ASSERT_EQ(copysetMetric, nullptr);
}

TEST_F(CSMetricTest, OnRequestTest) {
    CopysetID copysetId = 1;
    int rc = metric_->CreateCopysetMetric(logicId, copysetId);
    ASSERT_EQ(rc, 0);

    CopysetMetricPtr copysetMetric = metric_->GetCopysetMetric(logicId, copysetId);  // NOLINT
    ASSERT_NE(copysetMetric, nullptr);

    const IOMetricPtr serverWriteMetric = metric_->GetWriteMetric();
    const IOMetricPtr serverReadMetric = metric_->GetReadMetric();
    const IOMetricPtr cpWriteMetric = copysetMetric->GetWriteMetric();
    const IOMetricPtr cpReadMetric = copysetMetric->GetReadMetric();
    const IOMetricPtr cpRecoverMetric = copysetMetric->GetRecoverMetric();
    const IOMetricPtr cpPasteMetric = copysetMetric->GetPasteMetric();
    const IOMetricPtr cpDownloadMetric = copysetMetric->GetDownloadMetric();

    // 统计写入成功的情况
    metric_->OnRequestWrite(logicId, copysetId);
    ASSERT_EQ(1, serverWriteMetric->reqNum_.get_value());
    ASSERT_EQ(0, serverWriteMetric->ioNum_.get_value());
    ASSERT_EQ(0, serverWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, serverWriteMetric->errorNum_.get_value());
    ASSERT_EQ(1, cpWriteMetric->reqNum_.get_value());
    ASSERT_EQ(0, cpWriteMetric->ioNum_.get_value());
    ASSERT_EQ(0, cpWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpWriteMetric->errorNum_.get_value());

    // 统计读取成功的情况
    metric_->OnRequestRead(logicId, copysetId);
    ASSERT_EQ(1, serverReadMetric->reqNum_.get_value());
    ASSERT_EQ(0, serverReadMetric->ioNum_.get_value());
    ASSERT_EQ(0, serverReadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, serverReadMetric->errorNum_.get_value());
    ASSERT_EQ(1, cpReadMetric->reqNum_.get_value());
    ASSERT_EQ(0, cpReadMetric->ioNum_.get_value());
    ASSERT_EQ(0, cpReadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpReadMetric->errorNum_.get_value());

    // 统计恢复成功的情况
    metric_->OnRequestRecover(logicId, copysetId);
    ASSERT_EQ(1, cpRecoverMetric->reqNum_.get_value());
    ASSERT_EQ(0, cpRecoverMetric->ioNum_.get_value());
    ASSERT_EQ(0, cpRecoverMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpRecoverMetric->errorNum_.get_value());

    // 统计paste成功的情况
    metric_->OnRequestPaste(logicId, copysetId);
    ASSERT_EQ(1, cpPasteMetric->reqNum_.get_value());
    ASSERT_EQ(0, cpPasteMetric->ioNum_.get_value());
    ASSERT_EQ(0, cpPasteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpPasteMetric->errorNum_.get_value());

    // 统计下载成功的情况
    metric_->OnRequestDownload(logicId, copysetId);
    ASSERT_EQ(1, cpDownloadMetric->reqNum_.get_value());
    ASSERT_EQ(0, cpDownloadMetric->ioNum_.get_value());
    ASSERT_EQ(0, cpDownloadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpDownloadMetric->errorNum_.get_value());
}

TEST_F(CSMetricTest, OnResponseTest) {
    CopysetID copysetId = 1;
    int rc = metric_->CreateCopysetMetric(logicId, copysetId);
    ASSERT_EQ(rc, 0);

    CopysetMetricPtr copysetMetric = metric_->GetCopysetMetric(logicId, copysetId);  // NOLINT
    ASSERT_NE(copysetMetric, nullptr);

    const IOMetricPtr serverWriteMetric = metric_->GetWriteMetric();
    const IOMetricPtr serverReadMetric = metric_->GetReadMetric();
    const IOMetricPtr cpWriteMetric = copysetMetric->GetWriteMetric();
    const IOMetricPtr cpReadMetric = copysetMetric->GetReadMetric();
    const IOMetricPtr cpRecoverMetric = copysetMetric->GetRecoverMetric();
    const IOMetricPtr cpPasteMetric = copysetMetric->GetPasteMetric();
    const IOMetricPtr cpDownloadMetric = copysetMetric->GetDownloadMetric();

    size_t size = PAGE_SIZE;
    int64_t latUs = 100;
    bool hasError = false;
    // 统计写入成功的情况
    metric_->OnResponseWrite(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, serverWriteMetric->reqNum_.get_value());
    ASSERT_EQ(1, serverWriteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, serverWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, serverWriteMetric->errorNum_.get_value());
    ASSERT_EQ(0, cpWriteMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpWriteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpWriteMetric->errorNum_.get_value());

    // 统计读取成功的情况
    metric_->OnResponseRead(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, serverReadMetric->reqNum_.get_value());
    ASSERT_EQ(1, serverReadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, serverReadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, serverReadMetric->errorNum_.get_value());
    ASSERT_EQ(0, cpReadMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpReadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpReadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpReadMetric->errorNum_.get_value());

    // 统计恢复成功的情况
    metric_->OnResponseRecover(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpRecoverMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpRecoverMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpRecoverMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpRecoverMetric->errorNum_.get_value());

    // 统计paste成功的情况
    metric_->OnResponsePaste(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpPasteMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpPasteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpPasteMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpPasteMetric->errorNum_.get_value());

    // 统计下载成功的情况
    metric_->OnResponseDownload(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpDownloadMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpDownloadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpDownloadMetric->ioBytes_.get_value());
    ASSERT_EQ(0, cpDownloadMetric->errorNum_.get_value());

    hasError = true;
    // 统计写入失败的情况，错误数增加，其他不变
    metric_->OnResponseWrite(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, serverWriteMetric->reqNum_.get_value());
    ASSERT_EQ(1, serverWriteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, serverWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(1, serverWriteMetric->errorNum_.get_value());
    ASSERT_EQ(0, cpWriteMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpWriteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpWriteMetric->ioBytes_.get_value());
    ASSERT_EQ(1, cpWriteMetric->errorNum_.get_value());

    // 统计读取失败的情况，错误数增加，其他不变
    metric_->OnResponseRead(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, serverReadMetric->reqNum_.get_value());
    ASSERT_EQ(1, serverReadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, serverReadMetric->ioBytes_.get_value());
    ASSERT_EQ(1, serverReadMetric->errorNum_.get_value());
    ASSERT_EQ(0, cpReadMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpReadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpReadMetric->ioBytes_.get_value());
    ASSERT_EQ(1, cpReadMetric->errorNum_.get_value());

     // 统计恢复失败的情况
    metric_->OnResponseRecover(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpRecoverMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpRecoverMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpRecoverMetric->ioBytes_.get_value());
    ASSERT_EQ(1, cpRecoverMetric->errorNum_.get_value());

    // 统计paste失败的情况
    metric_->OnResponsePaste(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpPasteMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpPasteMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpPasteMetric->ioBytes_.get_value());
    ASSERT_EQ(1, cpPasteMetric->errorNum_.get_value());

    // 统计下载失败的情况
    metric_->OnResponseDownload(logicId, copysetId, size, latUs, hasError);
    ASSERT_EQ(0, cpDownloadMetric->reqNum_.get_value());
    ASSERT_EQ(1, cpDownloadMetric->ioNum_.get_value());
    ASSERT_EQ(PAGE_SIZE, cpDownloadMetric->ioBytes_.get_value());
    ASSERT_EQ(1, cpDownloadMetric->errorNum_.get_value());
}

TEST_F(CSMetricTest, CountTest) {
    // 初始状态下，没有copyset，chunkfilepool中有chunkNum个chunk
    ASSERT_EQ(0, metric_->GetCopysetCount());
    ASSERT_EQ(10, metric_->GetChunkLeftCount());

    // 创建copyset
    Configuration conf;
    CopysetID copysetId;
    ASSERT_TRUE(copysetMgr_->CreateCopysetNode(logicId, copysetId, conf));
    ASSERT_EQ(1, metric_->GetCopysetCount());
    // 此时copyset下面没有chunk和快照
    CopysetMetricPtr copysetMetric = metric_->GetCopysetMetric(logicId, copysetId);  // NOLINT
    ASSERT_EQ(0, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(0, copysetMetric->GetCloneChunkCount());

    // 写入数据生成chunk
    std::shared_ptr<CopysetNode> node =
        copysetMgr_->GetCopysetNode(logicId, copysetId);
    std::shared_ptr<CSDataStore> datastore = node->GetDataStore();
    ChunkID id = 1;
    SequenceNum seq = 1;
    char buf[PAGE_SIZE] = {0};
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    ASSERT_EQ(CSErrorCode::Success,
              datastore->WriteChunk(id, seq, buf, offset, length, nullptr));
    ASSERT_EQ(1, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(0, copysetMetric->GetCloneChunkCount());

    // 增加版本号，生成快照
    seq = 2;
    ASSERT_EQ(CSErrorCode::Success,
              datastore->WriteChunk(id, seq, buf, offset, length, nullptr));
    ASSERT_EQ(1, copysetMetric->GetChunkCount());
    ASSERT_EQ(1, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(0, copysetMetric->GetCloneChunkCount());

    // 删除快照
    ASSERT_EQ(CSErrorCode::Success,
              datastore->DeleteSnapshotChunkOrCorrectSn(id, seq));
    ASSERT_EQ(1, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(0, copysetMetric->GetCloneChunkCount());

    // 创建 clone chunk
    ChunkID id2 = 2;
    ChunkID id3 = 3;
    std::string location = "test@cs";
    ASSERT_EQ(CSErrorCode::Success,
              datastore->CreateCloneChunk(id2, 1, 0, CHUNK_SIZE, location));
    ASSERT_EQ(CSErrorCode::Success,
              datastore->CreateCloneChunk(id3, 1, 0, CHUNK_SIZE, location));
    ASSERT_EQ(3, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(2, copysetMetric->GetCloneChunkCount());

    // clone chunk被覆盖写一遍,clone chun转成普通chunk
    char* buf2 = new char[CHUNK_SIZE];
    ASSERT_EQ(CSErrorCode::Success,
              datastore->WriteChunk(id2, 1, buf2, 0, CHUNK_SIZE, nullptr));
    delete[] buf2;
    ASSERT_EQ(3, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(1, copysetMetric->GetCloneChunkCount());

    // 删除上面的chunk
    ASSERT_EQ(CSErrorCode::Success,
              datastore->DeleteChunk(id2, 1));
    ASSERT_EQ(2, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(1, copysetMetric->GetCloneChunkCount());

    // 模拟copyset重新加载datastore,重新初始化后，chunk数量不变
    // for bug fix: CLDCFS-1473
    datastore->Initialize();
    ASSERT_EQ(2, copysetMetric->GetChunkCount());
    ASSERT_EQ(0, copysetMetric->GetSnapshotCount());
    ASSERT_EQ(1, copysetMetric->GetCloneChunkCount());

    // 测试leader count计数
    ASSERT_EQ(0, metric_->GetLeaderCount());
    metric_->IncreaseLeaderCount();
    ASSERT_EQ(1, metric_->GetLeaderCount());
    metric_->DecreaseLeaderCount();
    ASSERT_EQ(0, metric_->GetLeaderCount());
}

TEST_F(CSMetricTest, ConfigTest) {
    common::Configuration conf;
    conf.SetConfigPath(confFile_);
    int ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);
    metric_->UpdateConfigMetric(&conf);

    std::string prefix = "chunkserver_127_0_0_1_9401_config_";
    ASSERT_STREQ(bvar::Variable::describe_exposed(prefix + "chunksize").c_str(),
                 "{\"conf_name\":\"chunksize\",\"conf_value\":\"1234\"}");
    ASSERT_STREQ(bvar::Variable::describe_exposed(prefix + "timeout").c_str(),
                 "{\"conf_name\":\"timeout\",\"conf_value\":\"100\"}");
    // 修改新增配置信息
    conf.SetStringValue("chunksize", "4321");
    conf.SetStringValue("port", "9999");
    metric_->UpdateConfigMetric(&conf);
    // // 验证修改后信息
    ASSERT_STREQ(bvar::Variable::describe_exposed(prefix + "chunksize").c_str(),
                 "{\"conf_name\":\"chunksize\",\"conf_value\":\"4321\"}");
    ASSERT_STREQ(bvar::Variable::describe_exposed(prefix + "timeout").c_str(),
                 "{\"conf_name\":\"timeout\",\"conf_value\":\"100\"}");
    ASSERT_STREQ(bvar::Variable::describe_exposed(prefix + "port").c_str(),
                 "{\"conf_name\":\"port\",\"conf_value\":\"9999\"}");
}

TEST_F(CSMetricTest, OnOffTest) {
    ASSERT_EQ(0, metric_->Fini());
    ChunkServerMetricOptions metricOptions;
    metricOptions.port = PORT;
    metricOptions.ip = IP;
    // 关闭metric开关后进行初始化
    {
        metricOptions.collectMetric = false;
        ASSERT_EQ(0, metric_->Init(metricOptions));
        metric_->MonitorChunkFilePool(chunkfilePool_.get());
        common::Configuration conf;
        conf.SetConfigPath(confFile_);
        int ret = conf.LoadConfig();
        ASSERT_EQ(ret, true);
        metric_->UpdateConfigMetric(&conf);
    }
    // 初始化后获取所有指标项都为空
    {
        ASSERT_EQ(metric_->GetReadMetric(), nullptr);
        ASSERT_EQ(metric_->GetWriteMetric(), nullptr);
        ASSERT_EQ(metric_->GetCopysetCount(), 0);
        ASSERT_EQ(metric_->GetLeaderCount(), 0);
        ASSERT_EQ(metric_->GetChunkLeftCount(), 0);
    }
    // 创建copyset的metric返回成功，但实际并未创建
    {
        CopysetID copysetId = 1;
        ASSERT_EQ(0, metric_->CreateCopysetMetric(logicId, copysetId));
        ASSERT_EQ(nullptr, metric_->GetCopysetMetric(logicId, copysetId));
        metric_->OnResponseRead(logicId, copysetId, PAGE_SIZE, 100, true);
        metric_->OnResponseWrite(logicId, copysetId, PAGE_SIZE, 100, false);
        ASSERT_EQ(0, metric_->RemoveCopysetMetric(logicId, copysetId));
    }
    // 增加leader count，但是实际未计数
    {
        metric_->IncreaseLeaderCount();
        ASSERT_EQ(metric_->GetLeaderCount(), 0);
        metric_->DecreaseLeaderCount();
        ASSERT_EQ(metric_->GetLeaderCount(), 0);
    }
}

}  // namespace chunkserver
}  // namespace curve
