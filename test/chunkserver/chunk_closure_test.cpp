/*
 * Project: curve
 * Created Date: 18-11-15
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <brpc/controller.h>

#include <algorithm>
#include <memory>

#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

class FakeCopysetNode : public CopysetNode {
 public:
    FakeCopysetNode(const LogicPoolID &logicPoolId,
                    const CopysetID &copysetId,
                    const Configuration &initConf,
                    std::unique_ptr<CSDataStore> dsPtr)
        : CopysetNode(logicPoolId, copysetId, initConf, std::move(dsPtr)) {
    }

    void RedirectChunkRequest(ChunkResponse *response) {
        LOG(INFO) << "FakeCopysetNode RedirectChunkRequest()";
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
    }
};

class FakeClosure : public braft::Closure {
 public:
    void Run() {
        LOG(INFO) << "FakeClosure run";
    }
};

TEST(ChunkClosureTest, error_test) {
    brpc::Controller cntl;
    ChunkRequest request;
    LogicPoolID logicPoolID = 1;
    CopysetID copysetID = 1;
    request.set_logicpoolid(logicPoolID);
    request.set_copysetid(copysetID);
    request.set_chunkid(1);
    request.set_offset(1);
    request.set_size(1);
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    ChunkResponse response;
    FakeClosure *done = new FakeClosure();
    ChunkOpRequest *opRequest = new ChunkOpRequest(nullptr,
                                                   &cntl,
                                                   &request,
                                                   &response, done);
    Configuration conf;
    std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
    FakeCopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
    ChunkClosure *chunkClosure = new ChunkClosure(&copysetNode,
                                                  opRequest);
    chunkClosure->status().set_error(-1, "error");
    chunkClosure->Run();
    ASSERT_EQ(response.status(), CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
}

}  // namespace chunkserver
}  // namespace curve
