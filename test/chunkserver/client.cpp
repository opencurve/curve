/*
 * Project: curve
 * Created Date: 18-8-24
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include "src/chunkserver/copyset_node.h"
#include "proto/chunk.pb.h"
#include "proto/copyset.pb.h"
#include "src/chunkserver/cli.h"
#include "test/chunkserver/chunkserver_test_util.h"

DEFINE_int32(request_size, 10, "Size of each requst");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(election_timeout_ms, 3000, "election timeout ms");
DEFINE_int32(write_percentage, 100, "Percentage of fetch_add");
DEFINE_string(confs,
              "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Configuration of the raft group");

using curve::chunkserver::CopysetRequest;
using curve::chunkserver::CopysetResponse;
using curve::chunkserver::CopysetService_Stub;
using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::ChunkService_Stub;
using curve::chunkserver::PeerId;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::CopysetID;
using curve::chunkserver::Configuration;
using curve::chunkserver::CHUNK_OP_TYPE;
using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::COPYSET_OP_STATUS;

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);


    LogicPoolID logicPoolId = 1;
    CopysetID copysetId     = 100001;
    uint64_t chunkId        = 1;
    uint64_t sn             = 1;
    char fillCh             = 'a';
    PeerId leader;
    Configuration conf;

    if (0 != conf.parse_from(FLAGS_confs)) {
        LOG(FATAL) << "conf parse failed: " << FLAGS_confs;
    }



    // 创建 copyset
    {
        std::vector<PeerId> peers;
        conf.list_peers(&peers);
        CopysetRequest request;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);

        for (auto it = peers.begin(); it != peers.end(); ++it) {
            request.add_peerid((*it).to_string());
        }

        for (auto it = peers.begin(); it != peers.end(); ++it) {
            brpc::Channel channel;
            if (0 != channel.Init(it->addr, NULL)) {
                LOG(FATAL) << "channel init failed: " << strerror(errno);
            }

            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            CopysetResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);

            for (auto it = peers.begin(); it != peers.end(); ++it) {
                request.add_peerid((*it).to_string());
            }
            CopysetService_Stub copysetStub(&channel);

            copysetStub.CreateCopysetNode(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                LOG(FATAL) << "create copyset fialed: " << cntl.ErrorText();
            }
            if (response.status() == COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS       //NOLINT
                || response.status() == COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST) {   //NOLINT
                LOG(INFO) << "create copyset success: " << response.status();
            } else {
                LOG(FATAL) << "create copyset failed: ";
            }
        }
    }

    // wait leader
    ::usleep(1000 * FLAGS_election_timeout_ms);
    butil::Status status = curve::chunkserver::WaitLeader(logicPoolId,
                                                          copysetId,
                                                          conf,
                                                          &leader,
                                                          FLAGS_election_timeout_ms);   //NOLINT
    LOG(INFO) << "leader is: " << leader.to_string();
    if (0 != status.error_code()) {
        LOG(FATAL) << "Wait leader failed";
    }

    {
        // read/write
        brpc::Channel channel;
        if (0 != channel.Init(leader.addr, NULL)) {
            LOG(FATAL) << "channel init failed: " << strerror(errno);
        }
        ChunkService_Stub chunkStub(&channel);
        /* Write */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(0);
            request.set_size(FLAGS_request_size);
            cntl.request_attachment().resize(FLAGS_request_size, fillCh);
            chunkStub.WriteChunk(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                LOG(INFO) << "write failed: " << cntl.ErrorText();
            }
            LOG(INFO) << "write status: " << response.status();
        }
        /* Read */
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_sn(sn);
            request.set_offset(0);
            request.set_size(FLAGS_request_size);
            request.set_appliedindex(1);
            chunkStub.ReadChunk(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                LOG(INFO) << "read failed: " << cntl.ErrorText();
            }
            LOG(INFO) << "read status: " << response.status() << " "
                      << cntl.response_attachment().to_string();
        }
    }


    return 0;
}


