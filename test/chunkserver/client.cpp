/*
 * Project: curve
 * Created Date: 18-8-24
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include "src/chunkserver/copyset_node.h"
#include "proto/chunk.pb.h"
#include "src/chunkserver/cli.h"
#include "src/chunkserver/type.h"

DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(request_size, 10, "Size of each requst");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(write_percentage, 100, "Percentage of fetch_add");
DEFINE_string(conf, "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0", "Configuration of the raft group");
DEFINE_string(group, "Block", "Id of the replication group");

bvar::LatencyRecorder g_latency_recorder("block_client");

using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::PeerId;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::CopysetID;
using curve::chunkserver::Configuration;
using curve::chunkserver::CHUNK_OP_TYPE;
using curve::chunkserver::CHUNK_OP_STATUS;

static void *sender(void *arg) {
    uint64_t loop = 1;
    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    Configuration conf;
    conf.parse_from(FLAGS_conf);

    butil::Status status = curve::chunkserver::GetLeader(logicPoolId, copysetId, conf, &leader);
    if (status.ok()) {
        std::cout << "leader id " << leader.to_string() << std::endl;
    } else {
        LOG(FATAL) << "get leader failed" << std::endl;
    }

    // Now we known who is the leader, construct Stub and then sending
    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << leader;
    }

    while (!brpc::IsAskedToQuit()) {
        curve::chunkserver::ChunkService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);

        ChunkRequest request;
        ChunkResponse response;
        uint64_t chunkId = 1;

        if (1 == loop % 2) {
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_size(FLAGS_request_size);
            cntl.request_attachment().resize(FLAGS_request_size, 'a');
            stub.WriteChunk(&cntl, &request, &response, nullptr);
        } else {
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(0);
            request.set_size(FLAGS_request_size);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS != response.status()) {
            if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == response.status()) {
                LOG(WARNING) << "Fail to send request to " << leader
                             << ", redirecting to "
                             << (response.has_redirect() ? response.redirect().c_str() : "nowhere");
                break;
            }
        } else {
            LOG(WARNING)
            << "response " << response.status() << ", response data: " << cntl.response_attachment().to_string();
        }
        bthread_usleep(FLAGS_timeout_ms * 1000L);
        ++loop;
    }
    LOG(WARNING) << "Write stop";
    return NULL;
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<bthread_t> tids;
    tids.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread) {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&tids[i], NULL, sender, NULL) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "Block client is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(tids[i], NULL);
        } else {
            bthread_join(tids[i], NULL);
        }
    }

    return 0;
}


