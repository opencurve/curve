/*
 * Project:
 * Created Date: Mon Sep 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include <gflags/gflags.h>
#include <brpc/channel.h>

#include "proto/topology.pb.h"

DEFINE_string(attachment, "foo", "Carry this along with requests");
DEFINE_string(protocol,
    "baidu_std",
    "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type,
    "",
    "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8000", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 1000, "Milliseconds between consecutive requests");
DEFINE_string(http_content_type,
    "application/json",
    "Content type of http request");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(),
            FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    curve::mds::topology::TopologyService_Stub stub(&channel);

    // Send a request and wait for the response every 1 second.
    int log_id = 0;
    while (!brpc::IsAskedToQuit()) {
        // We will receive response synchronously, safe to put variables
        // on stack.
        curve::mds::topology::PhysicalPoolRequest request;
        curve::mds::topology::PhysicalPoolResponse response;
        brpc::Controller cntl;

        request.set_physicalpoolname("default");
        request.set_desc("just for test");

        cntl.set_log_id(log_id ++);  // set by user
        if (FLAGS_protocol != "http" && FLAGS_protocol != "h2c")  {
            // Set attachment which is wired to network directly instead of
            // being serialized into protobuf messages.
            cntl.request_attachment().append(FLAGS_attachment);
        } else {
            cntl.http_request().set_content_type(FLAGS_http_content_type);
        }

        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).
        stub.CreatePhysicalPool(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            LOG(INFO) << "Received response from " << cntl.remote_side()
                << " to " << cntl.local_side()
                << ": " << " (attached="
                << cntl.response_attachment() << ")"
                << " latency=" << cntl.latency_us() << "us";
        } else {
            LOG(WARNING) << cntl.ErrorText();
        }
        usleep(FLAGS_interval_ms * 1000L);
    }

    LOG(INFO) << "Client is going to quit";
    return 0;
}

