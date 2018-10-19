/*
 * Project: curve
 * File Created: Wednesday, 3rd October 2018 5:08:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"

#include "include/client/libcurve.h"
#include "src/client/client_common.h"

DEFINE_string(mds_addr, "127.0.0.1:9000", "mds addr");
DEFINE_string(file_name, "vdisk_001", "file name");
DEFINE_uint64(file_size, 1 * 1024 * 1024 * 1024, "file size");

using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    brpc::Channel channel;
    if (channel.Init(FLAGS_mds_addr.c_str(), nullptr) != 0) {
        LOG(FATAL) << "Init channel  failed!";
        return CreateFileErrorType::FILE_CREATE_FAILED;
    }
    curve::mds::CurveFSService_Stub stub(&channel);

    // CreateFile
    curve::mds::CreateFileRequest request;
    curve::mds::CreateFileResponse response;
    brpc::Controller cntl;

    request.set_filename(FLAGS_file_name);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    request.set_filelength(FLAGS_file_size);

    cntl.set_log_id(1);  // TODO(tongguangxun) : specify the log id usage
    stub.CreateFile(&cntl, &request, &response, NULL);

    if (cntl.Failed()) {
        LOG(ERROR) << "Create file failed, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
    }
    if (response.has_statuscode()) {
        if (response.statuscode() == curve::mds::StatusCode::kFileExists) {
            LOG(INFO) << "file already exists!";
        } else if (response.statuscode() == curve::mds::StatusCode::kOK) {
            LOG(INFO) << "Create file success!";
        } else {
            LOG(INFO) << "Create file failed, "
                      << cntl.ErrorText();
        }
    }
    return 0;
}
