/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-12-23
 * Author: chengyi01
 */

#include "curvefs/src/tools/create/curvefs_create_fs.h"

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"

DECLARE_string(fsName);
DECLARE_string(confPath);
DECLARE_string(mdsAddr);
DECLARE_uint64(blockSize);
DECLARE_string(fsType);
DECLARE_uint64(volumeSize);
DECLARE_uint64(volumeBlockSize);
DECLARE_string(volumeName);
DECLARE_string(volumeUser);
DECLARE_string(volumePassword);
DECLARE_string(s3_ak);
DECLARE_string(s3_sk);
DECLARE_string(s3_endpoint);
DECLARE_string(s3_bucket_name);
DECLARE_uint64(s3_blocksize);
DECLARE_uint64(s3_chunksize);
DECLARE_uint32(rpcTimeoutMs);
DECLARE_uint32(rpcRetryTimes);

namespace curvefs {
namespace tools {
namespace create {

void CreateFsTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsName=" << FLAGS_fsName
              << "[-blockSize=" << FLAGS_blockSize
              << "] [-fsType=volume -volumeSize=" << FLAGS_volumeSize
              << " -volumeBlockSize=" << FLAGS_volumeBlockSize
              << " -volumeName=" << FLAGS_volumeName
              << " -volumeUser=" << FLAGS_volumeUser
              << " -volumePassword=" << FLAGS_volumePassword
              << "]|[-fsType=s3 -s3_ak=" << FLAGS_s3_ak
              << " -s3_sk=" << FLAGS_s3_sk
              << " -s3_endpoint=" << FLAGS_s3_endpoint
              << " -s3_bucket_name=" << FLAGS_s3_bucket_name
              << " -s3_blocksize=" << FLAGS_s3_blocksize
              << " -s3_chunkSize=" << FLAGS_s3_chunksize
              << "] [-mdsAddr=" << FLAGS_mdsAddr
              << "] [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs
              << "-rpcRetryTimes=" << FLAGS_rpcRetryTimes << "]" << std::endl;
}

void CreateFsTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetBlockSize);
    AddUpdateFlagsFunc(curvefs::tools::SetFsType);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeSize);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeBlockSize);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeName);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeUser);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumePassword);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_ak);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_sk);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_endpoint);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_bucket_name);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_blocksize);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_chunksize);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcTimeoutMs);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcRetryTimes);
}

int CreateFsTool::Init() {
    int ret = CurvefsToolRpc::Init();

    google::CommandLineFlagInfo info;
    if (CheckFsNameDefault(&info)) {
        std::cerr << "no -fsName=***, please check it!" << std::endl;
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    service_stub_func_ =
        std::bind(&mds::MdsService_Stub::CreateFs, service_stub_.get(),
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3, nullptr);

    mds::CreateFsRequest request;
    request.set_fsname(FLAGS_fsName);
    request.set_blocksize(FLAGS_blockSize);
    if (FLAGS_fsType == "s3") {
        // s3
        request.set_fstype(common::FSType::TYPE_S3);
        auto s3 = new common::S3Info();
        s3->set_ak(FLAGS_s3_ak);
        s3->set_sk(FLAGS_s3_sk);
        s3->set_endpoint(FLAGS_s3_endpoint);
        s3->set_bucketname(FLAGS_s3_bucket_name);
        s3->set_blocksize(FLAGS_s3_blocksize);
        s3->set_chunksize(FLAGS_s3_chunksize);
        request.mutable_fsdetail()->set_allocated_s3info(s3);
    } else if (FLAGS_fsType == "volume") {
        // volume
        request.set_fstype(common::FSType::TYPE_VOLUME);
        auto volume = new common::Volume();
        volume->set_volumesize(FLAGS_volumeSize);
        volume->set_blocksize(FLAGS_volumeBlockSize);
        volume->set_volumename(FLAGS_volumeName);
        volume->set_user(FLAGS_volumeUser);
        volume->set_password(FLAGS_volumePassword);
        request.mutable_fsdetail()->set_allocated_volume(volume);
    } else {
        std::cerr << "-fsType should be s3 or volume." << std::endl;
        ret = -1;
    }

    AddRequest(request);
    return ret;
}

bool CreateFsTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        errorOutput_ << "send create fs request to mds: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text: " << controller_->ErrorText()
                     << std::endl;
        ret = false;
    } else if (response_->statuscode() == mds::FSStatusCode::OK) {
        // create success
        std::cout << "create fs success." << std::endl;
        if (response_->has_fsinfo()) {
            auto const& fsInfo = response_->fsinfo();
            std::cout << "the create fs info is:\n"
                      << fsInfo.ShortDebugString() << std::endl;
        }
    } else {
        // create fail
        std::cerr << "create fs failed, errorcode= " << response_->statuscode()
                  << ", error name: "
                  << mds::FSStatusCode_Name(response_->statuscode())
                  << std::endl;
        ret = false;
    }
    return ret;
}

}  // namespace create
}  // namespace tools
}  // namespace curvefs
