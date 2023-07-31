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
#include <iostream>

#include <functional>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"
#include "src/common/fast_align.h"
#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"

DECLARE_string(fsName);
DECLARE_string(confPath);
DECLARE_string(mdsAddr);
DECLARE_uint64(blockSize);
DECLARE_string(fsType);

// volume fs
DECLARE_uint64(volumeBlockSize);
DECLARE_string(volumeName);
DECLARE_string(volumeUser);
DECLARE_string(volumePassword);
DECLARE_uint64(volumeBlockGroupSize);
DECLARE_string(volumeBitmapLocation);
DECLARE_uint64(volumeSliceSize);
DECLARE_bool(volumeAutoExtend);
DECLARE_double(volumeExtendFactor);
DECLARE_string(volumeCluster);

DECLARE_string(s3_ak);
DECLARE_string(s3_sk);
DECLARE_string(s3_endpoint);
DECLARE_string(s3_bucket_name);
DECLARE_uint64(s3_blocksize);
DECLARE_uint64(s3_chunksize);
DECLARE_uint32(s3_objectPrefix);
DECLARE_uint32(rpcTimeoutMs);
DECLARE_uint32(rpcRetryTimes);
DECLARE_bool(enableSumInDir);
DECLARE_uint64(capacity);
DECLARE_string(user);
DECLARE_uint32(recycleTimeHour);
DECLARE_uint32(filterType);
DECLARE_string(filterList);

namespace curvefs {
namespace tools {
namespace create {

using ::curve::common::is_aligned;
using ::curvefs::common::BitmapLocation;
using ::curvefs::common::BitmapLocation_Parse;

void CreateFsTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsName=" << FLAGS_fsName << " [-user=" << FLAGS_user
              << "] [-capacity=" << FLAGS_capacity
              << "] [-blockSize=" << FLAGS_blockSize
              << "] [-enableSumInDir=" << FLAGS_enableSumInDir
              << "] [-mdsAddr=" << FLAGS_mdsAddr
              << "] [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs
              << " -rpcRetryTimes=" << FLAGS_rpcRetryTimes << "]"
              << "] [recycleTimeHour=" << FLAGS_recycleTimeHour
              << "] [filterType=" << FLAGS_filterType
              << "] [filterList=" << FLAGS_filterList
              << "] \n[-fsType=volume -volumeBlockGroupSize="
              << FLAGS_volumeBlockGroupSize
              << " -volumeBlockSize=" << FLAGS_volumeBlockSize
              << " -volumeName=" << FLAGS_volumeName
              << " -volumeUser=" << FLAGS_volumeUser
              << " -volumePassword=" << FLAGS_volumePassword
              << " -volumeBitmapLocation=AtStart|AtEnd"
              << " -volumeAutoExtend=false|true"
              << " -volumeExtendFactor=" << FLAGS_volumeExtendFactor
              << " -volumeCluster=" << FLAGS_volumeCluster
              << "]\n[-fsType=s3 -s3_ak=" << FLAGS_s3_ak
              << " -s3_sk=" << FLAGS_s3_sk
              << " -s3_endpoint=" << FLAGS_s3_endpoint
              << " -s3_bucket_name=" << FLAGS_s3_bucket_name
              << " -s3_blocksize=" << FLAGS_s3_blocksize
              << " -s3_chunksize=" << FLAGS_s3_chunksize
              << " -s3_objectPrefix=" << FLAGS_s3_objectPrefix
              << "]\n[-fsType=hybrid -volumeBlockGroupSize="
              << FLAGS_volumeBlockGroupSize
              << " -volumeBlockSize=" << FLAGS_volumeBlockSize
              << " -volumeName=" << FLAGS_volumeName
              << " -volumeUser=" << FLAGS_volumeUser
              << " -volumePassword=" << FLAGS_volumePassword
              << " -volumeBitmapLocation=AtStart|AtEnd"
              << " -s3_ak=" << FLAGS_s3_ak << " -s3_sk=" << FLAGS_s3_sk
              << " -s3_endpoint=" << FLAGS_s3_endpoint
              << " -s3_bucket_name=" << FLAGS_s3_bucket_name
              << " -s3_blocksize=" << FLAGS_s3_blocksize
              << " -s3_chunksize=" << FLAGS_s3_chunksize
              << " -s3_objectPrefix=" << FLAGS_s3_objectPrefix
              << "]" << std::endl;
}

void CreateFsTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetBlockSize);
    AddUpdateFlagsFunc(curvefs::tools::SetFsType);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeBlockSize);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeName);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeUser);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumePassword);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeBlockSize);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeBitmapLocation);
    AddUpdateFlagsFunc(curvefs::tools::SetVolumeCluster);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_ak);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_sk);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_endpoint);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_bucket_name);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_blocksize);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_chunksize);
    AddUpdateFlagsFunc(curvefs::tools::SetS3_objectPrefix);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcTimeoutMs);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcRetryTimes);
    AddUpdateFlagsFunc(curvefs::tools::SetEnableSumInDir);
    AddUpdateFlagsFunc(curvefs::tools::SetRecycleTimeHour);
    AddUpdateFlagsFunc(curvefs::tools::SetFilterType);
    AddUpdateFlagsFunc(curvefs::tools::SetFilterList);
}

namespace {
google::protobuf::RepeatedPtrField<::std::string> ParseVolumeCluster(
    const std::string& hosts) {
    std::vector<absl::string_view> split = absl::StrSplit(hosts, ',');
    google::protobuf::RepeatedPtrField<::std::string> res;
    res.Reserve(split.size());
    for (const auto& sv : split) {
        res.Add(std::string{sv.data(), sv.size()});
    }

    return res;
}
}  // namespace

int CreateFsTool::Init() {
    int ret = CurvefsToolRpc::Init();

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    service_stub_func_ =
        std::bind(&mds::MdsService_Stub::CreateFs, service_stub_.get(),
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3, nullptr);

    mds::CreateFsRequest request;
    request.set_fsname(FLAGS_fsName);
    request.set_blocksize(FLAGS_blockSize);
    request.set_enablesumindir(FLAGS_enableSumInDir);
    request.set_recycletimehour(FLAGS_recycleTimeHour);
    request.set_filtertype(FLAGS_filterType);
    request.set_filterlist(FLAGS_filterList);

    auto SetS3Request = [&]() -> int {
        request.set_fstype(common::FSType::TYPE_S3);
        auto* s3 = new common::S3Info();
        s3->set_ak(FLAGS_s3_ak);
        s3->set_sk(FLAGS_s3_sk);
        s3->set_endpoint(FLAGS_s3_endpoint);
        s3->set_bucketname(FLAGS_s3_bucket_name);
        s3->set_blocksize(FLAGS_s3_blocksize);
        s3->set_chunksize(FLAGS_s3_chunksize);
        s3->set_objectprefix(FLAGS_s3_objectPrefix);
        request.mutable_fsdetail()->set_allocated_s3info(s3);
        return 0;
    };

    auto SetVolumeRequest = [&]() -> int {
        // precheck
        if (!is_aligned(FLAGS_volumeBlockSize, 4096)) {
            std::cerr << "volumeBlockSize should align with 4096";
            return -1;
        }

        if (!is_aligned(FLAGS_volumeBlockGroupSize, FLAGS_volumeBlockSize)) {
            std::cerr
                << "volumeBlockGroupSize should align with volumeBlockSize";
            return -1;
        }

        if (!is_aligned(FLAGS_volumeBlockGroupSize, 128ULL * 1024 * 1024)) {
            std::cerr << "volumeBlockGroupSize should align with 128MiB";
            return -1;
        }

        if (!is_aligned(FLAGS_volumeSliceSize, FLAGS_volumeBlockGroupSize)) {
            std::cerr << "volume slice size should align with "
                         "FLAGS_volumeBlockGroupSize";
            return -1;
        }

        BitmapLocation location;
        if (!BitmapLocation_Parse(FLAGS_volumeBitmapLocation, &location)) {
            std::cerr << "Parse volumeBitmapLocation error, only support "
                         "|AtStart| and |AtEnd|";
            return -1;
        }

        // volume
        request.set_fstype(common::FSType::TYPE_VOLUME);
        auto* volume = new common::Volume();
        volume->set_blocksize(FLAGS_volumeBlockSize);
        volume->set_volumename(FLAGS_volumeName);
        volume->set_user(FLAGS_volumeUser);
        volume->set_password(FLAGS_volumePassword);
        volume->set_blockgroupsize(FLAGS_volumeBlockGroupSize);
        volume->set_bitmaplocation(location);
        volume->set_slicesize(FLAGS_volumeSliceSize);
        volume->set_autoextend(FLAGS_volumeAutoExtend);
        if (FLAGS_volumeAutoExtend) {
            volume->set_extendfactor(FLAGS_volumeExtendFactor);
        }
        *volume->mutable_cluster() = ParseVolumeCluster(FLAGS_volumeCluster);
        request.mutable_fsdetail()->set_allocated_volume(volume);
        return 0;
    };

    auto SetHybridRequest = [&]() -> int {
        if (SetS3Request() != 0 || SetVolumeRequest() != 0) {
            return -1;
        }
        request.set_fstype(common::FSType::TYPE_HYBRID);
        return 0;
    };

    std::unordered_map<std::string, std::function<int()>> setRequestFuncMap{
        {kFsTypeS3, SetS3Request},
        {kFsTypeVolume, SetVolumeRequest},
        {kFsTypeHybrid, SetHybridRequest}};

    auto func = setRequestFuncMap.find(FLAGS_fsType);

    if (func == setRequestFuncMap.end()) {
        std::cerr << "fsType should be one of [S3, Volume, Hybrid]";
        return -1;
    }

    if (func->second() != 0) {
        return -1;
    }

    request.set_owner(FLAGS_user);
    request.set_capacity(FLAGS_capacity);

    AddRequest(request);

    SetController();

    return ret;
}

void CreateFsTool::SetController() {
    controller_->set_timeout_ms(FLAGS_rpcTimeoutMs);
}

bool CreateFsTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "send create fs request to mds: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text: " << controller_->ErrorText()
                     << std::endl;
        return ret;
    }

    switch (response_->statuscode()) {
        case mds::FSStatusCode::OK:
            std::cout << "create fs success." << std::endl;
            ret = true;
            break;
        case mds::FSStatusCode::FS_EXIST:
            std::cerr << "create fs error, fs [" << FLAGS_fsName
                      << "] exist. But S3 info is inconsistent!" << std::endl;
            break;
        case mds::FSStatusCode::S3_INFO_ERROR:
            std::cerr << "create fs error, the s3 info is not available!"
                      << std::endl;
            break;
        default:
            std::cerr << "create fs failed, errorcode= "
                      << response_->statuscode() << ", error name: "
                      << mds::FSStatusCode_Name(response_->statuscode())
                      << std::endl;
    }
    return ret;
}

bool CreateFsTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckFsNameDefault(&info)) {
        std::cerr << "no -fsName=***, please use -example!" << std::endl;
        return true;
    }
    return false;
}

}  // namespace create
}  // namespace tools
}  // namespace curvefs
