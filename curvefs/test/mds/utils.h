/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Friday Jul 22 15:23:46 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_UTILS_H_
#define CURVEFS_TEST_MDS_UTILS_H_

#include <brpc/closure_guard.h>

#include "proto/nameserver2.pb.h"

namespace curvefs {
namespace mds {

struct FakeCurveFSService : public curve::mds::CurveFSService {
    void ExtendFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ExtendFileRequest* request,
                    ::curve::mds::ExtendFileResponse* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        if (extendCallback) {
            extendCallback(response);
        } else {
            response->set_statuscode(curve::mds::kOK);
        }
    }

    void GetFileInfo(::google::protobuf::RpcController* controller,
                     const ::curve::mds::GetFileInfoRequest* request,
                     ::curve::mds::GetFileInfoResponse* response,
                     ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        if (getinfoCallback) {
            getinfoCallback(response);
        } else {
            response->set_statuscode(curve::mds::kOK);
            auto* fileInfo = response->mutable_fileinfo();
            fileInfo->set_length(volumeSize);
            fileInfo->set_segmentsize(volumeSegmentSize);
        }
    }

    std::function<void(curve::mds::ExtendFileResponse*)> extendCallback;
    std::function<void(curve::mds::GetFileInfoResponse*)> getinfoCallback;

    uint64_t volumeSize{100ULL << 30};
    uint64_t volumeSegmentSize{1ULL << 30};
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_UTILS_H_
