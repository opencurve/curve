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

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <iostream>
#include <thread>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space/common.h"

using ::curvefs::space::kGiB;
using ::curvefs::space::kKiB;
using ::curvefs::space::kMiB;

brpc::Channel channel;

DEFINE_string(address, "0.0.0.0:19999", "space server address");

DEFINE_int32(fsId, -1, "filesystem");
DEFINE_uint64(capacity, 100 * kGiB, "filesystem size");

using ::curvefs::space::PExtent;

const std::vector<uint64_t> sizes = {
    4 * kKiB, 8 * kKiB, 16 * kKiB, 32 * kKiB, 512 * kKiB,  // NOLINT
    1 * kMiB, 4 * kMiB, 8 * kMiB,  16 * kMiB, 32 * kMiB    // NOLINT
};

std::deque<PExtent> exts;

std::atomic<bool> noSpace(false);

void Alloc() {
    ::curvefs::space::AllocateSpaceRequest request;
    ::curvefs::space::AllocateSpaceResponse response;

    request.set_fsid(FLAGS_fsId);
    request.set_size(sizes[rand() % sizes.size()]);

    brpc::Controller cntl;
    ::curvefs::space::SpaceAllocService_Stub stub(&channel);
    stub.AllocateSpace(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(FATAL) << "rpc cntl failed, error: " << cntl.ErrorText();
    }

    if (response.status() == curvefs::space::SPACE_NO_SPACE) {
        noSpace.store(true, std::memory_order_relaxed);
        LOG(INFO) << "NO SPACE";
    } else {
        uint64_t allocated = 0;
        for (auto& e : response.extents()) {
            exts.emplace_back(e.offset(), e.length());
            allocated += e.length();

            LOG_IF(FATAL, e.offset() >= FLAGS_capacity ||
                              e.offset() + e.length() > FLAGS_capacity)
                << "allocate exceed capacity";
        }

        LOG_IF(FATAL, allocated != request.size())
            << "allocate success, but size is not as expected";
    }
}

void Dealloc() {
    if (exts.empty()) {
        return;
    }

    ::curvefs::space::DeallocateSpaceRequest request;
    ::curvefs::space::DeallocateSpaceResponse response;

    request.set_fsid(FLAGS_fsId);
    auto* e = request.add_extents();
    auto p = exts.front();
    exts.pop_front();
    e->set_offset(p.offset);
    e->set_length(p.len);

    brpc::Controller cntl;
    ::curvefs::space::SpaceAllocService_Stub stub(&channel);
    stub.DeallocateSpace(&cntl, &request, &response, nullptr);

    if (cntl.Failed() || response.status() != ::curvefs::space::SPACE_OK) {
        LOG(FATAL) << "deallocate failed";
    }

    noSpace.store(false, std::memory_order_relaxed);
}

void Init() {
    ::curvefs::space::InitSpaceRequest request;
    ::curvefs::space::InitSpaceResponse response;

    auto* fsInfo = request.mutable_fsinfo();
    fsInfo->set_fsid(FLAGS_fsId);
    fsInfo->set_fsname(std::to_string(FLAGS_fsId));
    fsInfo->set_rootinodeid(0);
    fsInfo->set_capacity(FLAGS_capacity);
    fsInfo->set_blocksize(4 * kKiB);
    fsInfo->set_mountnum(0);

    auto* vol = fsInfo->mutable_detail()->mutable_volume();
    vol->set_volumesize(FLAGS_capacity);
    vol->set_blocksize(4 * kKiB);
    vol->set_volumename(std::to_string(FLAGS_fsId));
    vol->set_user("hello");

    brpc::Controller cntl;
    int rv = channel.Init(FLAGS_address.c_str(), nullptr);
    if (rv != 0) {
        LOG(FATAL) << "init channel to space server failed";
    }

    ::curvefs::space::SpaceAllocService_Stub stub(&channel);
    stub.InitSpace(&cntl, &request, &response, nullptr);

    if (cntl.Failed() || response.status() != ::curvefs::space::SPACE_OK) {
        LOG(FATAL) << "init space failed";
    }
}

void Run() {
    static thread_local unsigned int seed;
    seed = time(nullptr);
    while (true) {
        if (rand_r(&seed) % 4 == 0 ||
            noSpace.load(std::memory_order_relaxed) == true) {
            Dealloc();
        } else {
            Alloc();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_log_dir = "/home/wuhanqing/log";

    ::google::InitGoogleLogging(argv[0]);

    srand(time(nullptr));

    Init();
    Run();

    return 0;
}
