#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <deque>
#include <iostream>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space_allocator/common.h"

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

    if (response.status() == ::curvefs::space::SPACE_NOSPACE) {
        noSpace.store(true, std::memory_order_relaxed);
    } else {
        for (auto& e : response.extents()) {
            exts.emplace_back(e.offset(), e.length());
        }
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

    auto* vol = fsInfo->mutable_volume();
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
    while (true) {
        if (rand() % 2 == 0 ||
            noSpace.load(std::memory_order_relaxed) == true) {
            Dealloc();
        } else {
            Alloc();
        }
    }
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_log_dir = "/tmp";

    ::google::InitGoogleLogging(argv[0]);

    srand(time(nullptr));

    Init();
    Run();

    return 0;
}