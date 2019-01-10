/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:17:31 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <vector>

#include "test/backup/context_slab.h"
#include "src/client/io_tracker.h"
#include "src/client/request_context.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {
class ContextSlabTest : public ::testing::Test {
 public:
    void SetUp() {
        iotrackerslab_ = new IOTrackerSlab();
        requestcontextslab_ = new RequestContextSlab;

        iotrackerslab_->Initialize();
        requestcontextslab_->Initialize();
    }

    void TearDown() {
        iotrackerslab_->UnInitialize();
        requestcontextslab_->UnInitialize();
        delete iotrackerslab_;
        delete requestcontextslab_;
    }

    IOTrackerSlab* iotrackerslab_;
    RequestContextSlab* requestcontextslab_;
};

TEST_F(ContextSlabTest, GetandRecyleTest) {
    int i = 0;
    std::vector<IOTracker*> ioctxvec;
    std::vector<RequestContext*> reqctxvec;
    while (i < 5000) {
        auto ioctx = iotrackerslab_->Get();
        auto reqctx = requestcontextslab_->Get();
        ioctxvec.push_back(ioctx);
        reqctxvec.push_back(reqctx);
        ASSERT_NE(ioctx, nullptr);
        ASSERT_NE(reqctx, nullptr);
        i++;
    }
    ASSERT_EQ(iotrackerslab_->Size(), 0);
    ASSERT_EQ(requestcontextslab_->Size(), 0);

    for (auto iter : ioctxvec) {
        iotrackerslab_->Recyle(iter);
    }

    for (auto iter : reqctxvec) {
        iter->RecyleSelf();
    }
    LOG(ERROR) << "iotrackerslab_->Size() = "
              << iotrackerslab_->Size();
    LOG(ERROR) << "requestcontextslab_->Size() = "
              << requestcontextslab_->Size();
    ASSERT_TRUE(iotrackerslab_->Size()
                 == 2 * ClientConfig::GetContextSlabOption().
                                        pre_allocate_context_num);
    ASSERT_TRUE(requestcontextslab_->Size()
                 == 2 * ClientConfig::GetContextSlabOption().
                                        pre_allocate_context_num);
}
}   // namespace client
}   // namespace curve
