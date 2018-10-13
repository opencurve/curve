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

#include "src/client/context_slab.h"
#include "src/client/io_context.h"
#include "src/client/request_context.h"
#include "src/client/client_common.h"

DECLARE_int32(pre_allocate_context_num);

namespace curve {
namespace client {
class ContextSlabTest : public ::testing::Test {
 public:
    void SetUp() {
        iocontextslab_ = new IOContextSlab();
        requestcontextslab_ = new RequestContextSlab();

        iocontextslab_->Initialize();
        requestcontextslab_->Initialize();
    }

    void TearDown() {
        iocontextslab_->UnInitialize();
        requestcontextslab_->UnInitialize();
        delete iocontextslab_;
        delete requestcontextslab_;
    }

    IOContextSlab* iocontextslab_;
    RequestContextSlab* requestcontextslab_;
};

TEST_F(ContextSlabTest, GetandRecyleTest) {
    int i = 0;
    std::vector<IOContext*> ioctxvec;
    std::vector<RequestContext*> reqctxvec;
    while (i < 5000) {
        auto ioctx = iocontextslab_->Get();
        auto reqctx = requestcontextslab_->Get();
        ioctxvec.push_back(ioctx);
        reqctxvec.push_back(reqctx);
        ASSERT_NE(ioctx, nullptr);
        ASSERT_NE(reqctx, nullptr);
        i++;
    }
    ASSERT_EQ(iocontextslab_->Size(), 0);
    ASSERT_EQ(requestcontextslab_->Size(), 0);

    for (auto iter : ioctxvec) {
        iocontextslab_->Recyle(iter);
    }

    for (auto iter : reqctxvec) {
        iter->RecyleSelf();
    }
    LOG(INFO) << "iocontextslab_->Size() = "
              << iocontextslab_->Size();
    LOG(INFO) << "requestcontextslab_->Size() = "
              << requestcontextslab_->Size();
    ASSERT_TRUE(iocontextslab_->Size()
                 == 2 * FLAGS_pre_allocate_context_num);
    ASSERT_TRUE(requestcontextslab_->Size()
                 == 2 * FLAGS_pre_allocate_context_num);
}
}   // namespace client
}   // namespace curve
