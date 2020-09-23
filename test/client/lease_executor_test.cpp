/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: 2019年11月20日
 * Author: wuhanqing
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/client/iomanager4file.h"
#include "src/client/lease_executor.h"
#include "src/client/libcurve_file.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern std::string configpath;
extern curve::client::FileClient* globalclient;

using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;

TEST(LeaseExecutorBaseTest, test_StartFailed) {
    UserInfo_t userInfo;
    MDSClient mdsClient;
    FInfo fi;
    fi.fullPathName = "/test_StartFailed";
    IOManager4File io4File;

    {
        LeaseOption leaseOpt;
        LeaseExecutor leaseExecutor(leaseOpt, userInfo, &mdsClient, &io4File);

        LeaseSession lease;
        lease.leaseTime = 0;
        ASSERT_FALSE(leaseExecutor.Start(fi, lease));
    }

    {
        LeaseOption leaseOpt;
        leaseOpt.mdsRefreshTimesPerLease = 0;

        LeaseExecutor leaseExecutor(leaseOpt, userInfo, &mdsClient, &io4File);

        LeaseSession lease;
        lease.leaseTime = 1000;
        ASSERT_FALSE(leaseExecutor.Start(fi, lease));
    }
}

}  // namespace client
}  // namespace curve
