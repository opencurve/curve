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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <fiu-control.h>

#include <string>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT
#include <vector>
#include <algorithm>

#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mockMDS.h"
#include "src/client/metacache.h"
#include "test/client/fake/mock_schedule.h"
#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_config.h"
#include "src/client/service_helper.h"
#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/metacache_struct.h"
#include "src/client/lease_excutor.h"
#include "src/common/net_common.h"
#include "src/client/iomanager4file.h"

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern std::string configpath;
extern curve::client::FileClient* globalclient;

using curve::client::UserInfo_t;
using curve::client::CopysetPeerInfo;
using curve::client::CopysetInfo_t;
using curve::client::IOManager4File;
using curve::client::SegmentInfo;
using curve::client::FInfo;
using curve::client::LeaseSession;
using curve::client::LogicalPoolCopysetIDInfo_t;
using curve::client::MetaCacheErrorType;
using curve::client::MDSClient;
using curve::client::ServiceHelper;
using curve::client::FileClient;
using curve::client::LogicPoolID;
using curve::client::CopysetID;
using curve::client::ChunkServerID;
using curve::client::ChunkServerAddr;
using curve::client::FileInstance;
using curve::client::LeaseExcutor;
using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;

TEST(LeaseExcutorBaseTest, CommonTest) {
    LeaseOption leaseOpt;
    UserInfo_t userInfo;
    MDSClient mdsClient;
    IOManager4File io4File;
    LeaseExcutor leaseExcutor(leaseOpt, userInfo, &mdsClient, &io4File);

    FInfo fi;
    LeaseSession lease;
    lease.leaseTime = 0;
    ASSERT_FALSE(leaseExcutor.Start(fi, lease));
}



