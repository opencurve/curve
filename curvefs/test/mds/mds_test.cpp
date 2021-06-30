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
 * @Project: curve
 * @Date: 2021-06-10 10:46:50
 * @Author: chenwei
 */

#include "curvefs/src/mds/mds.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>  // NOLINT

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace mds {
class MdsTest : public ::testing::Test {
 protected:
    void SetUp() override { addr_ = "127.0.0.1:6703"; }

    void TearDown() override { return; }

    std::string addr_;
};

TEST_F(MdsTest, test1) {
    curvefs::mds::Mds mds;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/mds.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", addr_);

    // initialize MDS options
    mds.InitOptions(conf);

    // Initialize other modules after winning election
    mds.Init();

    // start mds server and wait CTRL+C to quit
    // mds.Run();
    std::thread mdsThread(&Mds::Run, &mds);

    // sleep 5s
    sleep(3);

    // stop server and background threads
    mds.Stop();
    mdsThread.join();
}

TEST_F(MdsTest, test2) {
    curvefs::mds::Mds mds;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/mds.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", addr_);

    // initialize MDS options
    mds.InitOptions(conf);

    // not init, run
    mds.Run();
    mds.Run();

    // not start, stop
    mds.Stop();
    mds.Stop();

    // Initialize other modules after winning election
    mds.Init();
    mds.Init();

    // start mds server and wait CTRL+C to quit
    // mds.Run();
    std::thread mdsThread(&Mds::Run, &mds);

    // sleep 5s
    sleep(3);

    // stop server and background threads
    mds.Stop();
    mdsThread.join();
}
}  // namespace mds
}  // namespace curvefs
