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

#include "curvefs/src/metaserver/metaserver.h"
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
namespace metaserver {
class MetaserverTest : public ::testing::Test {
 protected:
    void SetUp() override { addr_ = "127.0.0.1:6702"; }

    void TearDown() override { return; }

    std::string addr_;
};

TEST_F(MetaserverTest, test1) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("metaserver.listen.addr", addr_);

    // initialize MDS options
    metaserver.InitOptions(conf);

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();
    metaserverThread.join();
}

TEST_F(MetaserverTest, test2) {
    curvefs::metaserver::Metaserver metaserver;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/metaserver.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("metaserver.listen.addr", addr_);

    // initialize Metaserver options
    metaserver.InitOptions(conf);

    // not init, run
    metaserver.Run();
    metaserver.Run();

    // not start, stop
    metaserver.Stop();
    metaserver.Stop();

    // Initialize other modules after winning election
    metaserver.Init();
    metaserver.Init();

    // start metaserver server
    std::thread metaserverThread(&Metaserver::Run, &metaserver);

    // sleep 2s
    sleep(2);

    // stop server and background threads
    metaserver.Stop();
    metaserverThread.join();
}
}  // namespace metaserver
}  // namespace curvefs
