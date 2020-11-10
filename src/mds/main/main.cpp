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
 * Created Date: Friday October 19th 2018
 * Author: hzsunjianliang
 */
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "src/mds/server/mds.h"
#include "src/mds/common/mds_define.h"

DEFINE_string(confPath, "conf/mds.conf", "mds confPath");
DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds listen addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd client");
DEFINE_string(mdsDbName, "curve_mds", "mds db name");
DEFINE_int32(sessionInterSec, 5, "mds session expired second");
DEFINE_int32(updateToRepoSec, 5, "interval of update data in mds to repo");
DEFINE_uint32(dummyPort, 6667, "dummy server port");

using ::curve::mds::kMB;
using ::curve::mds::kGB;
using ::curve::mds::DefaultSegmentSize;
using ::curve::mds::kMiniFileLength;

DEFINE_uint64(chunkSize, 16 * kMB, "chunk size");
DEFINE_uint64(segmentSize, 1 * kGB, "segment size");
DEFINE_uint64(minFileLength, 10 * kGB, "min filglength");

void LoadConfigFromCmdline(Configuration *conf) {
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("mdsAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.listen.addr", FLAGS_mdsAddr);
    }

    if (GetCommandLineFlagInfo("etcdAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.etcd.endpoint", FLAGS_etcdAddr);
    }

    if (GetCommandLineFlagInfo("chunkSize", &info) && !info.is_default) {
        conf->SetUInt64Value("mds.curvefs.defaultChunkSize", FLAGS_chunkSize);
    }

    if (GetCommandLineFlagInfo("segmentSize", &info) && !info.is_default) {
        DefaultSegmentSize = FLAGS_segmentSize;
    }

    if (GetCommandLineFlagInfo("minFileLength", &info) && !info.is_default) {
        kMiniFileLength = FLAGS_minFileLength;
    }

    if (GetCommandLineFlagInfo("mdsDbName", &info) && !info.is_default) {
        conf->SetStringValue("mds.DbName", FLAGS_mdsDbName);
    }

    if (GetCommandLineFlagInfo("sessionInterSec", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.leader.sessionInterSec", FLAGS_sessionInterSec);
    }

    if (GetCommandLineFlagInfo("updateToRepoSec", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.topology.TopologyUpdateToRepoSec", FLAGS_updateToRepoSec);
    }

    if (GetCommandLineFlagInfo("dummyPort", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.dummy.listen.port", FLAGS_dummyPort);
    }
}


int main(int argc, char **argv) {
    // config initialization
    google::ParseCommandLineFlags(&argc, &argv, false);
    std::string confPath = FLAGS_confPath.c_str();
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;
    LoadConfigFromCmdline(conf.get());
    conf->PrintConfig();
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("mds.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no mds.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }

    // initialize logging module
    google::InitGoogleLogging(argv[0]);

    curve::mds::MDS mds;

    // initialize MDS options
    mds.InitMdsOptions(conf);

    // start MDS dummy server for liveness probe and metric exportation of MDS
    mds.StartDummy();

    mds.StartCompaginLeader();

    // Initialize other modules after winning election
    mds.Init();

    // start mds server and wait CTRL+C to quit
    mds.Run();

    // stop server and background threads
    mds.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}

