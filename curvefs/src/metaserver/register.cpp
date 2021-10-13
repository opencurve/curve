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
 * Project: curve
 * Created Date: 2021-09-12
 * Author: chenwei
 */

#include <brpc/channel.h>
#include <fcntl.h>
#include <json2pb/json_to_pb.h>
#include <json2pb/pb_to_json.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/metaserver/register.h"
#include "src/common/string_util.h"
#include "src/common/uri_parser.h"

using ::curvefs::mds::topology::MetaServerRegistRequest;
using ::curvefs::mds::topology::MetaServerRegistResponse;
using ::curvefs::mds::topology::TopoStatusCode;

namespace curvefs {
namespace metaserver {
Register::Register(const RegisterOptions &ops) {
    this->ops_ = ops;

    // Resolve multiple addresses of mds
    ::curve::common::SplitString(ops.mdsListenAddr, ",", &mdsEps_);
    // Check the legitimacy of each address
    for (const auto &addr : mdsEps_) {
        butil::EndPoint endpt;
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(FATAL) << "Invalid sub mds ip:port provided: " << addr;
        }
    }
    inServiceIndex_ = 0;
}

int Register::RegisterToMDS(MetaServerMetadata *metadata) {
    MetaServerRegistRequest req;
    MetaServerRegistResponse resp;

    // get hostname
    char hostname[128];
    int ret = gethostname(hostname, sizeof(hostname));
    if (ret == -1) {
        LOG(ERROR) << "get hostname failed!";
        return -1;
    }

    req.set_hostname(hostname);
    req.set_hostip(ops_.metaserverInternalIp);
    req.set_port(ops_.metaserverPort);
    req.set_externalip(ops_.metaserverExternalIp);
    req.set_externalport(ops_.metaserverPort);

    LOG(INFO) << " Registering to MDS " << mdsEps_[inServiceIndex_]
              << ". hostname: " << hostname
              << ", internal ip: " << ops_.metaserverInternalIp
              << ", port: " << ops_.metaserverPort
              << ", external ip: " << ops_.metaserverExternalIp;

    int retries = ops_.registerRetries;
    while (retries >= 0) {
        brpc::Channel channel;
        brpc::Controller cntl;

        cntl.set_timeout_ms(ops_.registerTimeout);

        if (channel.Init(mdsEps_[inServiceIndex_].c_str(), NULL) != 0) {
            LOG(ERROR) << ops_.metaserverInternalIp << ":"
                       << ops_.metaserverPort
                       << " Fail to init channel to MDS "
                       << mdsEps_[inServiceIndex_];
            return -1;
        }
        curvefs::mds::topology::TopologyService_Stub stub(&channel);

        stub.RegistMetaServer(&cntl, &req, &resp, nullptr);
        if (!cntl.Failed() && resp.statuscode() == TopoStatusCode::TOPO_OK) {
            break;
        } else {
            LOG(INFO) << ops_.metaserverInternalIp << ":"
                       << ops_.metaserverPort << " Fail to register to MDS "
                       << mdsEps_[inServiceIndex_]
                       << ", cntl errorCode: " << cntl.ErrorCode() << ","
                       << " cntl error: " << cntl.ErrorText() << ","
                       << " statusCode: " << resp.statuscode() << ","
                       << " going to sleep and try again.";
            if (cntl.ErrorCode() == EHOSTDOWN ||
                cntl.ErrorCode() == brpc::ELOGOFF) {
                inServiceIndex_ = (inServiceIndex_ + 1) % mdsEps_.size();
            }
            sleep(1);
            --retries;
        }
    }

    if (retries <= 0) {
        LOG(ERROR) << ops_.metaserverInternalIp << ":" << ops_.metaserverPort
                   << " Fail to register to MDS for " << ops_.registerRetries
                   << " times.";
        return -1;
    }

    metadata->set_version(CURRENT_METADATA_VERSION);
    metadata->set_id(resp.metaserverid());
    metadata->set_token(resp.token());

    LOG(INFO) << ops_.metaserverInternalIp << ":" << ops_.metaserverPort
              << " Successfully registered to MDS: " << mdsEps_[inServiceIndex_]
              << ", metaserver id: " << metadata->id() << ","
              << " token: " << metadata->token();

    return 0;
}
}  // namespace metaserver
}  // namespace curvefs
