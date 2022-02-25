/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_METASERVER_MDSCLIENT_MDS_CLIENT_H_
#define CURVEFS_SRC_METASERVER_MDSCLIENT_MDS_CLIENT_H_

#include <string>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "src/client/mds_client.h"
#include "curvefs/src/client/metric/client_metric.h"


namespace curvefs {
namespace metaserver {
namespace mdsclient {

using ::curvefs::client::metric::MDSClientMetric;
using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;
using ::curvefs::mds::GetFsInfoResponse;
using ::curvefs::client::rpcclient::MDSBaseClient;

class MdsClient {
 public:
    MdsClient() {}
    virtual ~MdsClient() {}

    virtual FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                              MDSBaseClient *baseclient) = 0;

    virtual FSStatusCode GetFsInfo(const std::string &fsName,
                                   FsInfo *fsInfo) = 0;

    virtual FSStatusCode GetFsInfo(uint32_t fsId, FsInfo *fsInfo) = 0;
};

class MdsClientImpl : public MdsClient {
 public:
    explicit MdsClientImpl(const std::string &metricPrefix = "")
        : mdsClientMetric_(metricPrefix) {}

    FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                      MDSBaseClient *baseclient) override;

    FSStatusCode GetFsInfo(const std::string &fsName, FsInfo *fsInfo) override;

    FSStatusCode GetFsInfo(uint32_t fsId, FsInfo *fsInfo) override;

 private:
    FSStatusCode ReturnError(int retcode);

 private:
    MDSBaseClient *mdsbasecli_;
    ::curve::client::RPCExcutorRetryPolicy rpcexcutor_;
    ::curve::client::MetaServerOption mdsOpt_;

    MDSClientMetric mdsClientMetric_;
};

}  // namespace mdsclient
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_MDSCLIENT_MDS_CLIENT_H_
