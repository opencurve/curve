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
 * Date: Thursday Jul 14 19:56:54 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/mds_proxy.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/mds/space/utils.h"
#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/client/mds_client.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curve::client::FInfo;
using ::curve::client::FileEpoch;
using ::curve::client::UserInfo;

MdsProxy::MdsProxy() = default;

MdsProxy::~MdsProxy() = default;

std::unique_ptr<MdsProxy> MdsProxy::Create(
    const std::vector<std::string>& hosts,
    const MdsProxyOptions& opts) {
    auto proxy = absl::WrapUnique(new MdsProxy());
    proxy->mdsClient_ =
        absl::make_unique<curve::client::MDSClient>(ConcatHosts(hosts));

    curve::client::MetaServerOption mdsOpts = opts.option;
    mdsOpts.rpcRetryOpt.addrs = hosts;

    auto ret = proxy->mdsClient_->Initialize(mdsOpts);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Fail to initialize mds client, ret: " << ret;
        return nullptr;
    }

    return proxy;
}

bool MdsProxy::GetVolumeInfo(const common::Volume& volume,
                             uint64_t* size,
                             uint64_t* extendAlignment) {
    UserInfo info(volume.user(), volume.password());
    FInfo fi;
    FileEpoch dummy;
    auto ret = mdsClient_->GetFileInfo(volume.volumename(), info, &fi, &dummy);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(WARNING) << "Fail to get volume size, ret: "
                     << LibCurveErrorName(ret);
        return false;
    }

    (void)dummy;
    *size = fi.length;
    *extendAlignment = fi.segmentsize;
    return true;
}

bool MdsProxy::ExtendVolume(const common::Volume& volume, uint64_t size) {
    UserInfo info(volume.user(), volume.password());

    auto ret = mdsClient_->Extend(volume.volumename(), info, size);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(WARNING) << "Fail to extend volume, ret: "
                     << LibCurveErrorName(ret);
        return false;
    }

    return true;
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
