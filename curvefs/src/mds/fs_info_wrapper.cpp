
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
 * @Date: Fri Jul 23 16:37:33 CST 2021
 * @Author: wuhanqing
 */

#include "curvefs/src/mds/fs_info_wrapper.h"

#include <algorithm>
#include <limits>

#include "curvefs/src/mds/codec/codec.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;

bool FsInfoWrapper::IsMountPointExist(const std::string& mp) const {
    return std::find(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                     mp) != fsInfo_.mountpoints().end();
}

void FsInfoWrapper::AddMountPoint(const std::string& mp) {
    // TODO(wuhanqing): sort after add ?
    auto* p = fsInfo_.add_mountpoints();
    *p = mp;

    fsInfo_.set_mountnum(fsInfo_.mountnum() + 1);
}

FSStatusCode FsInfoWrapper::DeleteMountPoint(const std::string& mp) {
    auto iter = std::find(fsInfo_.mountpoints().begin(),
                          fsInfo_.mountpoints().end(), mp);

    bool found = iter != fsInfo_.mountpoints().end();
    if (found) {
        fsInfo_.mutable_mountpoints()->erase(iter);
        fsInfo_.set_mountnum(fsInfo_.mountnum() - 1);
        return FSStatusCode::OK;
    }

    return FSStatusCode::MOUNT_POINT_NOT_EXIST;
}

std::vector<std::string> FsInfoWrapper::MountPoints() const {
    if (fsInfo_.mountpoints_size() == 0) {
        return {};
    }

    return {fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end()};
}

}  // namespace mds
}  // namespace curvefs
