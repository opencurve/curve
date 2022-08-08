
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

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <limits>

#include "curvefs/src/mds/codec/codec.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using google::protobuf::util::MessageDifferencer;

FsInfoWrapper::FsInfoWrapper(const ::curvefs::mds::CreateFsRequest* request,
                             uint64_t fsId,
                             uint64_t rootInodeId) {
    FsInfo fsInfo;
    fsInfo.set_fsname(request->fsname());
    fsInfo.set_fsid(fsId);
    fsInfo.set_status(FsStatus::NEW);
    fsInfo.set_rootinodeid(rootInodeId);
    fsInfo.set_blocksize(request->blocksize());
    fsInfo.set_mountnum(0);
    fsInfo.set_enablesumindir(request->enablesumindir());
    fsInfo.set_txsequence(0);
    fsInfo.set_txowner("");

    const auto& detail = request->fsdetail();
    fsInfo.set_allocated_detail(new FsDetail(detail));

    switch (request->fstype()) {
        case FSType::TYPE_S3:
            fsInfo.set_fstype(FSType::TYPE_S3);
            fsInfo.set_capacity(request->capacity());
            break;
        case FSType::TYPE_VOLUME:
            fsInfo.set_fstype(FSType::TYPE_VOLUME);
            fsInfo.set_capacity(detail.volume().volumesize());
            break;
        case FSType::TYPE_HYBRID:
            fsInfo.set_fstype(FSType::TYPE_HYBRID);
            // TODO(huyao): set capacity for hybrid fs
            fsInfo.set_capacity(
                std::min(detail.volume().volumesize(), request->capacity()));
            break;
    }

    fsInfo.set_owner(request->owner());
    fsInfo_ = std::move(fsInfo);
}

bool FsInfoWrapper::IsMountPointExist(const Mountpoint &mp) const {
    return std::find_if(fsInfo_.mountpoints().begin(),
                        fsInfo_.mountpoints().end(),
                        [mp](const Mountpoint &mountPoint) {
                            return mp.path() == mountPoint.path() &&
                                   mp.hostname() == mountPoint.hostname();
                        }) != fsInfo_.mountpoints().end();
}

bool FsInfoWrapper::IsMountPointConflict(const Mountpoint &mp) const {
    bool cto = (fsInfo_.mountpoints_size() ? false : mp.cto());

    bool exist =
        std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                     [&](const Mountpoint &mountPoint) {
                         if (mountPoint.has_cto() && mountPoint.cto()) {
                             cto = true;
                         }

                         return mp.path() == mountPoint.path() &&
                                mp.hostname() == mountPoint.hostname();
                     }) != fsInfo_.mountpoints().end();

    // NOTE:
    // 1. if mount point exist (exist = true), conflict
    // 2. if existing mount point enableCto is diffrent from newcomer, conflict
    return exist || (cto != mp.cto());
}

void FsInfoWrapper::AddMountPoint(const Mountpoint& mp) {
    // TODO(wuhanqing): sort after add ?
    auto* p = fsInfo_.add_mountpoints();
    *p = mp;

    fsInfo_.set_mountnum(fsInfo_.mountnum() + 1);
}

FSStatusCode FsInfoWrapper::DeleteMountPoint(const Mountpoint& mp) {
    auto iter =
        std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                     [mp](const Mountpoint &mountPoint) {
                         return mp.path() == mountPoint.path() &&
                                mp.hostname() == mountPoint.hostname() &&
                                mp.port() == mountPoint.port();
                     });

    bool found = iter != fsInfo_.mountpoints().end();
    if (found) {
        fsInfo_.mutable_mountpoints()->erase(iter);
        fsInfo_.set_mountnum(fsInfo_.mountnum() - 1);
        return FSStatusCode::OK;
    }

    return FSStatusCode::MOUNT_POINT_NOT_EXIST;
}

std::vector<Mountpoint> FsInfoWrapper::MountPoints() const {
    if (fsInfo_.mountpoints_size() == 0) {
        return {};
    }

    return {fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end()};
}

void FsInfoWrapper::SetVolumeSize(uint64_t size) {
    fsInfo_.mutable_detail()->mutable_volume()->set_volumesize(size);
}

}  // namespace mds
}  // namespace curvefs
