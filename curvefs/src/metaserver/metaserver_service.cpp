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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "curvefs/src/metaserver/metaserver_service.h"
#include <list>
#include <string>

namespace curvefs {
namespace metaserver {
void MetaServerServiceImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetDentryRequest* request,
    ::curvefs::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    MetaStatusCode status = dentryManager_->GetDentry(
        fsId, parentInodeId, name, response->mutable_dentry());
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        response->clear_dentry();
    }
    return;
}

void MetaServerServiceImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::ListDentryRequest* request,
    ::curvefs::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->dirinodeid();

    std::list<Dentry> dentryList;
    MetaStatusCode status =
        dentryManager_->ListDentry(fsId, parentInodeId, &dentryList);
    if (status != MetaStatusCode::OK) {
        response->set_statuscode(status);
        return;
    }

    // find last dentry
    std::string last;
    bool findLast = false;
    auto iter = dentryList.begin();
    if (request->has_last()) {
        last = request->last();
        VLOG(1) << "last = " << last;
        for (; iter != dentryList.end(); ++iter) {
            if (iter->name() == last) {
                iter++;
                findLast = true;
                break;
            }
        }
    }

    if (!findLast) {
        iter = dentryList.begin();
    }

    uint32_t count = UINT32_MAX;
    if (request->has_count()) {
        count = request->count();
        VLOG(1) << "count = " << count;
    }

    uint64_t index = 0;
    while (iter != dentryList.end() && index < count) {
        Dentry* dentry = response->add_dentrys();
        dentry->CopyFrom(*iter);
        VLOG(1) << "return client, index = " << index
                  << ", dentry :" << iter->ShortDebugString();
        index++;
        iter++;
    }

    VLOG(1) << "return count = " << index;

    response->set_statuscode(status);
    return;
}

void MetaServerServiceImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateDentryRequest* request,
    ::curvefs::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = dentryManager_->CreateDentry(request->dentry());
    response->set_statuscode(status);
    return;
}

void MetaServerServiceImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteDentryRequest* request,
    ::curvefs::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    MetaStatusCode status =
        dentryManager_->DeleteDentry(fsId, parentInodeId, name);
    response->set_statuscode(status);
    return;
}

void MetaServerServiceImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetInodeRequest* request,
    ::curvefs::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    MetaStatusCode status =
        inodeManager_->GetInode(fsId, inodeId, response->mutable_inode());
    if (status != MetaStatusCode::OK) {
        response->clear_inode();
    }
    response->set_statuscode(status);
    return;
}

void MetaServerServiceImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateInodeRequest* request,
    ::curvefs::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t length = request->length();
    uint32_t uid = request->uid();
    uint32_t gid = request->gid();
    uint32_t mode = request->mode();
    FsFileType type = request->type();
    std::string symlink;
    if (type == FsFileType::TYPE_SYM_LINK) {
        if (!request->has_symlink()) {
            response->set_statuscode(MetaStatusCode::SYM_LINK_EMPTY);
            return;
        }

        symlink = request->symlink();
        if (symlink.empty()) {
            response->set_statuscode(MetaStatusCode::SYM_LINK_EMPTY);
            return;
        }
    }

    MetaStatusCode status = inodeManager_->CreateInode(
        fsId, length, uid, gid, mode, type, symlink, response->mutable_inode());
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        response->clear_inode();
    }
    return;
}

void MetaServerServiceImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateRootInodeRequest* request,
    ::curvefs::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint32_t uid = request->uid();
    uint32_t gid = request->gid();
    uint32_t mode = request->mode();

    MetaStatusCode status =
        inodeManager_->CreateRootInode(fsId, uid, gid, mode);
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateRootInode fail, fsId = " << fsId
                   << ", uid = " << uid << ", gid = " << gid
                   << ", mode = " << mode
                   << ", retCode = " << MetaStatusCode_Name(status);
    }
    return;
}

void MetaServerServiceImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::UpdateInodeRequest* request,
    ::curvefs::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    if (request->has_volumeextentlist() && request->has_s3chunkinfolist()) {
        LOG(ERROR) << "only one of type space info, choose volume or s3";
        response->set_statuscode(MetaStatusCode::PARAM_ERROR);
        return;
    }

    Inode inode;
    MetaStatusCode status = inodeManager_->GetInode(fsId, inodeId, &inode);
    if (status != MetaStatusCode::OK) {
        response->set_statuscode(status);
        return;
    }

    bool needUpdate = false;

#define UPDATE_INODE(param)                  \
    if (request->has_##param()) {            \
        inode.set_##param(request->param()); \
        needUpdate = true;                   \
    }

    UPDATE_INODE(length)
    UPDATE_INODE(ctime)
    UPDATE_INODE(mtime)
    UPDATE_INODE(atime)
    UPDATE_INODE(uid)
    UPDATE_INODE(gid)
    UPDATE_INODE(mode)

    if (request->has_volumeextentlist()) {
        VLOG(1) << "update inode has extent";
        inode.mutable_volumeextentlist()->CopyFrom(request->volumeextentlist());
        needUpdate = true;
    }

    // TODO(huyao): delete version
    // if (request->has_s3chunkinfolist()) {
    //     VLOG(1) << "update inode has extent";
    //     inode.mutable_s3chunkinfolist()->CopyFrom(request->s3chunkinfolist());
    //     needUpdate = true;
    // }

    if (needUpdate) {
        // TODO(cw123) : Update each field individually
        status = inodeManager_->UpdateInode(inode);
        response->set_statuscode(status);
    } else {
        LOG(WARNING) << "inode has no param to update";
        response->set_statuscode(MetaStatusCode::OK);
    }

    return;
}

void MetaServerServiceImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteInodeRequest* request,
    ::curvefs::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    MetaStatusCode status = inodeManager_->DeleteInode(fsId, inodeId);
    response->set_statuscode(status);
    return;
}

// TODO(huyao): delete version
/*
void MetaServerServiceImpl::UpdateInodeS3Version(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::UpdateInodeS3VersionRequest* request,
    ::curvefs::metaserver::UpdateInodeS3VersionResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    uint64_t version;
    MetaStatusCode status =
        inodeManager_->UpdateInodeVersion(fsId, inodeId, &version);
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateInodeS3Version fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(status);
    } else {
        response->set_version(version);
    }
    return;
}
*/

}  // namespace metaserver
}  // namespace curvefs
