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

#include <list>
#include <string>
#include <vector>
#include <algorithm>

#include "curvefs/src/metaserver/metaserver_service.h"

namespace curvefs {
namespace metaserver {

void MetaServerServiceImpl::DEFINE_RPC(CreateDentry) {
    brpc::ClosureGuard doneGuard(done);
    auto rc = dentryManager_->CreateDentry(request->dentry());
    response->set_statuscode(rc);
}

void MetaServerServiceImpl::DEFINE_RPC(DeleteDentry) {
    brpc::ClosureGuard doneGuard(done);
    Dentry dentry;
    dentry.set_fsid(request->fsid());
    dentry.set_parentinodeid(request->parentinodeid());
    dentry.set_name(request->name());
    dentry.set_txid(request->txid());

    auto rc = dentryManager_->DeleteDentry(dentry);
    response->set_statuscode(rc);
}

void MetaServerServiceImpl::DEFINE_RPC(GetDentry) {
    brpc::ClosureGuard doneGuard(done);
    auto dentry = response->mutable_dentry();
    dentry->set_fsid(request->fsid());
    dentry->set_parentinodeid(request->parentinodeid());
    dentry->set_name(request->name());
    dentry->set_txid(request->txid());

    auto rc = dentryManager_->GetDentry(dentry);
    response->set_statuscode(rc);
    if (rc != MetaStatusCode::OK) {
        response->clear_dentry();
    }
}

void MetaServerServiceImpl::DEFINE_RPC(ListDentry) {
    brpc::ClosureGuard doneGuard(done);
    Dentry dentry;
    dentry.set_fsid(request->fsid());
    dentry.set_parentinodeid(request->dirinodeid());
    dentry.set_txid(request->txid());
    if (request->has_last()) {
        dentry.set_name(request->last());
    }

    std::vector<Dentry> dentrys;
    auto rc = dentryManager_->ListDentry(dentry, &dentrys, request->count());
    response->set_statuscode(rc);
    if (rc == MetaStatusCode::OK && !dentrys.empty()) {
        *response->mutable_dentrys() = { dentrys.begin(), dentrys.end() };
    }
}

void MetaServerServiceImpl::DEFINE_RPC(PrepareRenameTx) {
    brpc::ClosureGuard doneGuard(done);

    std::vector<Dentry> dentrys{
        request->dentrys().begin(),
        request->dentrys().end()
    };

    auto rc = dentryManager_->HandleRenameTx(dentrys);
    response->set_statuscode(rc);
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

    if (request->has_s3chunkinfolist()) {
        VLOG(1) << "update inode has extent";
        inode.mutable_s3chunkinfolist()->CopyFrom(request->s3chunkinfolist());
        needUpdate = true;
    }

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

}  // namespace metaserver
}  // namespace curvefs
