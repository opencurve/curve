/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/vfs/operations.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::FileSystem::EntryOut;
using ::curvefs::client::FileSystem::FileOut;

OperationsImpl::OperationsImpl(std::shared_ptr<FuseClient> client)
    : client_(client) {}

fuse_req_t OperationsImpl::DummyRequest() {
    return fuse_req_t();
}

fuse_req_t OperationsImpl::DummyFileInfo() {
    return fuse_file_info();
}

CURVEFS_ERROR OperationsImpl::Mkdir(Ino parent,
                                    const std::string& name,
                                    uint32_t mode) {
    EntryOut entryOut;
    auto req = DummyRequest();
    return client_->FuseOpMkDir(req, parent, name.c_str(), mode, &entryOut);
}

CURVEFS_ERROR OperationsImpl::OpenDir(Ino ino) {
    auto req = DummyRequest();
    auto fi = DummyFileInfo();
    return client_->FuseOpenDir(req, ino, &fi);
}

CURVEFS_ERROR OperationsImpl::CloseDir(Ino ino) {
    auto req = DummyRequest();
    auto fi = DummyFileInfo();
    return client_->FuseOpRelease(req, ino, &fi);
}

CURVEFS_ERROR OperationsImpl::RmDir(Ino parent, const std::string& name) {
    auto req = DummyRequest();
    return client_->FuseOpRmDir(req, parent, name.c_str());
}

CURVEFS_ERROR OperationsImpl::Write(Ino ino,
                                    off_t offset,
                                    char* buffer,
                                    size_t size,
                                    size_t* nwritten) {
    auto req = DummyRequest();
    auto fi = DummyFileInfo();
    FileOut fileOut;
    auto rc = client_->FuseOpWrite(req, ino, buffer, size,
                                  offset, &fi, &fileOut);
    if (rc == CURVEFS_ERROR::OK) {
        *nwritten = fileOut.nwritten;
    } else {
        *nwritten = 0;
    }
    return rc;
}

CURVEFS_ERROR OperationsImpl::Read(Ino ino,
                                   off_t offset,
                                   char* buffer,
                                   size_t size,
                                   size_t* nread) {
    auto req = DummyRequest();
    auto fi = DummyFileInfo();
    return client_->FuseOpRead(req, ino, buffer, size,
                               offset, &fi, nread);
}

CURVEFS_ERROR OperationsImpl::Close(Ino ino) {
    auto req = DummyRequest();
    auto fi = DummyFileInfo();
    return client_->FuseOpRelease(req, ino, &fi);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
