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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */
#include "curvefs/src/client/curve_fuse_op.h"
#include "curvefs/src/client/fuse_client.h"

using ::curvefs::client::FuseClient;

static FuseClient *g_ClientInstance = nullptr;

int InitFuseClient() {
    g_ClientInstance = new FuseClient();
    return g_ClientInstance->Init();
}

void UnInitFuseClient() {
    g_ClientInstance->UnInit();
    delete g_ClientInstance;
}

void curve_ll_init(void *userdata, struct fuse_conn_info *conn) {
    g_ClientInstance->init(userdata, conn);
}

void curve_ll_destroy(void *userdata) {
    g_ClientInstance->destroy(userdata);
}

void curve_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    g_ClientInstance->lookup(req, parent, name);
}

void curve_ll_getattr(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    g_ClientInstance->getattr(req, ino, fi);
}

void curve_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
         struct fuse_file_info *fi) {
    g_ClientInstance->readdir(req, ino, size, off, fi);
}

void curve_ll_open(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi) {
    g_ClientInstance->open(req, ino, fi);
}

void curve_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
          struct fuse_file_info *fi) {
    g_ClientInstance->read(req, ino, size, off, fi);
}

void curve_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
           size_t size, off_t off, struct fuse_file_info *fi) {
    g_ClientInstance->write(req, ino, buf, size, off, fi);
}

void curve_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
        mode_t mode, struct fuse_file_info *fi) {
    g_ClientInstance->create(req, parent, name, mode, fi);
}

void curve_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
           mode_t mode, dev_t rdev) {
    g_ClientInstance->mknod(req, parent, name, mode, rdev);
}

void curve_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
           mode_t mode) {
    g_ClientInstance->mkdir(req, parent, name, mode);
}

void curve_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    g_ClientInstance->unlink(req, parent, name);
}

void curve_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    g_ClientInstance->rmdir(req, parent, name);
}

void curve_ll_opendir(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    g_ClientInstance->opendir(req, ino, fi);
}

void curve_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
         int to_set, struct fuse_file_info *fi) {
    g_ClientInstance->setattr(req, ino, attr, to_set, fi);
}

