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
 * Created Date: 2023-07-12
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/sdk/libcurvefs/libcurvefs.h"

using ::curvefs::client::filesystem::CURVEFS_ERROR;
using ::curvefs::client::filesystem::SysErr;
using ::curvefs::client::vfs::DirStream;
using ::curvefs::client::vfs::DirEntry;

static curvefs_mount_t* get_instance(uintptr_t instance_ptr) {
    return reinterpret_cast<curvefs_mount_t*>(instance_ptr);
}

uintptr_t curvefs_create() {
    auto mount = new curvefs_mount_t();
    mount->cfg = Configure::Default();
    mount->vfs = std::make_shared<VFS>();
    return reinterpret_cast<uintptr_t>(mount);
}

void curvefs_release(uintptr_t instance_ptr) {
    auto mount = get_instance(instance_ptr);
    delete mount;
    mount = nullptr;
}

void curvefs_conf_set(uintptr_t instance_ptr,
                      const char* key,
                      const char* value) {
    auto mount = get_instance(instance_ptr);
    return mount->cfg->Set(key, value);
}

int curvefs_mount(uintptr_t instance_ptr,
                  const char* fsname,
                  const char* mountpoint) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Mount(fsname, mountpoint, mount->cfg);
    return SysErr(rc);
}

int curvefs_umonut(uintptr_t instance_ptr) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Umount();
    return SysErr(rc);
}

int curvefs_mkdir(uintptr_t instance_ptr, const char* path, uint16_t mode) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->MkDir(path, mode);
    return SysErr(rc);
}

int curvefs_rmdir(uintptr_t instance_ptr, const char* path) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->RmDir(path);
    return SysErr(rc);
}

int curvefs_opendir(uintptr_t instance_ptr,
                    const char* path,
                    dir_stream_t* dir_stream) {
    DirStream stream;
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->OpenDir(path, &stream);
    if (rc == CURVEFS_ERROR::OK) {
        *dir_stream = *reinterpret_cast<dir_stream_t*>(&stream);
    }
    return SysErr(rc);
}

ssize_t curvefs_readdir(uintptr_t instance_ptr,
                        dir_stream_t* dir_stream,
                        dirent_t* dirent) {
    DirEntry dirEntry;
    auto mount = get_instance(instance_ptr);
    DirStream* stream = reinterpret_cast<DirStream*>(dir_stream);
    auto rc = mount->vfs->ReadDir(stream, &dirEntry);
    if (rc == CURVEFS_ERROR::OK) {
        strcpy(dirent->name, dirEntry.name.c_str());
        mount->vfs->Attr2Stat(&dirEntry.attr, &dirent->stat);
        return 1;
    } else if (rc == CURVEFS_ERROR::END_OF_FILE) {
        return 0;
    }
    return SysErr(rc);
}

int curvefs_closedir(uintptr_t instance_ptr, dir_stream_t* dir_stream) {
    DirStream* stream = reinterpret_cast<DirStream*>(dir_stream);
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->CloseDir(stream);
    return SysErr(rc);
}

int curvefs_open(uintptr_t instance_ptr,
                 const char* path,
                 uint32_t flags,
                 uint16_t mode) {
    CURVEFS_ERROR rc;
    auto mount = get_instance(instance_ptr);
    if (flags & O_CREAT) {
        rc = mount->vfs->Create(path, mode);
        if (rc != CURVEFS_ERROR::OK) {
            return SysErr(rc);
        }
    }

    uint64_t fd;
    rc = mount->vfs->Open(path, flags, mode, &fd);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }
    return static_cast<int>(fd);
}

int curvefs_lseek(uintptr_t instance_ptr,
                  int fd,
                  uint64_t offset,
                  int whence) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->LSeek(fd, offset, whence);
    return SysErr(rc);
}

ssize_t curvefs_read(uintptr_t instance_ptr,
                     int fd,
                     char* buffer,
                     size_t count) {
    size_t nread;
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Read(fd, buffer, count, &nread);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }
    return nread;
}

ssize_t curvefs_write(uintptr_t instance_ptr,
                      int fd,
                      char* buffer,
                      size_t count) {
    size_t nwritten;
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Write(fd, buffer, count, &nwritten);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }
    return nwritten;
}

int curvefs_fsync(uintptr_t instance_ptr, int fd) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->FSync(fd);
    return SysErr(rc);
}

int curvefs_close(uintptr_t instance_ptr, int fd) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Close(fd);
    return SysErr(rc);
}

int curvefs_unlink(uintptr_t instance_ptr, const char* path) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Unlink(path);
    return SysErr(rc);
}

int curvefs_statfs(uintptr_t instance_ptr,
                   const char* path,
                   struct statvfs* statvfs) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->StatFS(path, statvfs);
    return SysErr(rc);
}

int curvefs_lstat(uintptr_t instance_ptr, const char* path, struct stat* stat) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->LStat(path, stat);
    return SysErr(rc);
}

int curvefs_fstat(uintptr_t instance_ptr, int fd, struct stat* stat) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->FStat(fd, stat);
    return SysErr(rc);
}

int curvefs_setattr(uintptr_t instance_ptr,
                    const char* path,
                    struct stat* stat,
                    int to_set) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->SetAttr(path, stat, to_set);
    return SysErr(rc);
}

int curvefs_chmod(uintptr_t instance_ptr, const char* path, uint16_t mode) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Chmod(path, mode);
    return SysErr(rc);
}

int curvefs_rename(uintptr_t instance_ptr,
                   const char* oldpath,
                   const char* newpath) {
    auto mount = get_instance(instance_ptr);
    auto rc = mount->vfs->Rename(oldpath, newpath);
    return SysErr(rc);
}
