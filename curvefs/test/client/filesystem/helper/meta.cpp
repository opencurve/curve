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
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/test/client/filesystem/helper/meta.h"

namespace curvefs {
namespace client {
namespace filesystem {

AttrOption AttrOption::type(FsFileType type) {
    type_ = type;
    return *this;
}

AttrOption AttrOption::mode(uint32_t mode) {
    mode_ = mode;
    return *this;
}

AttrOption AttrOption::nlink(uint32_t nlink) {
    nlink_ = nlink;
    return *this;
}

AttrOption AttrOption::uid(uint32_t uid) {
    uid_ = uid;
    return *this;
}

AttrOption AttrOption::gid(uint32_t gid) {
    gid_ = gid;
    return *this;
}

AttrOption AttrOption::length(uint64_t length) {
    length_ = length;
    return *this;
}

AttrOption AttrOption::rdev(uint64_t rdev) {
    rdev_ = rdev;
    return *this;
}

AttrOption AttrOption::atime(uint64_t seconds, uint32_t naoSeconds) {
    atime_ = TimeSpec(seconds, naoSeconds);
    return *this;
}

AttrOption AttrOption::mtime(uint64_t seconds, uint32_t naoSeconds) {
    mtime_ = TimeSpec(seconds, naoSeconds);
    return *this;
}

AttrOption AttrOption::ctime(uint64_t seconds, uint32_t naoSeconds) {
    ctime_ = TimeSpec(seconds, naoSeconds);
    return *this;
}

InodeOption InodeOption::ctime(uint64_t seconds, uint32_t naoSeconds) {
    ctime_ = TimeSpec(seconds, naoSeconds);
    return *this;
}

InodeOption InodeOption::mtime(uint64_t seconds, uint32_t naoSeconds) {
    mtime_ = TimeSpec(seconds, naoSeconds);
    return *this;
}

InodeOption InodeOption::length(uint64_t length) {
    length_ = length;
    return *this;
}

InodeOption InodeOption::metaClient(
    std::shared_ptr<MetaServerClient> metaClient) {
    metaClient_ = metaClient;
    return *this;
}

InodeAttr MkAttr(Ino ino, AttrOption option) {
    InodeAttr attr;
    attr.set_inodeid(ino);
    if (option.type_) { attr.set_type(option.type_); }
    attr.set_mode(option.mode_);
    attr.set_nlink(option.nlink_);
    attr.set_uid(option.uid_);
    attr.set_gid(option.gid_);
    attr.set_length(option.length_);
    attr.set_rdev(option.rdev_);
    attr.set_atime(option.atime_.seconds);
    attr.set_atime_ns(option.atime_.nanoSeconds);
    attr.set_mtime(option.mtime_.seconds);
    attr.set_mtime_ns(option.mtime_.nanoSeconds);
    attr.set_ctime(option.ctime_.seconds);
    attr.set_ctime_ns(option.ctime_.nanoSeconds);
    return attr;
}

std::shared_ptr<InodeWrapper> MkInode(Ino ino, InodeOption option) {
    Inode inode;
    inode.set_inodeid(ino);
    inode.set_ctime(option.ctime_.seconds);
    inode.set_ctime_ns(option.ctime_.nanoSeconds);
    inode.set_mtime(option.mtime_.seconds);
    inode.set_mtime_ns(option.mtime_.nanoSeconds);
    inode.set_length(option.length_);
    return std::make_shared<InodeWrapper>(inode, option.metaClient_);
}

Dentry MkDentry(Ino ino, const std::string& name) {
    Dentry dentry;
    dentry.set_inodeid(ino);
    dentry.set_name(name);
    return dentry;
}

DirEntry MkDirEntry(Ino ino,
                    const std::string& name,
                    InodeAttr attr) {
    return DirEntry(ino, name, attr);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
