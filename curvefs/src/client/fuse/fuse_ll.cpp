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
 * Created Date: 2023-06-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/fuse/fuse_ll.h"
#include "curvefs/src/client/fuse/helper.h"
#include "curvefs/src/client/filesystem/error.h"

using ::curvefs::client::filesystem::CURVEFS_ERROR;

void FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    CURVEFS_ERROR rc;
    auto client = Context();
    AccessLogGuard log([&](){
        return StrFormat("init : %s", StrErr(rc));
    });

    rc = client->FuseOpInit(userdata, conn);
}

void FuseOpDestroy(void *userdata) {
    auto client = Client();
    AccessLogGuard log([&](){
        return StrFormat("destory : OK");
    });
    client->FuseOpDestroy(userdata);
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Lookup);
    AccessLogGuard log([&](){
        return StrFormat("lookup (%d,%s): %s%s",
                         parent, name, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpLookup(req, parent, name, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    AttrOut attrOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(GetAttr);
    AccessLogGuard log([&](){
        return StrFormat("getattr (%d): %s%s",
                         ino, StrErr(rc), StrAttr(attrOut));
    });

    rc = client->FuseOpGetAttr(req, ino, fi, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyAttr(req, &attrOut);
}

void FuseOpSetAttr(fuse_req_t req,
                   fuse_ino_t ino,
                   struct stat* attr,
                   int to_set,
                   struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    AttrOut attrOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(SetAttr);
    AccessLogGuard log([&](){
        return StrFormat("setattr (%d,0x%X): %s%s",
                         ino, to_set, StrErr(rc), StrAttr(attrOut));
    });

    rc = client->FuseOpSetAttr(req, ino, attr, to_set, fi, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyAttr(req, &attrOut);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
    CURVEFS_ERROR rc;
    std::string link;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadLink);
    AccessLogGuard log([&](){
        return StrFormat("readlink (%d): %s %s", ino, StrErr(rc), link.c_str());
    });

    rc = client->FuseOpReadLink(req, ino, &link);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyReadlink(req, link);
}

void FuseOpMkNod(fuse_req_t req,
                 fuse_ino_t parent,
                 const char* name,
                 mode_t mode,
                 dev_t rdev) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(MkNod);
    AccessLogGuard log([&](){
        return StrFormat("mknod (%d,%s,%s:0%04o): %s%s",
                         parent, name, StrMode(mode), mode,
                         StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpMkNod(req, parent, name, mode, rdev, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpMkDir(fuse_req_t req,
                 fuse_ino_t parent,
                 const char* name,
                 mode_t mode) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(MkDir);
    AccessLogGuard log([&](){
        return StrFormat("mkdir (%d,%s,%s:0%04o): %s%s",
                         parent, name, StrMode(mode), mode,
                         StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpMkDir(req, parent, name, mode, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Unlink);
    AccessLogGuard log([&](){
        return StrFormat("unlink (%d,%s): %s", parent, name, StrErr(rc));
    });

    rc = client->FuseOpUnlink(req, parent, name);
    return fs->ReplyError(req, rc);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(RmDir);
    AccessLogGuard log([&](){
        return StrFormat("rmdir (%d,%s): %s", parent, name, StrErr(rc));
    });

    rc = client->FuseOpRmDir(req, parent, name);
    return fs->ReplyError(req, rc);
}

void FuseOpSymlink(fuse_req_t req,
                   const char *link,
                   fuse_ino_t parent,
                   const char* name) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Symlink);
    AccessLogGuard log([&](){
        return StrFormat("symlink (%d,%s,%s): %s%s",
                         parent, name, link, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpSymlink(req, link, parent, name, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpRename(fuse_req_t req,
                  fuse_ino_t parent,
                  const char *name,
                  fuse_ino_t newparent,
                  const char *newname,
                  unsigned int flags) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Rename);
    AccessLogGuard log([&](){
        return StrFormat("rename (%d,%s,%d,%s,%d): %s",
                         parent, name, newparent, newname, flags, StrErr(rc));
    });

    rc = client->FuseOpRename(req, parent, name, newparent, newname, flags);
    return fs->ReplyError(req, rc);
}

void FuseOpLink(fuse_req_t req,
                fuse_ino_t ino,
                fuse_ino_t newparent,
                const char *newname) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Link);
    AccessLogGuard log([&](){
        return StrFormat(
            "link (%d,%d,%s): %s%s",
            ino, newparent, newname, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpLink(req, ino, newparent, newname, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    FileOut fileOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Open);
    AccessLogGuard log([&](){
        return StrFormat("open (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
    });

    rc = client->FuseOpOpen(req, ino, fi, &fileOut);
    if (rc != CURVEFS_ERROR::OK) {
        fs->ReplyError(req, rc);
        return;
    }
    return fs->ReplyOpen(req, &fileOut);
}

void FuseOpRead(fuse_req_t req,
                fuse_ino_t ino,
                size_t size,
                off_t off,
                struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    size_t rSize = 0;
    std::unique_ptr<char[]> buffer(new char[size]);
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Read);
    AccessLogGuard log([&](){
        return StrFormat("read (%d,%d,%d,%d): %s (%d)",
                         ino, size, off, fi->fh, StrErr(rc), rSize);
    });

    ReadThrottleAdd(size);
    rc = client->FuseOpRead(req, ino, size, off, fi, buffer.get(), &rSize);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(rSize);
    bufvec.buf[0].mem = buffer.get();
    return fs->ReplyData(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req,
                 fuse_ino_t ino,
                 const char* buf,
                 size_t size,
                 off_t off,
                 struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    FileOut fileOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Read);
    AccessLogGuard log([&](){
        return StrFormat("write (%d,%d,%d,%d): %s (%d)",
                         ino, size, off, fi->fh, StrErr(rc), fileOut.nwritten);
    });

    WriteThrottleAdd(size);
    rc = client->FuseOpWrite(req, ino, buf, size, off, fi, &fileOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyWrite(req, &fileOut);
}

void FuseOpFlush(fuse_req_t req,
                 fuse_ino_t ino,
                 struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Flush);
    AccessLogGuard log([&](){
        return StrFormat("flush (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpFlush(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Release);
    AccessLogGuard log([&](){
        return StrFormat("release (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpRelease(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpFsync(fuse_req_t req,
                 fuse_ino_t ino,
                 int datasync,
                 struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Fsync);
    AccessLogGuard log([&](){
        return StrFormat("fsync (%d,%d): %s", ino, datasync, StrErr(rc));
    });

    rc = client->FuseOpFsync(req, ino, datasync, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(OpenDir);
    AccessLogGuard log([&](){
        return StrFormat("opendir (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
    });

    rc = client->FuseOpOpenDir(req, ino, fi);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyOpen(req, fi);
}

void FuseOpReadDir(fuse_req_t req,
                   fuse_ino_t ino,
                   size_t size,
                   off_t off,
                   struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    char *buffer;
    size_t rSize;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadDir);
    AccessLogGuard log([&](){
        return StrFormat("readdir (%d,%d,%d): %s (%d)",
                         ino, size, off, StrErr(rc), rSize);
    });

    rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &rSize, false);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyBuffer(req, buffer, rSize);
}

void FuseOpReadDirPlus(fuse_req_t req,
                       fuse_ino_t ino,
                       size_t size,
                       off_t off,
                       struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    char *buffer;
    size_t rSize;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadDir);
    AccessLogGuard log([&](){
        return StrFormat("readdirplus (%d,%d,%d): %s (%d)",
                         ino, size, off, StrErr(rc), rSize);
    });

    rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &rSize, true);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }

    return fs->ReplyBuffer(req, buffer, rSize);
}

void FuseOpReleaseDir(fuse_req_t req,
                      fuse_ino_t ino,
                      struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReleaseDir);
    AccessLogGuard log([&](){
        return StrFormat("releasedir (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpReleaseDir(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
    CURVEFS_ERROR rc;
    struct statvfs stbuf;
    auto client = Client();
    auto fs = client->GetFileSystem();
    AccessLogGuard log([&](){
        return StrFormat("statfs (%d): %s", ino, StrErr(rc));
    });

    rc = client->FuseOpStatFs(req, ino, &stbuf);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyStatfs(req, &stbuf);
}

void FuseOpSetXattr(fuse_req_t req,
                    fuse_ino_t ino,
                    const char* name,
                    const char* value,
                    size_t size,
                    int flags) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    AccessLogGuard log([&](){
        return StrFormat("setxattr (%d,%s,%d,%d): %s",
                         ino, name, size, flags, StrErr(rc));
    });

    rc = client->FuseOpSetXattr(req, ino, name, value, size, flags);
    return fs->ReplyError(req, rc);
}

void FuseOpGetXattr(fuse_req_t req,
                    fuse_ino_t ino,
                    const char *name,
                    size_t size) {
    CURVEFS_ERROR rc;
    std::string value;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(GetXattr);
    AccessLogGuard log([&](){
        return StrFormat("getxattr (%d,%s,%d): %s (%d)",
                         ino, name, size, StrErr(rc), value.size());
    });

    rc = Client()->FuseOpGetXattr(req, ino, name, &value, size);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    } else if (size == 0) {
        return fs->ReplyXattr(req, value.length());
    }
    return fs->ReplyBuffer(req, value.data(), value.length());
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
    CURVEFS_ERROR rc;
    size_t xattrSize = 0;
    std::unique_ptr<char[]> buf(new char[size]);
    std::memset(buf.get(), 0, size);
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ListXattr);
    AccessLogGuard log([&](){
        return StrFormat("listxattr (%d,%s): %s (%d)",
                         ino, size, StrErr(rc), xattrSize);
    });

    rc = Client()->FuseOpListXattr(req, ino, buf.get(), size, &xattrSize);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    } else if (size == 0) {
        return fs->ReplyXattr(req, xattrSize);
    }
    return fs->ReplyBuffer(req, buf.get(), xattrSize);
}

void FuseOpCreate(fuse_req_t req,
                  fuse_ino_t parent,
                  const char* name,
                  mode_t mode,
                  struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Create);
    AccessLogGuard log([&](){
        return StrFormat("create (%d,%s): %s%s [fh:%d]",
                         parent, name, StrErr(rc), StrEntry(entryOut), fi->fh);
    });

    rc = client->FuseOpCreate(req, parent, name, mode, fi, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyCreate(req, &entryOut, fi);
}

// TODO(wuhanqing): implement for volume storage
void FuseOpBmap(fuse_req_t req,
                fuse_ino_t /*ino*/,
                size_t /*blocksize*/,
                uint64_t /*idx*/) {
    auto client = Client();
    auto fs = client->GetFileSystem();
    return fs->ReplyError(req, CURVEFS_ERROR::NOT_SUPPORT,);
}
