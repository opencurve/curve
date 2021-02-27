/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2020-06-11
 * Author: charisu
 */

// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include "src/chunkserver/raftsnapshot/curve_snapshot_copier.h"

namespace curve {
namespace chunkserver {

CurveSnapshotCopier::CurveSnapshotCopier(CurveSnapshotStorage* storage,
                                         bool filter_before_copy_remote,
                                         braft::FileSystemAdaptor* fs,
                                         braft::SnapshotThrottle* throttle)
    : _tid(INVALID_BTHREAD)
    , _cancelled(false)
    , _filter_before_copy_remote(filter_before_copy_remote)
    , _fs(fs)
    , _throttle(throttle)
    , _writer(NULL)
    , _storage(storage)
    , _reader(NULL)
    , _cur_session(NULL)
{}

CurveSnapshotCopier::~CurveSnapshotCopier() {
    CHECK(!_writer);
}

void *CurveSnapshotCopier::start_copy(void* arg) {
    CurveSnapshotCopier* c = reinterpret_cast<CurveSnapshotCopier*>(arg);
    c->copy();
    return NULL;
}

void CurveSnapshotCopier::copy() {
    do {
        // Download the files recorded in the snapshot meta
        load_meta_table();
        if (!ok()) {
            break;
        }
        filter();
        if (!ok()) {
            break;
        }
        std::vector<std::string> files;
        _remote_snapshot.list_files(&files);
        for (size_t i = 0; i < files.size() && ok(); ++i) {
            copy_file(files[i]);
        }

        // Download snapshot attachment file
        load_attach_meta_table();
        if (!ok()) {
            break;
        }
        std::vector<std::string> attachFiles;
        _remote_snapshot.list_attach_files(&attachFiles);
        for (size_t i = 0; i < attachFiles.size() && ok(); ++i) {
            copy_file(attachFiles[i], true);
        }
    } while (0);
    if (!ok() && _writer && _writer->ok()) {
        LOG(WARNING) << "Fail to copy, error_code " << error_code()
                     << " error_msg " << error_cstr()
                     << " writer path " << _writer->get_path();
        _writer->set_error(error_code(), error_cstr());
    }
    if (_writer) {
        // set_error for copier only when failed to close writer and copier was
        // ok before this moment
        if (_storage->close(_writer, _filter_before_copy_remote) != 0 && ok()) {
            set_error(EIO, "Fail to close writer");
        }
        _writer = NULL;
    }
    if (ok()) {
        _reader = _storage->open();
    }
}

void CurveSnapshotCopier::load_meta_table() {
    butil::IOBuf meta_buf;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (_cancelled) {
        set_error(ECANCELED, "%s", berror(ECANCELED));
        return;
    }
    scoped_refptr<braft::RemoteFileCopier::Session> session
            = _copier.start_to_copy_to_iobuf(BRAFT_SNAPSHOT_META_FILE,
                                            &meta_buf, NULL);
    _cur_session = session.get();
    lck.unlock();
    session->join();
    lck.lock();
    _cur_session = NULL;
    lck.unlock();
    if (!session->status().ok()) {
        LOG(WARNING) << "Fail to copy meta file : " << session->status();
        set_error(session->status().error_code(),
                  session->status().error_cstr());
        return;
    }
    if (_remote_snapshot._meta_table.load_from_iobuf_as_remote(meta_buf) != 0) {
        LOG(WARNING) << "Bad meta_table format";
        set_error(-1, "Bad meta_table format");
        return;
    }
    CHECK(_remote_snapshot._meta_table.has_meta());
}

void CurveSnapshotCopier::load_attach_meta_table() {
    butil::IOBuf meta_buf;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (_cancelled) {
        set_error(ECANCELED, "%s", berror(ECANCELED));
        return;
    }
    scoped_refptr<braft::RemoteFileCopier::Session> session
        = _copier.start_to_copy_to_iobuf(BRAFT_SNAPSHOT_ATTACH_META_FILE,
                                         &meta_buf, NULL);
    _cur_session = session.get();
    lck.unlock();
    session->join();
    lck.lock();
    _cur_session = NULL;
    lck.unlock();
    if (!session->status().ok()) {
        LOG(WARNING) << "Fail to copy attach meta file : " << session->status();
        set_error(session->status().error_code(),
                  session->status().error_cstr());
        return;
    }

    // If the attach meta table is empty, then there are no snapshot attachment files
    if (0 == meta_buf.size()) {
        return;
    }

    if (_remote_snapshot._attach_meta_table.load_from_iobuf_as_remote(meta_buf)
                                                            != 0) {
        LOG(WARNING) << "Bad attach_meta_table format";
        set_error(-1, "Bad attach_meta_table format");
        return;
    }
}

int CurveSnapshotCopier::filter_before_copy(CurveSnapshotWriter* writer,
                                    braft::SnapshotReader* last_snapshot) {
    std::vector<std::string> existing_files;
    writer->list_files(&existing_files);
    std::vector<std::string> to_remove;

    for (size_t i = 0; i < existing_files.size(); ++i) {
        if (_remote_snapshot.get_file_meta(existing_files[i], NULL) != 0) {
            to_remove.push_back(existing_files[i]);
            writer->remove_file(existing_files[i]);
        }
    }

    std::vector<std::string> remote_files;
    _remote_snapshot.list_files(&remote_files);
    for (size_t i = 0; i < remote_files.size(); ++i) {
        const std::string& filename = remote_files[i];
        braft::LocalFileMeta remote_meta;
        CHECK_EQ(0, _remote_snapshot.get_file_meta(
                filename, &remote_meta));
        if (!remote_meta.has_checksum()) {
            // Redownload file if this file doen't have checksum
            writer->remove_file(filename);
            to_remove.push_back(filename);
            continue;
        }

        braft::LocalFileMeta local_meta;
        if (writer->get_file_meta(filename, &local_meta) == 0) {
            if (local_meta.has_checksum() &&
                local_meta.checksum() == remote_meta.checksum()) {
                LOG(INFO) << "Keep file=" << filename
                          << " checksum=" << remote_meta.checksum()
                          << " in " << writer->get_path();
                continue;
            }
            // Remove files from writer so that the file is to be copied from
            // remote_snapshot or last_snapshot
            writer->remove_file(filename);
            to_remove.push_back(filename);
        }

        // Try find files in last_snapshot
        if (!last_snapshot) {
            continue;
        }
        if (last_snapshot->get_file_meta(filename, &local_meta) != 0) {
            continue;
        }
        if (!local_meta.has_checksum() ||
                    local_meta.checksum() != remote_meta.checksum()) {
            continue;
        }
        LOG(INFO) << "Found the same file=" << filename
                  << " checksum=" << remote_meta.checksum()
                  << " in last_snapshot=" << last_snapshot->get_path();
        if (local_meta.source() == braft::FILE_SOURCE_LOCAL) {
            std::string source_path = last_snapshot->get_path() + '/'
                                      + filename;
            std::string dest_path = writer->get_path() + '/'
                                      + filename;
            _fs->delete_file(dest_path, false);
            if (!_fs->link(source_path, dest_path)) {
                PLOG(ERROR) << "Fail to link " << source_path
                            << " to " << dest_path;
                continue;
            }
            // Don't delete linked file
            if (!to_remove.empty() && to_remove.back() == filename) {
                to_remove.pop_back();
            }
        }
        // Copy file from last_snapshot
        writer->add_file(filename, &local_meta);
    }

    if (writer->sync() != 0) {
        LOG(ERROR) << "Fail to sync writer on path=" << writer->get_path();
        return -1;
    }

    for (size_t i = 0; i < to_remove.size(); ++i) {
        std::string file_path = writer->get_path() + "/" + to_remove[i];
        _fs->delete_file(file_path, false);
    }

    return 0;
}

void CurveSnapshotCopier::filter() {
    _writer = reinterpret_cast<CurveSnapshotWriter*>(_storage->create(
                                    !_filter_before_copy_remote));
    if (_writer == NULL) {
        set_error(EIO, "Fail to create snapshot writer");
        return;
    }

    if (_filter_before_copy_remote) {
        braft::SnapshotReader* reader = _storage->open();
        if (filter_before_copy(_writer, reader) != 0) {
            LOG(WARNING) << "Fail to filter writer before copying"
                            ", path: " << _writer->get_path()
                         << ", destroy and create a new writer";
            _writer->set_error(-1, "Fail to filter");
            _storage->close(_writer, false);
            _writer = reinterpret_cast<CurveSnapshotWriter*>(
                                            _storage->create(true));
        }
        if (reader) {
            _storage->close(reader);
        }
        if (_writer == NULL) {
            set_error(EIO, "Fail to create snapshot writer");
            return;
        }
    }
    _writer->save_meta(_remote_snapshot._meta_table.meta());
    if (_writer->sync() != 0) {
        set_error(EIO, "Fail to sync snapshot writer");
        return;
    }
}

void CurveSnapshotCopier::copy_file(const std::string& filename, bool attch) {
    if (_writer->get_file_meta(filename, NULL) == 0) {
        LOG(INFO) << "Skipped downloading " << filename
                  << " path: " << _writer->get_path();
        return;
    }
    std::string rfilename = get_rfilename(filename);
    std::string file_path = _writer->get_path() + '/' + rfilename;
    butil::FilePath sub_path(rfilename);

    if (sub_path != sub_path.DirName() && sub_path.DirName().value() != ".") {
        butil::File::Error e;
        bool rc = false;
        if (braft::FLAGS_raft_create_parent_directories) {
            butil::FilePath sub_dir = butil::FilePath(
                            _writer->get_path()).Append(sub_path.DirName());
            rc = _fs->create_directory(sub_dir.value(), &e, true);
        } else {
            rc = create_sub_directory(
                    _writer->get_path(), sub_path.DirName().value(), _fs, &e);
        }
        if (!rc) {
            LOG(ERROR) << "Fail to create directory for " << file_path
                       << " : " << butil::File::ErrorToString(e);
            set_error(braft::file_error_to_os_error(e),
                      "Fail to create directory");
        }
    }
    braft::LocalFileMeta meta;
    _remote_snapshot.get_file_meta(filename, &meta);
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (_cancelled) {
        set_error(ECANCELED, "%s", berror(ECANCELED));
        return;
    }
    scoped_refptr<braft::RemoteFileCopier::Session> session
        = _copier.start_to_copy_to_file(filename, file_path, NULL);
    if (session == NULL) {
        LOG(WARNING) << "Fail to copy " << filename
                     << " path: " << _writer->get_path();
        set_error(-1, "Fail to copy %s", filename.c_str());
        return;
    }
    _cur_session = session.get();
    lck.unlock();
    session->join();
    lck.lock();
    _cur_session = NULL;
    lck.unlock();
    if (!session->status().ok()) {
        // If the file does not exist, then delete the file that was just opened
        if (session->status().error_code() == ENOENT) {
            bool rc = _fs->delete_file(file_path, false);
            if (!rc) {
                LOG(ERROR) << "Fail to delete file" << file_path
                           << " : " << ::berror(errno);
                set_error(errno,
                          "Fail to create delete file " + file_path);
            }
            return;
        }

        set_error(session->status().error_code(),
                  session->status().error_cstr());
        return;
    }
    // If it is an attach file, then there is no need to persist the file meta information
    if (!attch && _writer->add_file(filename, &meta) != 0) {
        set_error(EIO, "Fail to add file to writer");
        return;
    }
    if (_writer->sync() != 0) {
        set_error(EIO, "Fail to sync writer");
        return;
    }
}

std::string CurveSnapshotCopier::get_rfilename(const std::string& filename) {
    std::string rfilename;
    auto pos = filename.rfind("../");
    if (pos != filename.npos) {
        rfilename = filename.substr(pos + 3);
    } else {
        rfilename = filename;
    }
    return rfilename;
}

void CurveSnapshotCopier::start() {
    if (bthread_start_background(
                &_tid, NULL, start_copy, this) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        copy();
    }
}

void CurveSnapshotCopier::join() {
    bthread_join(_tid, NULL);
}

void CurveSnapshotCopier::cancel() {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_cancelled) {
        return;
    }
    _cancelled = true;
    if (_cur_session) {
        _cur_session->cancel();
    }
}

int CurveSnapshotCopier::init(const std::string& uri) {
    return _copier.init(uri, _fs, _throttle);
}

}  // namespace chunkserver
}  // namespace curve
