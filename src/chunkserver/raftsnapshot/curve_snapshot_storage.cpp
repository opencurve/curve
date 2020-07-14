/*
 * Project: curve
 * Created Date: 2020-06-09
 * Author: charisu
 * Copyright (c) 2018 netease
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

#include "src/chunkserver/raftsnapshot/curve_snapshot_storage.h"

namespace braft {
    DECLARE_bool(raft_create_parent_directories);
}

namespace curve {
namespace chunkserver {

butil::EndPoint CurveSnapshotStorage::_addr;

const char* CurveSnapshotStorage::_s_temp_path = "temp";

CurveSnapshotStorage::CurveSnapshotStorage(const std::string& path)
        : _path(path), _last_snapshot_index(0) {}

void CurveSnapshotStorage::ref(const int64_t index) {
    BAIDU_SCOPED_LOCK(_mutex);
    _ref_map[index]++;
}

int CurveSnapshotStorage::init() {
    butil::File::Error e;
    if (_fs == NULL) {
        _fs = braft::default_file_system();
    }
    if (!_fs->create_directory(
                _path, &e, braft::FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << _path << " : " << e;
        return -1;
    }
    // delete temp snapshot
    if (!_filter_before_copy_remote) {
        std::string temp_snapshot_path(_path);
        temp_snapshot_path.append("/");
        temp_snapshot_path.append(_s_temp_path);
        LOG(INFO) << "Deleting " << temp_snapshot_path;
        if (!_fs->delete_file(temp_snapshot_path, true)) {
            LOG(WARNING) << "delete temp snapshot path failed, path "
                         << temp_snapshot_path;
            return EIO;
        }
    }

    // delete old snapshot
    braft::DirReader* dir_reader = _fs->directory_reader(_path);
    if (!dir_reader->is_valid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or"
                        " PERMISSION. path: " << _path;
        delete dir_reader;
        return EIO;
    }
    std::set<int64_t> snapshots;
    while (dir_reader->next()) {
        int64_t index = 0;
        int match = sscanf(dir_reader->name(), BRAFT_SNAPSHOT_PATTERN, &index);
        if (match == 1) {
            snapshots.insert(index);
        }
    }
    delete dir_reader;

    // get last_snapshot_index
    if (snapshots.size() > 0) {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path(_path);
            butil::string_appendf(&snapshot_path,
                                "/" BRAFT_SNAPSHOT_PATTERN, index);
            LOG(INFO) << "Deleting snapshot `" << snapshot_path << "'";
            if (!_fs->delete_file(snapshot_path, true)) {
                LOG(WARNING) << "delete old snapshot path failed, path "
                             << snapshot_path;
                return EIO;
            }
        }

        _last_snapshot_index = *snapshots.begin();
        ref(_last_snapshot_index);
    }

    return 0;
}

int CurveSnapshotStorage::destroy_snapshot(const std::string& path) {
    LOG(INFO) << "Deleting "  << path;
    if (!_fs->delete_file(path, true)) {
        LOG(WARNING) << "delete old snapshot path failed, path " << path;
        return -1;
    }
    return 0;
}

void CurveSnapshotStorage::unref(const int64_t index) {
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    std::map<int64_t, int>::iterator it = _ref_map.find(index);
    if (it != _ref_map.end()) {
        it->second--;

        if (it->second == 0) {
            _ref_map.erase(it);
            lck.unlock();
            std::string old_path(_path);
            butil::string_appendf(&old_path, "/" BRAFT_SNAPSHOT_PATTERN, index);
            destroy_snapshot(old_path);
        }
    }
}

braft::SnapshotWriter* CurveSnapshotStorage::create() {
    return create(true);
}

braft::SnapshotWriter* CurveSnapshotStorage::create(bool from_empty) {
    CurveSnapshotWriter* writer = NULL;

    do {
        std::string snapshot_path(_path);
        snapshot_path.append("/");
        snapshot_path.append(_s_temp_path);

        // delete temp
        if (_fs->path_exists(snapshot_path) && from_empty) {
            if (destroy_snapshot(snapshot_path) != 0) {
                break;
            }
        }
        writer = new CurveSnapshotWriter(snapshot_path, _fs.get());
        if (writer->init() != 0) {
            LOG(ERROR) << "Fail to init writer, path: " << snapshot_path;
            delete writer;
            writer = NULL;
            break;
        }
        BRAFT_VLOG << "Create writer success, path: " << snapshot_path;
    } while (0);

    return writer;
}

braft::SnapshotCopier* CurveSnapshotStorage::start_to_copy_from(
                                        const std::string& uri) {
    CurveSnapshotCopier* copier = new CurveSnapshotCopier(this,
            _filter_before_copy_remote, _fs.get(), _snapshot_throttle.get());
    if (copier->init(uri) != 0) {
        LOG(ERROR) << "Fail to init copier from " << uri
                   << " path: " << _path;
        delete copier;
        return NULL;
    }
    copier->start();
    return copier;
}

int CurveSnapshotStorage::close(braft::SnapshotCopier* copier) {
    delete copier;
    return 0;
}

braft::SnapshotReader* CurveSnapshotStorage::copy_from(const std::string& uri) {
    braft::SnapshotCopier* c = start_to_copy_from(uri);
    if (c == NULL) {
        return NULL;
    }
    c->join();
    braft::SnapshotReader* reader = c->get_reader();
    close(c);
    return reader;
}

int CurveSnapshotStorage::close(braft::SnapshotWriter* writer) {
    return close(writer, false);
}

int CurveSnapshotStorage::close(braft::SnapshotWriter* writer_base,
                                bool keep_data_on_error) {
    CurveSnapshotWriter* writer =
                dynamic_cast<CurveSnapshotWriter*>(writer_base);
    int ret = writer->error_code();
    do {
        if (0 != ret) {
            break;
        }
        ret = writer->sync();
        if (ret != 0) {
            break;
        }
        int64_t old_index = 0;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            old_index = _last_snapshot_index;
        }
        int64_t new_index = writer->snapshot_index();
        if (new_index == old_index) {
            ret = EEXIST;
            break;
        }

        // rename temp to new
        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(_s_temp_path);
        std::string new_path(_path);
        butil::string_appendf(&new_path, "/" BRAFT_SNAPSHOT_PATTERN, new_index);
        LOG(INFO) << "Deleting " << new_path;
        if (!_fs->delete_file(new_path, true)) {
            LOG(WARNING) << "delete new snapshot path failed, path "
                         << new_path;
            ret = EIO;
            break;
        }
        LOG(INFO) << "Renaming " << temp_path << " to " << new_path;
        if (!_fs->rename(temp_path, new_path)) {
            LOG(WARNING) << "rename temp snapshot failed, from_path "
                         << temp_path << " to_path " << new_path;
            ret = EIO;
            break;
        }

        ref(new_index);
        {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK_EQ(old_index, _last_snapshot_index);
            _last_snapshot_index = new_index;
        }
        // unref old_index, ref new_index
        unref(old_index);
    } while (0);

    if (ret != 0 && !keep_data_on_error) {
        destroy_snapshot(writer->get_path());
    }
    delete writer;
    return ret != EIO ? 0 : -1;
}

braft::SnapshotReader* CurveSnapshotStorage::open() {
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (_last_snapshot_index != 0) {
        const int64_t last_snapshot_index = _last_snapshot_index;
        ++_ref_map[last_snapshot_index];
        lck.unlock();
        std::string snapshot_path(_path);
        butil::string_appendf(&snapshot_path, "/" BRAFT_SNAPSHOT_PATTERN,
                              last_snapshot_index);
        CurveSnapshotReader* reader = new CurveSnapshotReader(snapshot_path,
                            _addr, _fs.get(), _snapshot_throttle.get());
        if (reader->init() != 0) {
            CHECK(!lck.owns_lock());
            unref(last_snapshot_index);
            delete reader;
            return NULL;
        }
        return reader;
    } else {
        errno = ENODATA;
        return NULL;
    }
}

int CurveSnapshotStorage::close(braft::SnapshotReader* reader_) {
    CurveSnapshotReader* reader = dynamic_cast<CurveSnapshotReader*>(reader_);
    unref(reader->snapshot_index());
    delete reader;
    return 0;
}

int CurveSnapshotStorage::set_filter_before_copy_remote() {
    _filter_before_copy_remote = true;
    return 0;
}

int CurveSnapshotStorage::set_file_system_adaptor(
                                braft::FileSystemAdaptor* fs) {
    if (fs == NULL) {
        LOG(ERROR) << "file system is NULL, path: " << _path;
        return -1;
    }
    _fs = fs;
    return 0;
}

int CurveSnapshotStorage::set_snapshot_throttle(
                        braft::SnapshotThrottle* snapshot_throttle) {
    _snapshot_throttle = snapshot_throttle;
    return 0;
}

braft::SnapshotStorage* CurveSnapshotStorage::new_instance(
                                const std::string& uri) const {
    return new CurveSnapshotStorage(uri);
}


}  // namespace chunkserver
}  // namespace curve
