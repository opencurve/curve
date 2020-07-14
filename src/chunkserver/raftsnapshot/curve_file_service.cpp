/*
 * Project: curve
 * Created Date: 2020-06-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */

// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include <inttypes.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <braft/util.h>
#include <stack>
#include "src/chunkserver/raftsnapshot/curve_file_service.h"

namespace curve {
namespace chunkserver {

CurveFileService& kCurveFileService = CurveFileService::GetInstance();

void CurveFileService::get_file(::google::protobuf::RpcController* controller,
                               const ::braft::GetFileRequest* request,
                               ::braft::GetFileResponse* response,
                               ::google::protobuf::Closure* done) {
    scoped_refptr<braft::FileReader> reader;
    brpc::ClosureGuard done_gurad(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    Map::const_iterator iter = _reader_map.find(request->reader_id());
    if (iter == _reader_map.end()) {
        lck.unlock();
        /**
         * 为了和文件不存在的错误区分开来，且考虑到install snapshot
         * 的uri format为:remote://ip:port/reader_id，所以使用ENXIO
         * 代表reader id不存在的错误
         */
        LOG(ERROR) << "reader " << request->reader_id()
                   << " not found in reader map";
        cntl->SetFailed(ENXIO, "Fail to find reader=%" PRId64,
                                    request->reader_id());
        return;
    }
    // Don't touch iter ever after
    reader = iter->second;
    lck.unlock();
    LOG(INFO) << "get_file for " << cntl->remote_side() << " path="
              << reader->path() << " filename=" << request->filename()
              << " offset=" << request->offset() << " count="
              << request->count();

    if (request->count() <= 0 || request->offset() < 0) {
        LOG(ERROR) << "request count or offset elegal";
        cntl->SetFailed(brpc::EREQUEST, "Invalid request=%s",
                        request->ShortDebugString().c_str());
        return;
    }

    butil::IOBuf buf;
    bool is_eof = false;
    size_t read_count = 0;
    // 1. 如果是read attch meta file
    if (request->filename() == BRAFT_SNAPSHOT_ATTACH_META_FILE) {
        // 如果没有设置snapshot attachment，那么read文件的长度为零
        // 表示没有 snapshot attachment文件列表
        bool snapshotAttachmentExist = false;
        {
            std::unique_lock<braft::raft_mutex_t> lck(_mutex);
            if (nullptr == _snapshot_attachment.get()) {
                LOG(WARNING) << "_snapshot_attachment not set";
                is_eof = true;
                read_count = 0;
            } else {
                snapshotAttachmentExist = true;
            }
        }
        if (snapshotAttachmentExist) {
            // 否则获取snapshot attachment file list
            std::vector<std::string> files;
            _snapshot_attachment->list_attach_files(&files, reader->path());
            CurveSnapshotAttachMetaTable attachMetaTable;
            for (size_t i = 0; i < files.size(); ++i) {
                LocalFileMeta meta;
                attachMetaTable.add_attach_file(files[i], meta);
            }

            {
                std::unique_lock<braft::raft_mutex_t> lck(_mutex);
                auto it = _reader_map.find(request->reader_id());
                if (it == _reader_map.end()) {
                    cntl->SetFailed(ENXIO, "Fail to find reader=%" PRId64,
                                    request->reader_id());
                    return;
                }
                CurveSnapshotFileReader *reader =
                    dynamic_cast<CurveSnapshotFileReader*>(it->second.get());
                if (reader != nullptr) {
                    reader->set_attach_meta_table(attachMetaTable);
                } else {
                    LOG(ERROR) << "reader cannot be dynamic_cast"
                                  " to CurveSnapshotFileReader";
                    cntl->SetFailed(ENXIO, "Fail to case reader=%" PRId64,
                                    request->reader_id());
                    return;
                }
            }

            if (0 != attachMetaTable.save_to_iobuf_as_remote(&buf)) {
                // 内部错误: EINTERNAL
                LOG(ERROR) << "Fail to serialize "
                                "LocalSnapshotAttachMetaTable as iobuf";
                cntl->SetFailed(brpc::EINTERNAL,
                            "serialize snapshot attach meta table fail");
                return;
            } else {
                LOG(INFO) << "LocalSnapshotAttachMetaTable encode buf length = "
                          << buf.size();
            }
            is_eof = true;
            read_count = buf.size();
        }
    } else {
        // 2. 否则其它文件下载继续走raft原先的文件下载流程
        const int rc = reader->read_file(
                                &buf, request->filename(),
                                request->offset(), request->count(),
                                request->read_partly(),
                                &read_count,
                                &is_eof);
        if (rc != 0) {
            LOG(ERROR) << "Fail to read file " << reader->path() << "/"
                       << request->filename() << " error code: " << rc;
            cntl->SetFailed(rc, "Fail to read from path=%s filename=%s : %s",
                            reader->path().c_str(),
                            request->filename().c_str(), berror(rc));
            return;
        }
    }

    response->set_eof(is_eof);
    response->set_read_size(read_count);
    // skip empty data
    if (buf.size() == 0) {
        return;
    }

    braft::FileSegData seg_data;
    seg_data.append(buf, request->offset());
    cntl->response_attachment().swap(seg_data.data());
}

void CurveFileService::set_snapshot_attachment(
                SnapshotAttachment *snapshot_attachment) {
    _snapshot_attachment = snapshot_attachment;
}

CurveFileService::CurveFileService() {
    _next_id = ((int64_t)getpid() << 45) |
            (butil::gettimeofday_us() << 17 >> 17);
}

int CurveFileService::add_reader(braft::FileReader* reader,
                                 int64_t* reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    *reader_id = _next_id++;
    _reader_map[*reader_id] = reader;
    return 0;
}

int CurveFileService::remove_reader(int64_t reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _reader_map.erase(reader_id) == 1 ? 0 : -1;
}

}  // namespace chunkserver
}  // namespace curve
