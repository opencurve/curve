/*
 * Project: curve
 * Created Date: 18-12-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/op_context.h"

#include <glog/logging.h>
#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <brpc/closure_guard.h>

#include <memory>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

int ChunkOpContext::Encode(butil::IOBuf *data) {
    if (nullptr == data) {
        return -1;
    }

    /* 1. append RequestType */
    data->push_back((uint8_t) RequestType::CHUNK_OP);
    /* 2. append op request */
    const uint32_t metaSize = butil::HostToNet32(request_->ByteSize());
    data->append(&metaSize, sizeof(uint32_t));
    butil::IOBufAsZeroCopyOutputStream wrapper(data);
    if (!request_->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }
    /* 3. append op data */
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(cntl_);
    if (CHUNK_OP_TYPE::CHUNK_OP_WRITE == request_->optype()) {
        data->append(cntl->request_attachment());
    }

    return 0;
}

int ChunkOpContext::OnApply(std::shared_ptr<CopysetNode> copysetNode) {
    int ret = 0;
    size_t size;
    bool dsRet = true;
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(cntl_);
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            char *readBuffer = nullptr;
            size = request_->size();
            /* FixMe(wudemiao) 后期修订为统一的 buffer 接口 */
            readBuffer = new(std::nothrow)char[size];
            CHECK(nullptr != readBuffer) << "new readBuffer failed, "
                                         << " errno: " << errno
                                         << " error str: " << strerror(errno);
            ::memset(readBuffer, 0, size);
            dsRet = copysetNode->dataStore_->ReadChunk(request_->chunkid(),
                                                       readBuffer,
                                                       request_->offset(),
                                                       &size);
            if (true == dsRet) {
                cntl->response_attachment().append(readBuffer, size);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
                DVLOG(9) << "read: (" << request_->logicpoolid() << ", "
                         << request_->copysetid()
                         << ") chunkid: " << request_->chunkid()
                         << " data size: " << request_->size()
                         << " error, read len :" << size << ", error: "
                         << strerror(errno);
            } else {
                LOG(ERROR) << "read: (" << request_->logicpoolid() << ", "
                           << request_->copysetid()
                           << ") chunkid: " << request_->chunkid()
                           << " data size: " << request_->size()
                           << " error, read len :" << size << ", error: "
                           << strerror(errno);
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            delete[] readBuffer;
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            DVLOG(9) << "Apply write request with header: "
                     << reinterpret_cast<const unsigned int *>(cntl->request_attachment().to_string().c_str());  //NOLINT
            dsRet = copysetNode->dataStore_->WriteChunk(request_->chunkid(),
                                                        cntl->request_attachment().to_string().c_str(),  //NOLINT
                                                        request_->offset(),
                                                        request_->size());
            if (true == dsRet) {
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
                DVLOG(9) << "write: (" << request_->logicpoolid() << ", "
                         << request_->copysetid()
                         << ") chunkid: " << request_->chunkid()
                         << " data size: " << request_->size()
                         << " error, read len :" << size << ", error: "
                         << strerror(errno);
            } else {
                LOG(FATAL) << "write: (" << request_->logicpoolid() << ", "
                           << request_->copysetid()
                           << ") chunkid: " << request_->chunkid()
                           << " data size: " << request_->size()
                           << ", error: " << strerror(errno);
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            dsRet = copysetNode->dataStore_->DeleteChunk(request_->chunkid());
            if (true == dsRet) {
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            } else {
                LOG(ERROR) << "delete: (" << request_->logicpoolid() << ", "
                           << request_->copysetid()
                           << ") chunkid: " << request_->chunkid()
                           << ", error: " << strerror(errno);
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            break;
        default:
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
            LOG(ERROR) << "Unknown chunk op, ";
            ret = -1;
    }

    return ret;
}

void ChunkOpContext::OnApply(std::shared_ptr<CopysetNode> copysetNode,
                             butil::IOBuf *log) {
    /* 反序列化 */
    uint32_t metaSize = 0;
    bool dsRet = true;
    log->cutn(&metaSize, sizeof(uint32_t));
    metaSize = butil::NetToHost32(metaSize);

    butil::IOBuf meta;
    log->cutn(&meta, metaSize);
    butil::IOBufAsZeroCopyInputStream wrapper(meta);
    ChunkRequest request;
    CHECK(request.ParseFromZeroCopyStream(&wrapper));
    butil::IOBuf data;
    data.swap(*log);

    switch (request.optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ:
            // read 直接忽略不处理
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            dsRet = copysetNode->dataStore_->WriteChunk(request.chunkid(),
                                                        data.to_string().c_str(),  //NOLINT
                                                        request.offset(),
                                                        request.size());
            if (false == dsRet) {
                LOG(FATAL) << "write: (" << request.logicpoolid() << ", "
                           << request.copysetid() << ") chunkid: "
                           << request.chunkid() << " data size: "
                           << request.size() << ", error: " << strerror(errno);
            }
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            /* 日志回放存在重复删除的可能性 */
            dsRet = copysetNode->dataStore_->DeleteChunk(request.chunkid());
            if (false == dsRet) {
                LOG(WARNING) << "delete: (" << request.logicpoolid() << ", "
                             << request.copysetid()
                             << ") chunkid: " << request.chunkid()
                             << ", error: "
                             << strerror(errno);
            }
            break;
        default:
            LOG(ERROR) << "Unknown chunk op";
            return;
    }
}

}  // namespace chunkserver
}  // namespace curve
