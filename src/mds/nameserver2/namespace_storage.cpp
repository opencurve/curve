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
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 */

#include <glog/logging.h>
#include <utility>
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/namespace_define.h"

using ::curve::common::DISCARDSEGMENTKEYEND;
using ::curve::common::DISCARDSEGMENTKEYPREFIX;
using ::curve::common::SNAPSHOTFILEINFOKEYEND;
using ::curve::common::SNAPSHOTFILEINFOKEYPREFIX;

namespace curve {
namespace mds {

std::ostream &operator<<(std::ostream &os, StoreStatus &s) {
    os << static_cast<std::underlying_type<StoreStatus>::type>(s);
    return os;
}

NameServerStorageImp::NameServerStorageImp(
    std::shared_ptr<KVStorageClient> client, std::shared_ptr<Cache> cache, bool is_kvstorage )
    : cache_(cache), client_(client), discardMetric_(),is_kvstorage_(is_kvstorage) {}

NameServerStorageImp::NameServerStorageImp(
    std::shared_ptr<MysqlClientImp> mysqlclient, std::shared_ptr<Cache> cache, bool is_kvstorage )
    : cache_(cache), mysqlclient_(mysqlclient), discardMetric_(),is_kvstorage_(is_kvstorage) {}

StoreStatus NameServerStorageImp::PutFile(const FileInfo &fileInfo) {
    std::string storeKey;
    if (GetStoreKey(fileInfo.filetype(), fileInfo.parentid(),
                    fileInfo.filename(), &storeKey) != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << fileInfo.filename();
        return StoreStatus::InternalError;
    }
    std::string encodeFileInfo;
    if (!NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &encodeFileInfo)) {
        LOG(ERROR) << "encode file: " << fileInfo.filename() << "err";
        return StoreStatus::InternalError;
    }

    int errCode = EtcdErrCode::EtcdOK;
    if(is_kvstorage_)  errCode = client_->Put(storeKey, encodeFileInfo);
    else errCode = mysqlclient_->Put(storeKey, encodeFileInfo);

    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "put file: [" << fileInfo.filename()
                   << "] err: " << errCode;
    } else {
        // update to cache
        cache_->Put(storeKey, encodeFileInfo);
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetFile(InodeID parentid,
                                          const std::string &filename,
                                          FileInfo *fileInfo) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_PAGEFILE, parentid, filename, &storeKey) !=
        StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = " << filename;
        return StoreStatus::InternalError;
    }

    int errCode = EtcdErrCode::EtcdOK;
    std::string out;
    if (!cache_->Get(storeKey, &out)) {
        if(is_kvstorage_) errCode = client_->Get(storeKey, &out);
        else errCode = mysqlclient_->Get(storeKey, out);

        if (errCode == EtcdErrCode::EtcdOK) {
            cache_->Put(storeKey, out);
        }
    }

    if (errCode == EtcdErrCode::EtcdOK) {
        bool decodeOK = NameSpaceStorageCodec::DecodeFileInfo(out, fileInfo);
        if (decodeOK) {
            return StoreStatus::OK;
        } else {
            LOG(ERROR) << "decode info error. parentid: " << parentid
                       << ", filename: " << filename;
            return StoreStatus::InternalError;
        }
    } else if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        LOG(INFO) << "file not exist. parentid: " << parentid
                  << ", filename: " << filename;
    } else {
        LOG(ERROR) << "get file err: " << errCode << "."
                   << " parentid: " << parentid << ", filename: " << filename;
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::DeleteFile(InodeID id,
                                             const std::string &filename) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_PAGEFILE, id, filename, &storeKey) !=
        StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << filename;
        return StoreStatus::InternalError;
    }

    // delete cache first, then Etcd
    cache_->Remove(storeKey);
    int resCode = EtcdErrCode::EtcdOK;
    if(is_kvstorage_) resCode=client_->Delete(storeKey);
    else resCode=mysqlclient_->Delete(storeKey);
    if (resCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete file err: " << resCode << ","
                   << " inode id: " << id << ", filename: " << filename;
    }
    return getErrorCode(resCode);
}

StoreStatus
NameServerStorageImp::DeleteSnapshotFile(InodeID id,
                                         const std::string &filename) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE, id, filename,
                    &storeKey) != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = " << filename;
        return StoreStatus::InternalError;
    }

    // delete cache first, then Etcd
    cache_->Remove(storeKey);
    int resCode = EtcdErrCode::EtcdOK;
    if(is_kvstorage_) resCode=client_->Delete(storeKey);
    else resCode=mysqlclient_->Delete(storeKey);


    if (resCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete file err: " << resCode << "."
                   << " inodeid: " << id << ", filename: " << filename;
    }
    return getErrorCode(resCode);
}

StoreStatus NameServerStorageImp::RenameFile(const FileInfo &oldFInfo,
                                             const FileInfo &newFInfo) {
    std::string oldStoreKey;
    auto res = GetStoreKey(FileType::INODE_PAGEFILE, oldFInfo.parentid(),
                           oldFInfo.filename(), &oldStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << oldFInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string newStoreKey;
    res = GetStoreKey(FileType::INODE_PAGEFILE, newFInfo.parentid(),
                      newFInfo.filename(), &newStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << newFInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string encodeOldFileInfo;
    std::string encodeNewFileInfo;
    if (!NameSpaceStorageCodec::EncodeFileInfo(oldFInfo, &encodeOldFileInfo) ||
        !NameSpaceStorageCodec::EncodeFileInfo(newFInfo, &encodeNewFileInfo)) {
        LOG(ERROR) << "encode oldfile inodeid : " << oldFInfo.id()
                   << ", oldfile: " << oldFInfo.filename()
                   << " or newfile inodeid : " << newFInfo.id()
                   << ", newfile: " << newFInfo.filename() << "err";
        return StoreStatus::InternalError;
    }

    // delete the data in the cache first
    cache_->Remove(oldStoreKey);

    int errCode = EtcdErrCode::EtcdOK;
    if(is_kvstorage_){
    // update Etcd
    Operation op1{OpType::OpDelete, const_cast<char *>(oldStoreKey.c_str()), "",
                  static_cast<int>(oldStoreKey.size()), 0};
    Operation op2{OpType::OpPut, const_cast<char *>(newStoreKey.c_str()),
                  const_cast<char *>(encodeNewFileInfo.c_str()),
                  static_cast<int>(newStoreKey.size()),
                  static_cast<int>(encodeNewFileInfo.size())};
    std::vector<Operation> ops{op1, op2};
    errCode = client_->TxnN(ops);
    }
    else{
    // update mysql
    mysqlclient_->conn_->setAutoCommit(false);
    try
    {
        mysqlclient_->Delete(oldStoreKey);
        mysqlclient_->Put(newStoreKey, encodeNewFileInfo);
        mysqlclient_->conn_->commit();
        errCode=EtcdErrCode::EtcdOK;
    }
    catch(const std::exception& e)
    {
        mysqlclient_->conn_->rollback();
        LOG(ERROR) <<  "exception: " << e.what();
        errCode=-1;
    }
    mysqlclient_->conn_->setAutoCommit(true);
    }

    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "rename file from [" << oldFInfo.id() << ", "
                   << oldFInfo.filename() << "] to [" << newFInfo.id() << ", "
                   << newFInfo.filename() << "] err: " << errCode;
    } else {
        // update to cache at last
        cache_->Put(newStoreKey, encodeNewFileInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ReplaceFileAndRecycleOldFile(
    const FileInfo &oldFInfo, const FileInfo &newFInfo,
    const FileInfo &conflictFInfo, const FileInfo &recycleFInfo) {
    std::string oldStoreKey, newStoreKey, conflictStoreKey, recycleStoreKey;
    auto res = GetStoreKey(oldFInfo.filetype(), oldFInfo.parentid(),
                           oldFInfo.filename(), &oldStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << oldFInfo.filename();
        return StoreStatus::InternalError;
    }

    res = GetStoreKey(newFInfo.filetype(), newFInfo.parentid(),
                      newFInfo.filename(), &newStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << newFInfo.filename();
        return StoreStatus::InternalError;
    }

    res = GetStoreKey(conflictFInfo.filetype(), conflictFInfo.parentid(),
                      conflictFInfo.filename(), &conflictStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << conflictFInfo.filename();
        return StoreStatus::InternalError;
    }

    if (newStoreKey != conflictStoreKey) {
        LOG(ERROR) << "rename target[" << newFInfo.filename() << "] not exist";
        return StoreStatus::InternalError;
    }

    res = GetStoreKey(recycleFInfo.filetype(), recycleFInfo.parentid(),
                      recycleFInfo.filename(), &recycleStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << recycleFInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string encodeRecycleFInfo;
    std::string encodeNewFInfo;
    if (!NameSpaceStorageCodec::EncodeFileInfo(recycleFInfo,
                                               &encodeRecycleFInfo)) {
        LOG(ERROR) << "encode recycle file: " << recycleFInfo.filename()
                   << " err";
        return StoreStatus::InternalError;
    }

    if (!NameSpaceStorageCodec::EncodeFileInfo(newFInfo, &encodeNewFInfo)) {
        LOG(ERROR) << "encode recycle file: " << newFInfo.filename() << " err";
        return StoreStatus::InternalError;
    }

    // delete data in cache
    cache_->Remove(conflictStoreKey);
    cache_->Remove(oldStoreKey);

    // put recycleFInfo; delete oldFInfo; put newFInfo
    Operation op1{OpType::OpPut, const_cast<char *>(recycleStoreKey.c_str()),
                  const_cast<char *>(encodeRecycleFInfo.c_str()),
                  static_cast<int>(recycleStoreKey.size()),
                  static_cast<int>(encodeRecycleFInfo.size())};
    Operation op2{OpType::OpDelete, const_cast<char *>(oldStoreKey.c_str()), "",
                  static_cast<int>(oldStoreKey.size()), 0};
    Operation op3{OpType::OpPut, const_cast<char *>(newStoreKey.c_str()),
                  const_cast<char *>(encodeNewFInfo.c_str()),
                  static_cast<int>(newStoreKey.size()),
                  static_cast<int>(encodeNewFInfo.size())};

    std::vector<Operation> ops{op1, op2, op3};
    int errCode = 0;
    if(is_kvstorage_)errCode = client_->TxnN(ops);
    else{
        mysqlclient_->conn_->setAutoCommit(false);
        try
        {
            mysqlclient_->Put(recycleStoreKey, encodeRecycleFInfo);
            mysqlclient_->Delete(oldStoreKey);
            mysqlclient_->Put(newStoreKey, encodeNewFInfo);
            mysqlclient_->conn_->commit();
            errCode=EtcdErrCode::EtcdOK;
        }
        catch(const std::exception& e)
        {
            mysqlclient_->conn_->rollback();
            LOG(ERROR) <<  "exception: " << e.what();
            errCode=-1;
        }
        mysqlclient_->conn_->setAutoCommit(true);
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "rename file from [" << oldFInfo.filename() << "] to ["
                   << newFInfo.filename() << "] err: " << errCode;
    } else {
        // update to cache
        cache_->Put(recycleStoreKey, encodeRecycleFInfo);
        cache_->Put(newStoreKey, encodeNewFInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus
NameServerStorageImp::MoveFileToRecycle(const FileInfo &originFileInfo,
                                        const FileInfo &recycleFileInfo) {
    std::string originFileInfoKey;
    auto res = GetStoreKey(originFileInfo.filetype(), originFileInfo.parentid(),
                           originFileInfo.filename(), &originFileInfoKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << originFileInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string recycleFileInfoKey;
    res = GetStoreKey(recycleFileInfo.filetype(), recycleFileInfo.parentid(),
                      recycleFileInfo.filename(), &recycleFileInfoKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << recycleFileInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string encodeRecycleFInfo;
    if (!NameSpaceStorageCodec::EncodeFileInfo(recycleFileInfo,
                                               &encodeRecycleFInfo)) {
        LOG(ERROR) << "encode recycle file: " << recycleFileInfo.filename()
                   << " err";
        return StoreStatus::InternalError;
    }

    // delete data in cache
    cache_->Remove(originFileInfoKey);

    // remove originFileInfo from Etcd, and put recycleFileInfo
    Operation op1{OpType::OpDelete,
                  const_cast<char *>(originFileInfoKey.c_str()), "",
                  static_cast<int>(originFileInfoKey.size()), 0};
    Operation op2{OpType::OpPut, const_cast<char *>(recycleFileInfoKey.c_str()),
                  const_cast<char *>(encodeRecycleFInfo.c_str()),
                  static_cast<int>(recycleFileInfoKey.size()),
                  static_cast<int>(encodeRecycleFInfo.size())};

    std::vector<Operation> ops{op1, op2};
    int errCode = 0;
    if(is_kvstorage_) errCode = client_->TxnN(ops);
    else{
        mysqlclient_->conn_->setAutoCommit(false);
        try
        {
            mysqlclient_->Delete(originFileInfoKey);
            mysqlclient_->Put(recycleFileInfoKey, encodeRecycleFInfo);
            mysqlclient_->conn_->commit();
            errCode=EtcdErrCode::EtcdOK;
        }
        catch(const std::exception& e)
        {
            mysqlclient_->conn_->rollback();
            LOG(ERROR) <<  "exception: " << e.what();
            errCode=-1;
        }
        mysqlclient_->conn_->setAutoCommit(true);
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "move file [" << originFileInfo.filename()
                   << "] to recycle file [" << recycleFileInfo.filename()
                   << "] err: " << errCode;
    } else {
        // update to cache
        cache_->Put(recycleFileInfoKey, encodeRecycleFInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ListFile(InodeID startid, InodeID endid,
                                           std::vector<FileInfo> *files) {
    std::string startStoreKey;
    auto res =
        GetStoreKey(FileType::INODE_PAGEFILE, startid, "", &startStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << startid;
        return StoreStatus::InternalError;
    }

    std::string endStoreKey;
    res = GetStoreKey(FileType::INODE_PAGEFILE, endid, "", &endStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << endid;
        return StoreStatus::InternalError;
    }

    return ListFileInternal(startStoreKey, endStoreKey, files);
}

StoreStatus
NameServerStorageImp::ListSegment(InodeID id,
                                  std::vector<PageFileSegment> *segments) {
    std::string startStoreKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, 0);
    std::string endStoreKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id + 1, 0);

    std::vector<std::string> out;
    int errCode = client_->List(startStoreKey, endStoreKey, &out);

    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "list segment err:" << errCode;
        return getErrorCode(errCode);
    }

    for (size_t i = 0; i < out.size(); i++) {
        PageFileSegment segment;
        bool decodeOK = NameSpaceStorageCodec::DecodeSegment(out[i], &segment);
        if (decodeOK) {
            segments->emplace_back(segment);
        } else {
            LOG(ERROR) << "decode one segment err";
            return StoreStatus::InternalError;
        }
    }
    return StoreStatus::OK;
}

StoreStatus
NameServerStorageImp::ListSnapshotFile(InodeID startid, InodeID endid,
                                       std::vector<FileInfo> *files) {
    std::string startStoreKey;
    auto res = GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE, startid, "",
                           &startStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << startid;
        return StoreStatus::InternalError;
    }

    std::string endStoreKey;
    res =
        GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE, endid, "", &endStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << endid;
        return StoreStatus::InternalError;
    }

    return ListFileInternal(startStoreKey, endStoreKey, files);
}

StoreStatus
NameServerStorageImp::ListFileInternal(const std::string &startStoreKey,
                                       const std::string &endStoreKey,
                                       std::vector<FileInfo> *files) {
    std::vector<std::string> out;
    int errCode = client_->List(startStoreKey, endStoreKey, &out);

    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "list file err:" << errCode;
        return getErrorCode(errCode);
    }

    for (size_t i = 0; i < out.size(); i++) {
        FileInfo fileInfo;
        bool decodeOK =
            NameSpaceStorageCodec::DecodeFileInfo(out[i], &fileInfo);
        if (decodeOK) {
            files->emplace_back(fileInfo);
        } else {
            LOG(ERROR) << "decode one fileInfo err";
            return StoreStatus::InternalError;
        }
    }
    return StoreStatus::OK;
}

StoreStatus NameServerStorageImp::PutSegment(InodeID id, uint64_t off,
                                             const PageFileSegment *segment,
                                             int64_t *revision) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    std::string encodeSegment;
    if (!NameSpaceStorageCodec::EncodeSegment(*segment, &encodeSegment)) {
        return StoreStatus::InternalError;
    }

    int errCode = client_->PutRewithRevision(storeKey, encodeSegment, revision);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "put segment of logicalPoolId:"
                   << segment->logicalpoolid() << "err:" << errCode;
    } else {
        cache_->Put(storeKey, encodeSegment);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetSegment(InodeID id, uint64_t off,
                                             PageFileSegment *segment) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    int errCode = EtcdErrCode::EtcdOK;
    std::string out;
    if (!cache_->Get(storeKey, &out)) {
        errCode = client_->Get(storeKey, &out);
    }

    if (errCode == EtcdErrCode::EtcdOK) {
        bool decodeOK = NameSpaceStorageCodec::DecodeSegment(out, segment);
        if (decodeOK) {
            return StoreStatus::OK;
        } else {
            LOG(ERROR) << "decode segment inodeid: " << id << ", off: " << off
                       << " err";
            return StoreStatus::InternalError;
        }
    } else if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        LOG(INFO) << "segment not exist. inodeid: " << id << ", off: " << off;
    } else {
        LOG(ERROR) << "get segment inodeid: " << id << ", off: " << off
                   << " err: " << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::DeleteSegment(InodeID id, uint64_t off,
                                                int64_t *revision) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    int errCode = client_->DeleteRewithRevision(storeKey, revision);

    // update the cache first, then update Etcd
    cache_->Remove(storeKey);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "delete segment of inodeid: " << id << "off: " << off
                   << ", err:" << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ListDiscardSegment(
    std::map<std::string, DiscardSegmentInfo> *discardSegments) {
    assert(discardSegments != nullptr);

    std::vector<std::pair<std::string, std::string>> out;
    int err =
        client_->List(DISCARDSEGMENTKEYPREFIX, DISCARDSEGMENTKEYEND, &out);
    if (err != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "ListDiscardSegment return error, err = " << err;
        return StoreStatus::InternalError;
    }

    for (const auto &kv : out) {
        DiscardSegmentInfo info;
        if (!NameSpaceStorageCodec::DecodeDiscardSegment(kv.second, &info)) {
            LOG(ERROR) << "Decode DiscardSegment failed";
            return StoreStatus::InternalError;
        }

        discardSegments->emplace(kv.first, std::move(info));
    }

    return StoreStatus::OK;
}

StoreStatus
NameServerStorageImp::DiscardSegment(const FileInfo &fileInfo,
                                     const PageFileSegment &segment) {
    const uint64_t inodeId = fileInfo.id();
    const uint64_t offset = segment.startoffset();
    const std::string segmentKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(inodeId, offset);
    const std::string cleanSegmentKey =
        NameSpaceStorageCodec::EncodeDiscardSegmentStoreKey(inodeId, offset);

    std::string encodeSegment;
    if (!NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment)) {
        return StoreStatus::InternalError;
    }

    std::string encodeDiscardSegment;
    DiscardSegmentInfo discardInfo;
    discardInfo.set_allocated_fileinfo(new FileInfo(fileInfo));
    discardInfo.set_allocated_pagefilesegment(new PageFileSegment(segment));
    if (!NameSpaceStorageCodec::EncodeDiscardSegment(discardInfo,
                                                     &encodeDiscardSegment)) {
        return StoreStatus::InternalError;
    }

    Operation op1{OpType::OpDelete, const_cast<char *>(segmentKey.c_str()),
                  const_cast<char *>(encodeSegment.c_str()),
                  static_cast<int>(segmentKey.size()),
                  static_cast<int>(encodeSegment.size())};
    Operation op2{OpType::OpPut, const_cast<char *>(cleanSegmentKey.c_str()),
                  const_cast<char *>(encodeDiscardSegment.c_str()),
                  static_cast<int>(cleanSegmentKey.size()),
                  static_cast<int>(encodeDiscardSegment.size())};

    std::vector<Operation> ops{op1, op2};
    auto errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Discard segment failed, filename: "
                   << fileInfo.filename() << ", inodeid = " << inodeId
                   << ", offset: " << offset << ", errCode: " << errCode;
    } else {
        cache_->Remove(segmentKey);
        discardMetric_.OnReceiveDiscardRequest(segment.segmentsize());
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::CleanDiscardSegment(uint64_t segmentSize,
                                                      const std::string &key,
                                                      int64_t *revision) {
    int errCode = client_->DeleteRewithRevision(key, revision);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "CleanDiscardSegment failed, key = " << key
                   << ", err = " << errCode;
    } else {
        discardMetric_.OnDiscardFinish(segmentSize);
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::SnapShotFile(const FileInfo *originFInfo,
                                               const FileInfo *snapshotFInfo) {
    std::string originFileKey;
    auto res = GetStoreKey(originFInfo->filetype(), originFInfo->parentid(),
                           originFInfo->filename(), &originFileKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << originFInfo->filename();
        return StoreStatus::InternalError;
    }

    std::string snapshotFileKey;
    res = GetStoreKey(snapshotFInfo->filetype(), snapshotFInfo->parentid(),
                      snapshotFInfo->filename(), &snapshotFileKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << snapshotFInfo->filename();
        return StoreStatus::InternalError;
    }

    std::string encodeFileInfo;
    std::string encodeSnapshot;
    if (!NameSpaceStorageCodec::EncodeFileInfo(*originFInfo, &encodeFileInfo) ||
        !NameSpaceStorageCodec::EncodeFileInfo(*snapshotFInfo,
                                               &encodeSnapshot)) {
        LOG(ERROR) << "encode originfile inodeid: " << originFInfo->id()
                   << ", originfile: " << originFInfo->filename()
                   << " or snapshotfile inodeid: " << snapshotFInfo->id()
                   << ", snapshotfile: " << snapshotFInfo->filename() << "err";
        return StoreStatus::InternalError;
    }

    // delete the information in cache first
    cache_->Remove(originFileKey);

    // then update Etcd
    Operation op1{OpType::OpPut, const_cast<char *>(originFileKey.c_str()),
                  const_cast<char *>(encodeFileInfo.c_str()),
                  static_cast<int>(originFileKey.size()),
                  static_cast<int>(encodeFileInfo.size())};
    Operation op2{OpType::OpPut, const_cast<char *>(snapshotFileKey.c_str()),
                  const_cast<char *>(encodeSnapshot.c_str()),
                  static_cast<int>(snapshotFileKey.size()),
                  static_cast<int>(encodeSnapshot.size())};

    std::vector<Operation> ops{op1, op2};
    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "store snapshot inodeid: " << snapshotFInfo->id()
                   << ", snapshot: " << snapshotFInfo->filename()
                   << ", fileinfo inodeid: " << originFInfo->id()
                   << ", fileinfo: " << originFInfo->filename() << "err";
    } else {
        // update cache at last
        cache_->Put(originFileKey, encodeFileInfo);
        cache_->Put(snapshotFileKey, encodeSnapshot);
    }
    return getErrorCode(errCode);
}

StoreStatus
NameServerStorageImp::LoadSnapShotFile(std::vector<FileInfo> *snapshotFiles) {
    return ListFileInternal(SNAPSHOTFILEINFOKEYPREFIX, SNAPSHOTFILEINFOKEYEND,
                            snapshotFiles);
}

StoreStatus NameServerStorageImp::getErrorCode(int errCode) {
    switch (errCode) {
    case EtcdErrCode::EtcdOK:
        return StoreStatus::OK;

    case EtcdErrCode::EtcdKeyNotExist:
        return StoreStatus::KeyNotExist;

    case EtcdErrCode::EtcdUnknown:
    case EtcdErrCode::EtcdInvalidArgument:
    case EtcdErrCode::EtcdAlreadyExists:
    case EtcdErrCode::EtcdPermissionDenied:
    case EtcdErrCode::EtcdOutOfRange:
    case EtcdErrCode::EtcdUnimplemented:
    case EtcdErrCode::EtcdInternal:
    case EtcdErrCode::EtcdNotFound:
    case EtcdErrCode::EtcdDataLoss:
    case EtcdErrCode::EtcdUnauthenticated:
    case EtcdErrCode::EtcdCanceled:
    case EtcdErrCode::EtcdDeadlineExceeded:
    case EtcdErrCode::EtcdResourceExhausted:
    case EtcdErrCode::EtcdFailedPrecondition:
    case EtcdErrCode::EtcdAborted:
    case EtcdErrCode::EtcdUnavailable:
    case EtcdErrCode::EtcdTxnUnkownOp:
    case EtcdErrCode::EtcdObjectNotExist:
    case EtcdErrCode::EtcdErrObjectType:
        return StoreStatus::InternalError;

    default:
        return StoreStatus::InternalError;
    }
}

StoreStatus NameServerStorageImp::GetStoreKey(FileType filetype, InodeID id,
                                              const std::string &filename,
                                              std::string *storeKey) {
    switch (filetype) {
    case FileType::INODE_PAGEFILE:
    case FileType::INODE_DIRECTORY:
        *storeKey = NameSpaceStorageCodec::EncodeFileStoreKey(id, filename);
        break;
    case FileType::INODE_SNAPSHOT_PAGEFILE:
        *storeKey =
            NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(id, filename);
        break;
    default:
        LOG(ERROR) << "filetype: " << filetype << " of " << filename
                   << " not exist";
        return StoreStatus::InternalError;
    }
    return StoreStatus::OK;
}

}  // namespace mds
}  // namespace curve
