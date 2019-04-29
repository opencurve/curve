/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/nameserver2/namespace_storage.h"

namespace curve {
namespace mds {

std::ostream& operator << (std::ostream & os, StoreStatus &s) {
    os << static_cast<std::underlying_type<StoreStatus>::type>(s);
    return os;
}

NameServerStorageImp::NameServerStorageImp(
    std::shared_ptr<StorageClient> client, std::shared_ptr<Cache> cache) {
    this->client_ = client;
    this->cache_ = cache;
}

StoreStatus NameServerStorageImp::PutFile(const FileInfo &fileInfo) {
    std::string storeKey;
    if (GetStoreKey(fileInfo.filetype(),
                    fileInfo.parentid(),
                    fileInfo.filename(),
                    &storeKey)
            != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << fileInfo.filename();
        return StoreStatus::InternalError;
    }
    std::string encodeFileInfo;
    if (!NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &encodeFileInfo)) {
        LOG(ERROR) << "encode file: " << fileInfo.filename()<< "err";
        return StoreStatus::InternalError;
    }

    int errCode = client_->Put(storeKey, encodeFileInfo);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "put file: [" << fileInfo.filename() << "] err: "
                    << errCode;
    } else {
        // 更新到缓存
        cache_->Put(storeKey, encodeFileInfo);
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetFile(InodeID parentid,
                                          const std::string &filename,
                                          FileInfo *fileInfo) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_PAGEFILE, parentid, filename, &storeKey)
        != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = " << filename;
        return StoreStatus::InternalError;
    }

    int errCode = EtcdErrCode::OK;
    std::string out;
    if (!cache_->Get(storeKey, &out)) {
        errCode = client_->Get(storeKey, &out);
    }

    if (errCode == EtcdErrCode::OK) {
        bool decodeOK = NameSpaceStorageCodec::DecodeFileInfo(out, fileInfo);
        if (decodeOK) {
            return StoreStatus::OK;
        } else {
            LOG(ERROR) << "decode info of key[" << storeKey << "] err"
                       << ", fileinfo: " << fileInfo->DebugString();
            return StoreStatus::InternalError;
        }
    } else {
        LOG(ERROR) << "get file info of key[" << storeKey << "] err: "
                   << errCode;
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::DeleteFile(InodeID id,
                                            const std::string &filename) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_PAGEFILE, id, filename, &storeKey)
        != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << filename;
        return StoreStatus::InternalError;
    }

    // 先删缓存，再删etcd
    cache_->Remove(storeKey);
    int resCode = client_->Delete(storeKey);

    if (resCode != EtcdErrCode::OK) {
        LOG(ERROR) << "delete file of key: [" << storeKey << "] err: "
                   << resCode;
    }
    return getErrorCode(resCode);
}

StoreStatus NameServerStorageImp::DeleteRecycleFile(InodeID id,
                            const std::string &filename) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_RECYCLE_PAGEFILE, id, filename, &storeKey)
        != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << filename;
        return StoreStatus::InternalError;
    }

    // 先删缓存，再删etcd
    cache_->Remove(storeKey);
    int resCode = client_->Delete(storeKey);

    if (resCode != EtcdErrCode::OK) {
        LOG(ERROR) << "delete file of key: [" << storeKey << "] err: "
                   << resCode;
    }
    return getErrorCode(resCode);
}

StoreStatus NameServerStorageImp::DeleteSnapshotFile(InodeID id,
                                                const std::string &filename) {
    std::string storeKey;
    if (GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE,
                    id, filename, &storeKey) != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed,filename = " << filename;
        return StoreStatus::InternalError;
    }

    // 先删缓存，再删etcd
    cache_->Remove(storeKey);
    int resCode = client_->Delete(storeKey);

    if (resCode != EtcdErrCode::OK) {
        LOG(ERROR) << "delete file of key: [" << storeKey << "] err: "
                   << resCode;
    }
    return getErrorCode(resCode);
}

StoreStatus NameServerStorageImp::RenameFile(const FileInfo &oldFInfo,
                                            const FileInfo &newFInfo) {
    std::string oldStoreKey;
    auto res = GetStoreKey(FileType::INODE_PAGEFILE,
                    oldFInfo.parentid(),
                    oldFInfo.filename(),
                    &oldStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << oldFInfo.filename();
        return StoreStatus::InternalError;
    }

    std::string newStoreKey;
    res = GetStoreKey(FileType::INODE_PAGEFILE,
                    newFInfo.parentid(),
                    newFInfo.filename(),
                    &newStoreKey);
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

    // 先删除缓存中的数据
    cache_->Remove(oldStoreKey);

    // 更新etcd
    Operation op1{
        OpType::OpDelete,
        const_cast<char*>(oldStoreKey.c_str()), "",
        oldStoreKey.size(), 0};
    Operation op2{
        OpType::OpPut,
        const_cast<char*>(newStoreKey.c_str()),
        const_cast<char*>(encodeNewFileInfo.c_str()),
         newStoreKey.size(), encodeNewFileInfo.size()};
    std::vector<Operation> ops{op1, op2};
    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "rename file from [" << oldFInfo.id() << ", "
                   << oldFInfo.filename() << "] to [" << newFInfo.id()
                   << ", " << newFInfo.filename() << "] err: "
                   << errCode;
    } else {
        // 最后更新到缓存
        cache_->Put(newStoreKey, encodeNewFileInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ReplaceFileAndRecycleOldFile(
                                            const FileInfo &oldFInfo,
                                            const FileInfo &newFInfo,
                                            const FileInfo &conflictFInfo,
                                            const FileInfo &recycleFInfo) {
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
    if (!NameSpaceStorageCodec::EncodeFileInfo(
        recycleFInfo, &encodeRecycleFInfo)) {
        LOG(ERROR) << "encode recycle file: " << recycleFInfo.filename()
                  << " err";
        return StoreStatus::InternalError;
    }

    if (!NameSpaceStorageCodec::EncodeFileInfo(newFInfo, &encodeNewFInfo)) {
        LOG(ERROR) << "encode recycle file: " << newFInfo.filename()
                  << " err";
        return StoreStatus::InternalError;
    }

    // 删除缓存中的数据
    cache_->Remove(conflictStoreKey);
    cache_->Remove(oldStoreKey);

    // put recycleFInfo; delete oldFInfo; put newFInfo
    Operation op1{
        OpType::OpPut,
        const_cast<char*>(recycleStoreKey.c_str()),
        const_cast<char*>(encodeRecycleFInfo.c_str()),
        recycleStoreKey.size(), encodeRecycleFInfo.size()};
    Operation op2{
        OpType::OpDelete,
        const_cast<char*>(oldStoreKey.c_str()), "",
        oldStoreKey.size(), 0};
    Operation op3{
        OpType::OpPut,
        const_cast<char*>(newStoreKey.c_str()),
        const_cast<char*>(encodeNewFInfo.c_str()),
        newStoreKey.size(), encodeNewFInfo.size()};

    std::vector<Operation> ops{op1, op2, op3};
    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "rename file from [" << oldFInfo.filename()
                   << "] to [" << newFInfo.filename() << "] err: "
                   << errCode;
    } else {
        //更新到缓存
        cache_->Put(recycleStoreKey, encodeRecycleFInfo);
        cache_->Put(newStoreKey, encodeNewFInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::MoveFileToRecycle(
    const FileInfo &originFileInfo, const FileInfo &recycleFileInfo) {
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
    if (!NameSpaceStorageCodec::EncodeFileInfo(
        recycleFileInfo, &encodeRecycleFInfo)) {
        LOG(ERROR) << "encode recycle file: " << recycleFileInfo.filename()
                  << " err";
        return StoreStatus::InternalError;
    }

    // 删除缓存中的数据
    cache_->Remove(originFileInfoKey);

    // 从etcd中删除originFileInfo, put recycleFileInfo
    Operation op1{
        OpType::OpDelete,
        const_cast<char*>(originFileInfoKey.c_str()), "",
        originFileInfoKey.size(), 0};
    Operation op2{
        OpType::OpPut,
        const_cast<char*>(recycleFileInfoKey.c_str()),
        const_cast<char*>(encodeRecycleFInfo.c_str()),
        recycleFileInfoKey.size(), encodeRecycleFInfo.size()};

    std::vector<Operation> ops{op1, op2};
    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "move file [" << originFileInfo.filename()
                   << "] to recycle file ["
                   << recycleFileInfo.filename() << "] err: "
                   << errCode;
    } else {
        //更新到缓存
        cache_->Put(recycleFileInfoKey, encodeRecycleFInfo);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ListFile(InodeID startid,
                                           InodeID endid,
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

StoreStatus NameServerStorageImp::ListSnapshotFile(InodeID startid,
                                           InodeID endid,
                                           std::vector<FileInfo> *files) {
    std::string startStoreKey;
    auto res = GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE,
                    startid, "", &startStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << startid;
        return StoreStatus::InternalError;
    }

    std::string endStoreKey;
    res = GetStoreKey(FileType::INODE_SNAPSHOT_PAGEFILE,
                    endid, "", &endStoreKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, id = " << endid;
        return StoreStatus::InternalError;
    }

    return ListFileInternal(startStoreKey, endStoreKey, files);
}

StoreStatus NameServerStorageImp::ListFileInternal(
                                           const std::string& startStoreKey,
                                           const std::string& endStoreKey,
                                           std::vector<FileInfo> *files) {
    std::vector<std::string> out;
    int errCode = client_->List(
        startStoreKey, endStoreKey, &out);

    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "list file of [start:" << startStoreKey
                   << ", end:" << endStoreKey << "] err:" << errCode;
        return getErrorCode(errCode);
    }

    for (int i = 0; i < out.size(); i++) {
        FileInfo fileInfo;
        bool decodeOK = NameSpaceStorageCodec::DecodeFileInfo(out[i],
                                                              &fileInfo);
        if (decodeOK) {
            files->emplace_back(fileInfo);
        } else {
            LOG(ERROR) << "decode one fileInfo in [start:" << startStoreKey
                       << ", end:" << endStoreKey << ") err";
            return StoreStatus::InternalError;
        }
    }
    return StoreStatus::OK;
}

StoreStatus NameServerStorageImp::PutSegment(InodeID id,
                                             uint64_t off,
                                             const PageFileSegment *segment) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    std::string encodeSegment;
    if (!NameSpaceStorageCodec::EncodeSegment(*segment, &encodeSegment)) {
        return StoreStatus::InternalError;
    }

    int errCode = client_->Put(storeKey, encodeSegment);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "put segment of (logicalPoolId:"
                   << segment->logicalpoolid() << "err:" << errCode;
    } else {
        cache_->Put(storeKey, encodeSegment);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetSegment(InodeID id,
                                             uint64_t off,
                                             PageFileSegment *segment) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    int errCode = EtcdErrCode::OK;
    std::string out;
    if (!cache_->Get(storeKey, &out)) {
        errCode = client_->Get(storeKey, &out);
    }

    if (errCode == EtcdErrCode::OK) {
        bool decodeOK = NameSpaceStorageCodec::DecodeSegment(out, segment);
        if (decodeOK) {
            return StoreStatus::OK;
        } else {
            LOG(ERROR) << "decode segment[" << storeKey <<"] err";
            return StoreStatus::InternalError;
        }
    } else {
        LOG(ERROR) << "get segment[" << storeKey <<"] err: " << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::DeleteSegment(InodeID id, uint64_t off) {
    std::string storeKey =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(id, off);
    int errCode = client_->Delete(storeKey);

    // 先更新缓存，再更新etcd
    cache_->Remove(storeKey);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "delete segment of storeKey["
                   <<storeKey << "] err:" << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::SnapShotFile(const FileInfo *originFInfo,
                                            const FileInfo *snapshotFInfo) {
    std::string originFileKey;
    auto res = GetStoreKey(originFInfo->filetype(),
                    originFInfo->parentid(),
                    originFInfo->filename(),
                    &originFileKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << originFInfo->filename();
        return StoreStatus::InternalError;
    }

    std::string snapshotFileKey;
    res = GetStoreKey(snapshotFInfo->filetype(),
                    snapshotFInfo->parentid(),
                    snapshotFInfo->filename(),
                    &snapshotFileKey);
    if (res != StoreStatus::OK) {
        LOG(ERROR) << "get store key failed, filename = "
                   << snapshotFInfo->filename();
        return StoreStatus::InternalError;
    }

    std::string encodeFileInfo;
    std::string encodeSnapshot;
    if (!NameSpaceStorageCodec::EncodeFileInfo(*originFInfo, &encodeFileInfo) ||
    !NameSpaceStorageCodec::EncodeFileInfo(*snapshotFInfo, &encodeSnapshot)) {
        LOG(ERROR) << "encode originfile inodeid : " << originFInfo->id()
                   << ", originfile: " << originFInfo->filename()
                   << " or snapshotfile inodeid : " << snapshotFInfo->id()
                   << ", snapshotfile: " << snapshotFInfo->filename() << "err";
        return StoreStatus::InternalError;
    }

    // 先删除缓存中的信息
    cache_->Remove(originFileKey);

    // 再更新etcd
    Operation op1{
        OpType::OpPut,
        const_cast<char*>(originFileKey.c_str()),
        const_cast<char*>(encodeFileInfo.c_str()),
        originFileKey.size(), encodeFileInfo.size()};
    Operation op2{
        OpType::OpPut,
        const_cast<char*>(snapshotFileKey.c_str()),
        const_cast<char*>(encodeSnapshot.c_str()),
        snapshotFileKey.size(), encodeSnapshot.size()};

    std::vector<Operation> ops{op1, op2};
    int errCode = client_->TxnN(ops);
    if (errCode != EtcdErrCode::OK) {
        LOG(ERROR) << "store snapshot inodeid : " << snapshotFInfo->id()
                   << ", snapshot: " << snapshotFInfo->filename()
                   << ", fileinfo inodeid : " << originFInfo->id()
                   << ", fileinfo: " << originFInfo->filename() << "err";
    } else {
        // 最后put到缓存中
        cache_->Put(originFileKey, encodeFileInfo);
        cache_->Put(snapshotFileKey, encodeSnapshot);
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::LoadSnapShotFile(
    std::vector<FileInfo> *snapshotFiles) {
    return ListFileInternal(SNAPSHOTFILEINFOKEYPREFIX,
                            SNAPSHOTFILEINFOKEYEND, snapshotFiles);
}

StoreStatus NameServerStorageImp::LoadRecycleFile(
    std::vector<FileInfo> *recycleFiles) {
    return ListFileInternal(RECYCLEFILEINFOKEYPREFIX,
                            RECYCLEFILEINFOKEYEND, recycleFiles);
}

StoreStatus NameServerStorageImp::getErrorCode(int errCode) {
    switch (errCode) {
        case EtcdErrCode::OK:
            return StoreStatus::OK;

        case EtcdErrCode::KeyNotExist:
            return StoreStatus::KeyNotExist;

        case EtcdErrCode::Unknown:
        case EtcdErrCode::InvalidArgument:
        case EtcdErrCode::AlreadyExists:
        case EtcdErrCode::PermissionDenied:
        case EtcdErrCode::OutOfRange:
        case EtcdErrCode::Unimplemented:
        case EtcdErrCode::Internal:
        case EtcdErrCode::NotFound:
        case EtcdErrCode::DataLoss:
        case EtcdErrCode::Unauthenticated:
        case EtcdErrCode::Canceled:
        case EtcdErrCode::DeadlineExceeded:
        case EtcdErrCode::ResourceExhausted:
        case EtcdErrCode::FailedPrecondition:
        case EtcdErrCode::Aborted:
        case EtcdErrCode::Unavailable:
        case EtcdErrCode::TxnUnkownOp:
        case EtcdErrCode::ObjectNotExist:
        case EtcdErrCode::ErrObjectType:
            return StoreStatus::InternalError;

        default:
            return StoreStatus::InternalError;
    }
}

StoreStatus NameServerStorageImp::GetStoreKey(FileType filetype,
                                              InodeID id,
                                              const std::string& filename,
                                              std::string* storeKey) {
    switch (filetype) {
        case FileType::INODE_PAGEFILE:
        case FileType::INODE_DIRECTORY:
            *storeKey = NameSpaceStorageCodec::EncodeFileStoreKey(id, filename);
            break;
        case FileType::INODE_SNAPSHOT_PAGEFILE:
            *storeKey =
                NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(id, filename);
            break;
        case FileType::INODE_RECYCLE_PAGEFILE:
            *storeKey = NameSpaceStorageCodec::EncodeRecycleFileStoreKey(id,
                                                                    filename);
            break;
        default:
            LOG(ERROR) << "filetype: "
                       << filetype << " of " << filename << " not exist";
            return StoreStatus::InternalError;
    }
    return StoreStatus::OK;
}

}  // namespace mds
}  // namespace curve
