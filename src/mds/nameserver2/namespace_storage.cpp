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

std::string EncodeFileStoreKey(uint64_t parentID, const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), FILEINFOKEYPREFIX,  PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string EncodeSnapShotFileStoreKey(uint64_t parentID,
    const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), SNAPSHOTFILEINFOKEYPREFIX, PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string EncodeSegmentStoreKey(uint64_t inodeID, offset_t offset) {
    std::string storeKey;
    storeKey.resize(SEGMENTKEYLEN);
    memcpy(&(storeKey[0]), SEGMENTINFOKEYPREFIX,  PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), inodeID);
    ::curve::common::EncodeBigEndian(&(storeKey[10]), offset);
    return storeKey;
}

bool EncodeFileInfo(const FileInfo &fileInfo, std::string *out) {
    return fileInfo.SerializeToString(out);
}

bool DecodeFileInfo(const std::string info, FileInfo *fileInfo) {
    return fileInfo->ParseFromString(info);
}

bool EncodeSegment(const PageFileSegment &segment, std::string *out) {
    return segment.SerializeToString(out);
}

bool DecodeSegment(const std::string info, PageFileSegment *segment) {
    return segment->ParseFromString(info);
}

NameServerStorageImp::NameServerStorageImp(
    std::shared_ptr<StorageClient> client) {
    this->client_ = client;
}

StoreStatus NameServerStorageImp::PutFile(
    const std::string &storeKey, const FileInfo &fileInfo) {
    std::string encodeFileInfo;
    if (!EncodeFileInfo(fileInfo, &encodeFileInfo)) {
        LOG(ERROR) << "encode file: " << fileInfo.filename()<< "err";
        return StoreStatus::InternalError;
    }

    int errCode = client_->Put(storeKey, encodeFileInfo);
    if (errCode != EtcdErrCode::StatusOK) {
         LOG(ERROR) << "put file: [" << fileInfo.filename() << "] err: "
                    << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetFile(
    const std::string &storeKey, FileInfo *fileInfo) {
    std::string out;
    int errCode = client_->Get(storeKey, &out);

    if (errCode == EtcdErrCode::StatusOK) {
        bool decodeOK = DecodeFileInfo(out, fileInfo);
        if (decodeOK) {
            LOG(ERROR) << "decode info of key[" << storeKey << "] err";
            return StoreStatus::OK;
        } else {
            return StoreStatus::InternalError;
        }
    } else {
        LOG(ERROR) << "get file info of key[" << storeKey << "] err: "
                   << errCode;
    }

    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::DeleteFile(const std::string &storeKey) {
    int resCode = client_->Delete(storeKey);

    if (resCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "delete file of key: [" << storeKey << "] err: "
                   << resCode;
    }
    return getErrorCode(resCode);
}

StoreStatus NameServerStorageImp::RenameFile(
    const std::string &oldStoreKey, const FileInfo &oldFileInfo,
    const std::string &newStoreKey, const FileInfo &newFileInfo) {
    std::string encodeOldFileInfo;
    std::string encodeNewFileInfo;
    if (!EncodeFileInfo(oldFileInfo, &encodeOldFileInfo) ||
        !EncodeFileInfo(newFileInfo, &encodeNewFileInfo)) {
       LOG(ERROR) << "encode oldfile: " << oldFileInfo.fullpathname()
                  << " or newfile: " << newFileInfo.fullpathname() << "err";
       return StoreStatus::InternalError;
    }

    Operation op1{
        OpType::OpDelete,
        const_cast<char*>(oldStoreKey.c_str()),
        const_cast<char*>(encodeOldFileInfo.c_str()),
        oldStoreKey.size(), encodeOldFileInfo.size()};
    Operation op2{
        OpType::OpPut,
        const_cast<char*>(newStoreKey.c_str()),
        const_cast<char*>(encodeNewFileInfo.c_str()),
         newStoreKey.size(), encodeNewFileInfo.size()};

    int errCode = client_->Txn2(op1, op2);
    if (errCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "rename file from [" << oldFileInfo.fullpathname()
                   << "] to [" << newFileInfo.fullpathname() << "] err: "
                   << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::ListFile(
    const std::string &startStoreKey,
    const std::string &endStoreKey,
    std::vector<FileInfo> *files) {
    std::vector<std::string> out;
    int errCode = client_->List(
        startStoreKey.c_str(), endStoreKey.c_str(), &out);

    if (errCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "list file of [start:" << startStoreKey
                   << ", end:" << endStoreKey << "] err:" << errCode;
        return getErrorCode(errCode);
    }

    for (int i = 0; i < out.size(); i++) {
        FileInfo fileInfo;
        bool decodeOK = DecodeFileInfo(out[i], &fileInfo);
        if (decodeOK) {
            files->emplace_back(fileInfo);
        } else {
            LOG(ERROR) << "decode one fileInfo in [start:" << startStoreKey
                       << ", end:" << endStoreKey << ") err";
            return StoreStatus::InternalError;
        }
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::PutSegment(
    const std::string &storeKey, const PageFileSegment *segment) {
    std::string encodeSegment;
    if (!EncodeSegment(*segment, &encodeSegment)) {
        return StoreStatus::InternalError;
    }

    int errCode = client_->Put(storeKey, encodeSegment);
    if (errCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "put segment of (logicalPoolId:"
                   << segment->logicalpoolid() << "err:" << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::GetSegment(
    const std::string &storeKey, PageFileSegment *segment) {
    std::string out;
    int errCode = client_->Get(storeKey, &out);

    if (errCode == EtcdErrCode::StatusOK) {
        bool decodeOK = DecodeSegment(out, segment);
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

StoreStatus NameServerStorageImp::DeleteSegment(const std::string &storeKey) {
    int errCode = client_->Delete(storeKey);

    if (errCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "delete segment of storeKey["
                   <<storeKey << "] err:" << errCode;
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::SnapShotFile(
    const std::string &originFileKey,
    const FileInfo *originFileInfo,
    const std::string &snapshotFileKey,
    const FileInfo *snapshotFileInfo) {
    std::string encodeFileInfo;
    std::string encodeSnapshot;
    if (!EncodeFileInfo(*originFileInfo, &encodeFileInfo) ||
        !EncodeFileInfo(*snapshotFileInfo, &encodeSnapshot)) {
        LOG(ERROR) << "encode originfile:" << originFileInfo->fullpathname()
                   << " or snapshotfile:"  << snapshotFileInfo->fullpathname()
                   << " err";
        return StoreStatus::InternalError;
    }

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

    int errCode = client_->Txn2(op1, op2);
    if (errCode != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "store snapshot:" << snapshotFileInfo->fullpathname()
                   << ", fileinfo:" << originFileInfo->fullpathname() << "err";
    }
    return getErrorCode(errCode);
}

StoreStatus NameServerStorageImp::LoadSnapShotFile(
    std::vector<FileInfo> *snapshotFiles) {
    return ListFile(SNAPSHOTFILEINFOKEYPREFIX, "", snapshotFiles);
}

StoreStatus NameServerStorageImp::getErrorCode(int errCode) {
    switch (errCode) {
        case EtcdErrCode::StatusOK:
            return StoreStatus::OK;

        case EtcdErrCode::ErrNewEtcdClientV3:
        case EtcdErrCode::ErrEtcdPut:
        case EtcdErrCode::ErrEtcdGet:
        case EtcdErrCode::ErrEtcdDelete:
        case EtcdErrCode::ErrEtcdList:
        case EtcdErrCode::ErrObjectNotExist:
        case EtcdErrCode::ErrObjectType:
        case EtcdErrCode::ErrEtcdTxn:
        case EtcdErrCode::ErrEtcdTxnUnkownOp:
            return StoreStatus::InternalError;

        case EtcdErrCode::ErrEtcdGetNotExist:
        case EtcdErrCode::ErrEtcdListNotExist:
            return StoreStatus::KeyNotExist;

        default:
            return StoreStatus::InternalError;
    }
}
}  // namespace mds
}  // namespace curve





