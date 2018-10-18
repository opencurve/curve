/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/nameserver2/namespace_storage.h"


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

std::string EncodeSegmentStoreKey(uint64_t inodeID, offset_t offset) {
    std::string storeKey;
    storeKey.resize(18);
    memcpy(&(storeKey[0]), SEGMENTINFOKEYPREFIX,  PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), inodeID);
    ::curve::common::EncodeBigEndian(&(storeKey[10]), offset);
    return storeKey;
}


}  // namespace mds
}  // namespace curve


