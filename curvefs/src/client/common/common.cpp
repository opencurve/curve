/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Sept 3 2021
 * Author: lixiaocui
 */

#include "curvefs/src/client/common/common.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace client {
namespace common {

std::ostream &operator<<(std::ostream &os, MetaServerOpType optype) {
    switch (optype) {
    case MetaServerOpType::GetDentry:
        os << "GetDentry";
        break;
    case MetaServerOpType::ListDentry:
        os << "ListDentry";
        break;
    case MetaServerOpType::CreateDentry:
        os << "CreateDentry";
        break;
    case MetaServerOpType::DeleteDentry:
        os << "DeleteDentry";
        break;
    case MetaServerOpType::PrepareRenameTx:
        os << "PrepareRenameTx";
        break;
    case MetaServerOpType::GetInode:
        os << "GetInode";
        break;
    case MetaServerOpType::BatchGetInodeAttr:
        os << "BatchGetInodeAttr";
        break;
    case MetaServerOpType::BatchGetXAttr:
        os << "BatchGetXAttr";
        break;
    case MetaServerOpType::UpdateInode:
        os << "UpdateInode";
        break;
    case MetaServerOpType::CreateInode:
        os << "CreateInode";
        break;
    case MetaServerOpType::DeleteInode:
        os << "DeleteInode";
        break;
    case MetaServerOpType::GetOrModifyS3ChunkInfo:
        os << "GetOrModifyS3ChunkInfo";
        break;
    case MetaServerOpType::GetVolumeExtent:
        os << "GetVolumeExtent";
        break;
    case MetaServerOpType::UpdateVolumeExtent:
        os << "UpdateVolumeExtent";
        break;
    default:
        os << "Unknow opType";
    }
    return os;
}

const char kCurveFsWarmupOpAdd[] = "add";
const char kCurveFsWarmupOpCancel[] = "cancel";
const char kCurveFsWarmupOpCheck[] = "check";
const char kCurveFsWarmupTypeList[] = "list";
const char kCurveFsWarmupTypeSingle[] = "single";

WarmupOpType GetWarmupOpType(const std::string& op) {
    auto ret = WarmupOpType::kWarmupOpUnknown;
    if (op == kCurveFsWarmupOpAdd) {
        ret = WarmupOpType::kWarmupOpAdd;
    } else if (op == kCurveFsWarmupOpCancel) {
        ret = WarmupOpType::kWarmupOpCancel;
    } else if (op == kCurveFsWarmupOpCheck) {
        ret = WarmupOpType::kWarmupOpCheck;
    }
    return ret;
}

WarmupType GetWarmupType(const std::string& type) {
    auto ret =
        WarmupType::kWarmupTypeUnknown;
    if (type == kCurveFsWarmupTypeList) {
        ret = WarmupType::kWarmupTypeList;
    } else if (type == kCurveFsWarmupTypeSingle) {
        ret = WarmupType::kWarmupTypeSingle;
    }
    return ret;
}

const char kCurveFsWarmupStorageDisk[] = "disk";
const char kCurveFsWarmupStorageKvclient[] = "kvclient";

WarmupStorageType GetWarmupStorageType(const std::string &type) {
    auto ret = WarmupStorageType::kWarmupStorageTypeUnknown;
    if (type == kCurveFsWarmupStorageDisk) {
        ret = WarmupStorageType::kWarmupStorageTypeDisk;
    } else if (type == kCurveFsWarmupStorageKvclient) {
        ret = WarmupStorageType::kWarmupStorageTypeKvClient;
    }
    return ret;
}

using ::curve::common::StringToUll;

// if direction is true means '+', false means '-'
// is direction is true, add second to first
// if direction is false, sub second from first
bool AddUllStringToFirst(std::string *first, uint64_t second, bool direction) {
    uint64_t firstNum = 0;
    uint64_t secondNum = second;
    if (StringToUll(*first, &firstNum)) {
        if (direction) {
            *first = std::to_string(firstNum + secondNum);
        } else {
            if (firstNum < secondNum) {
                *first = std::to_string(0);
                LOG(WARNING) << "AddUllStringToFirst failed when minus,"
                             << " first = " << firstNum
                             << ", second = " << secondNum;
                return false;
            }
            *first = std::to_string(firstNum - secondNum);
        }
    } else {
        LOG(ERROR) << "StringToUll failed, first = " << *first
                   << ", second = " << second;
        return false;
    }
    return true;
}

bool AddUllStringToFirst(uint64_t *first, const std::string &second) {
    uint64_t secondNum = 0;
    if (StringToUll(second, &secondNum)) {
        *first += secondNum;
        return true;
    }

    LOG(ERROR) << "StringToUll failed, second = " << second;
    return false;
}

}  // namespace common
}  // namespace client
}  // namespace curvefs
