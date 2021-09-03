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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/error_code.h"

namespace curvefs {
namespace client {

using ::curvefs::metaserver::MetaStatusCode;

CURVEFS_ERROR MetaStatusCodeToCurvefsErrCode(
    MetaStatusCode code) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::UNKNOWN;
    switch (code) {
        case MetaStatusCode::OK:
            ret = CURVEFS_ERROR::OK;
            break;

        case MetaStatusCode::NOT_FOUND:
            ret = CURVEFS_ERROR::NOTEXIST;
            break;

        case MetaStatusCode::PARAM_ERROR:
            ret = CURVEFS_ERROR::INVALIDPARAM;
            break;

        case MetaStatusCode::INODE_EXIST:
        case MetaStatusCode::DENTRY_EXIST:
            ret = CURVEFS_ERROR::EXISTS;
            break;

        case MetaStatusCode::SYM_LINK_EMPTY:
        case MetaStatusCode::RPC_ERROR:
            ret = CURVEFS_ERROR::INTERNAL;
            break;

        default:
            ret = CURVEFS_ERROR::UNKNOWN;
    }
    return ret;
}

}  // namespace client
}  // namespace curvefs
