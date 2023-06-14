/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * File Created: 2023-06-15
 * Author: xuchaojie
 */

#include "src/client/global_metacache.h"

namespace curve {
namespace client {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

MetaCache* GlobalMetaCache::GetOrNewMetaCacheInstance(
    const std::string &fileName, 
    const UserInfo_t &userInfo,
    MDSClient* mdsclient) {
    {
        ReadLockGuard lk(mCacheMapMutex_);
        auto it = mCacheMap_.find(fileName);
        if (it != mCacheMap_.end()) {
            return it->second;
        }
    }
    {
        WriteLockGuard lk(mCacheMapMutex_);
        auto it = mCacheMap_.find(fileName);
        if (it != mCacheMap_.end()) {
            return it->second;
        } else {
            MetaCache *mCache = new MetaCache();
            mCache->Init(metaCacheOpt_, mdsclient);

            FInfo fileInfo;
            FileEpoch_t fEpoch;
            LIBCURVE_ERROR ret = 
                mdsclient->GetFileInfo(fileName, userInfo, &fileInfo, &fEpoch);
            if (ret != 0) {
                LOG(ERROR) << "Get file info failed!";
                delete mCache;
                return nullptr;
            }

            fileInfo.userinfo = userInfo;
            fileInfo.fullPathName = fileName;
            mCache->UpdateFileInfo(fileInfo);
            mCache->UpdateFileEpoch(fEpoch);

            mCacheMap_.emplace(fileName, mCache);
            return mCache;
        }
    }
}

}   // namespace client
}   // namespace curve

