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
 * Created Date: 22-04-21
 * Author: huyao (baijiaruo)
 */
#ifndef CURVEFS_SRC_CLIENT_VOLUME_CLIENT_VOLUME_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_VOLUME_CLIENT_VOLUME_ADAPTOR_H_

#include "curvefs/client/client_storage_adaptor.h"
#include "curvefs/client/cache/client_cache_manager.h"
#include "curvefs/client/volume/volume_storage.h"

/* volume scene adaptor */
class VolumeAdaptor : public StorageAdaptor{
 public:
    VolumeAdaptor(std::shared_ptr<VolumeStorage> volumeStorage,
                  std::shared_ptr<FsCacheManager> fsCacheManager);
    virtual ~VolumeAdaptor() {}
    CURVEFS_ERROR Init();
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data);
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data);     
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size);
    CURVEFS_ERROR Flush(uint64_t ino);
    void ReleaseCache(uint64_t inodeId);
    CURVEFS_ERROR FlushAllCache(uint64_t inodeId);
    CURVEFS_ERROR FsSync();
    int Stop();
 private:
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<VolumeStorage> volumeStorage_;     
};


#endif
