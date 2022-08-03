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
 * Project: Curve
 * Created Date: 2022-04-21
 * Author: huyao (baijiaruo)
 */

#ifndef CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_

#include "curvefs/src/client/error_code.h"
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs{
namespace client {

/* the base class of the underlying storage adaptation layer */
class StorageAdaptor {
 public:
    virtual ~StorageAdaptor() = default;
    /**
     * @brief Initailize some options for s3 adaptor
     */
    // virtual CURVEFS_ERROR Init() = 0;
    /**
     * @brief write data
     */
    virtual int Write(uint64_t ino, uint64_t offset, uint64_t len, const char* data) = 0;
    /**
     * @brief read data
     */
    virtual int Read(uint64_t ino, uint64_t offset, uint64_t len, char* data) = 0;
    /**
     * @brief truncate data , data len can be bigger or smaller
     */
    virtual CURVEFS_ERROR Truncate(Inode *inode, uint64_t size) = 0;
    /**
     * @brief flush file cache data to under storage
     */
    virtual CURVEFS_ERROR Flush(uint64_t ino) = 0;
    /**
     * @brief release file write and read cache. eg file is deleted should is used
     */
    virtual void ReleaseCache(uint64_t inodeId) = 0;
    /**
     * @brief flush write cache and release read cache. eg The cto scene file is closed
     */
    virtual CURVEFS_ERROR FlushAllCache(uint64_t inodeId) = 0;
    /**
     * @brief flush all files in fs flush write cache
     */
    virtual CURVEFS_ERROR FsSync() = 0;
    /**
     * @brief stop service eg umount scene
     */
    virtual int Stop() = 0;

    virtual std::shared<UnderStorage> GetUnderStorage() = 0;
};
}    
}

#endif  // CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_ 
