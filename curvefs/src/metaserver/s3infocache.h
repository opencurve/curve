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
 * @Project: curve
 * @Date: 2021-11-02
 * @Author: majie1
 */

#ifndef CURVEFS_SRC_METASERVER_S3INFOCACHE_H_
#define CURVEFS_SRC_METASERVER_S3INFOCACHE_H_

#include <braft/closure_helper.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {

using curvefs::common::S3Info;

class S3InfoCache {
 private:
    std::mutex mtx_;
    uint64_t capacity_;
    std::vector<std::string> mdsAddrs_;
    uint64_t mdsIndex_;
    butil::EndPoint metaserverAddr_;
    // lru cache
    std::unordered_map<uint64_t, S3Info> cache_;
    std::list<uint64_t> recent_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> pos_;

    void UpdateRecent(uint64_t fsid);
    S3Info Get(uint64_t fsid);
    void Put(uint64_t fsid, const S3Info& s3info);

 public:
    enum class RequestStatusCode { SUCCESS, NOS3INFO, RPCFAILURE };
    explicit S3InfoCache(uint64_t capacity,
                         const std::vector<std::string>& mdsAddrs,
                         butil::EndPoint metaserverAddr)
        : capacity_(capacity),
          mdsAddrs_(mdsAddrs),
          mdsIndex_(0),
          metaserverAddr_(metaserverAddr) {}
    virtual ~S3InfoCache() {}
    virtual RequestStatusCode RequestS3Info(uint64_t fsid, S3Info* s3info);
    virtual int GetS3Info(uint64_t fsid, S3Info* s3info);
    virtual void InvalidateS3Info(uint64_t fsid);
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3INFOCACHE_H_
