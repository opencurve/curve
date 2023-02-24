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
/*
#ifndef CURVEFS_SRC_CLIENT_FUSE_S3_CLIENT_H_
#define CURVEFS_SRC_CLIENT_FUSE_S3_CLIENT_H_

#include <memory>
#include <string>
#include <list>
#include <vector>
#include <utility>

#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/cache/client_cache_manager.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace client {

using curve::common::GetObjectAsyncContext;
using curve::common::GetObjectAsyncCallBack;



whs：现在压根就不需要FuseS3Client，FuseVolumeClient了
只需要FuseClient就可以了
直接把FuseS3Client改成default版本的就可以了--- 主要的差别在于StorageAdaptor的差别

这种支持混合部署么? -- 如果是混合部署 -- 是不是只需要直接使用两个StorageAdaptor就可以了?
*/
// class FuseS3Client : public FuseClient {
/*
class FuseS3Client {
 public:
    FuseS3Client()
        : FuseClient(), s3Adaptor_(std::make_shared<S3ClientAdaptorImpl>()) {}

    FuseS3Client(const std::shared_ptr<MdsClient> &mdsClient,
                 const std::shared_ptr<MetaServerClient> &metaClient,
                 const std::shared_ptr<InodeCacheManager> &inodeManager,
                 const std::shared_ptr<DentryCacheManager> &dentryManager,
                 const std::shared_ptr<StorageAdaptor> &s3Adaptor)
        : FuseClient(mdsClient, metaClient, inodeManager, dentryManager),
          s3Adaptor_(s3Adaptor) {}

    CURVEFS_ERROR Init(const FuseClientOption &option) override;

    void UnInit() override;

    CURVEFS_ERROR FuseOpInit(
        void *userdata, struct fuse_conn_info *conn) override;

    CURVEFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi, size_t *wSize) override;

    CURVEFS_ERROR FuseOpRead(fuse_req_t req,
        fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi,
        char *buffer,
        size_t *rSize) override;

    CURVEFS_ERROR FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, struct fuse_file_info *fi,
        fuse_entry_param *e) override;

    CURVEFS_ERROR FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, dev_t rdev,
        fuse_entry_param *e) override;

    CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
        fuse_ino_t newparent, const char *newname,
        fuse_entry_param *e) override;

    CURVEFS_ERROR FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
        const char *name) override;

    CURVEFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
        struct fuse_file_info *fi) override;

    CURVEFS_ERROR FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) override;

 private:
    bool InitKVCache(const KVClientManagerOpt &opt);

    CURVEFS_ERROR Truncate(InodeWrapper *inode, uint64_t length) override;

    void FlushData() override;
    // get the warmUp filelist
    void GetWarmUpFileList(const WarmUpFileContext_t&,
      std::vector<std::string>&);
    void BackGroundFetch();
    // put the file needed warmup to queue,
    // then can downlaod the objs belong to it
    void fetchDataEnqueue(fuse_ino_t ino);
    // travel all chunks
    void travelChunks(
        fuse_ino_t ino,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3ChunkInfoMap);
    // travel and download all objs belong to the chunk
    void travelChunk(fuse_ino_t ino, S3ChunkInfoList chunkInfo,
      std::list<std::pair<std::string, uint64_t>>* prefetchObjs);
    // warmup all the prefetchObjs
    void WarmUpAllObjs(const std::list<
      std::pair<std::string, uint64_t>> &prefetchObjs);

 private:
    // s3 adaptor
    std::shared_ptr<StorageAdaptor> s3Adaptor_;

    bool initbgFetchThread_;
    Thread bgFetchThread_;
    std::atomic<bool> bgFetchStop_;
    std::mutex fetchMtx_;
    uint32_t downloadMaxRetryTimes_;
};
*/
/*
class FuseS3Client {
 public:
    FuseS3Client() 
        : FuseClient(), s3Adaptor_(std::make_shared<S3ClientAdaptorImpl>()) {}

    FuseS3Client(const std::shared_ptr<MdsClient> &mdsClient,
                 const std::shared_ptr<MetaServerClient> &metaClient,
                 const std::shared_ptr<InodeCacheManager> &inodeManager,
                 const std::shared_ptr<DentryCacheManager> &dentryManager,
                 const std::shared_ptr<StorageAdaptor> &s3Adaptor) 
        : FuseClient(mdsClient, metaClient, inodeManager, dentryManager),
          s3Adaptor_(s3Adaptor) {}

    CURVEFS_ERROR Init(const FuseClientOption &option);

    void UnInit();

    CURVEFS_ERROR FuseOpInit(
        void *userdata, struct fuse_conn_info *conn);

    CURVEFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi, size_t *wSize);

    CURVEFS_ERROR FuseOpRead(fuse_req_t req,
        fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi,
        char *buffer,
        size_t *rSize);

    CURVEFS_ERROR FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, struct fuse_file_info *fi,
        fuse_entry_param *e);

    CURVEFS_ERROR FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, dev_t rdev,
        fuse_entry_param *e);

    CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
        fuse_ino_t newparent, const char *newname,
        fuse_entry_param *e);

    CURVEFS_ERROR FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
        const char *name);

    CURVEFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
        struct fuse_file_info *fi);

    CURVEFS_ERROR FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi);

// whs

// whs
 private:
    bool InitKVCache(const KVClientManagerOpt &opt);

    CURVEFS_ERROR Truncate(InodeWrapper *inode, uint64_t length);

    void FlushData();
    // get the warmUp filelist
    void GetWarmUpFileList(const WarmUpFileContext_t&,
      std::vector<std::string>&);
    void BackGroundFetch();
    // put the file needed warmup to queue,
    // then can downlaod the objs belong to it
    void fetchDataEnqueue(fuse_ino_t ino);
    // travel all chunks
    void travelChunks(
        fuse_ino_t ino,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3ChunkInfoMap);
    // travel and download all objs belong to the chunk
    void travelChunk(fuse_ino_t ino, S3ChunkInfoList chunkInfo,
      std::list<std::pair<std::string, uint64_t>>* prefetchObjs);
    // warmup all the prefetchObjs
    void WarmUpAllObjs(const std::list<
      std::pair<std::string, uint64_t>> &prefetchObjs);

 private:
    // s3 adaptor
    std::shared_ptr<StorageAdaptor> s3Adaptor_;

    bool initbgFetchThread_;
    Thread bgFetchThread_;
    std::atomic<bool> bgFetchStop_;
    std::mutex fetchMtx_;
    uint32_t downloadMaxRetryTimes_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_S3_CLIENT_H_
*/