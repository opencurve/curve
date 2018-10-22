/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 4:58:12 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_SESSION_H
#define CURVE_LIBCURVE_SESSION_H

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>
#include <mutex>    //NOLINT
#include <vector>
#include <atomic>

#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/request_scheduler.h"

namespace curve {
namespace client {

class MetaCache;
class IOContextManager;
class CURVE_CACHELINE_ALIGNMENT Session {
 public:
    Session();
    ~Session();

    bool Initialize();
    void UnInitialize();

    static CreateFileErrorType CreateFile(std::string filename, size_t size);
    static int GetFileInfo(std::string filename, FInfo_t* fi);
    OpenFileErrorType Open(std::string name);
    int GetOrAllocateSegment(off_t offset);
    int GetServerList(curve::client::LogicPoolID lpid,
                        const std::vector<CopysetID>& csid);

    MetaCache* GetMetaCache() {
        return mc_;
    }

    IOContextManager* GetIOCtxManager() {
        return ioctxManager_;
    }

 private:
    std::string filename_;
    std::mutex  mtx_;
    uint64_t segmentsize_;
    uint64_t chunksize_;

    MetaCache* mc_;
    IOContextManager*    ioctxManager_;
    brpc::Channel*      mdschannel_;
    RequestScheduler* scheduler_;
    RequestSenderManager* reqsenderManager_;
};
}   // namespace client
}   // namespace curve
#endif   // !CURVE_LIBCURVE_SESSION_H
