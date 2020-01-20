/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_REQUEST_EXECUTOR_H_
#define SRC_PART2_REQUEST_EXECUTOR_H_

#include <map>
#include <string>
#include "src/part2/define.h"

namespace nebd {
namespace server {

class CurveRequestExecutor;
class CephRequestExecutor;

using AdditionType = std::map<std::string, std::string>;
// 具体RequestExecutor中会用到的文件实例上下文信息
// 该类为抽象结构，ceph或curve需要继承定义自己的FileInstance
// RequestExecutor需要用到的文件上下文信息都记录到FileInstance内
class NebdFileInstance {
 public:
    NebdFileInstance() {}
    virtual ~NebdFileInstance() {}
    // 需要持久化到文件的内容，以kv形式返回，例如curve open时返回的sessionid
    // 文件reopen的时候也会用到该内容
    AdditionType addition;
};

class NebdRequestExecutor {
 public:
    NebdRequestExecutor() {}
    virtual ~NebdRequestExecutor() {}
    virtual std::shared_ptr<NebdFileInstance> Open(const std::string& filename) = 0;  // NOLINT
    virtual std::shared_ptr<NebdFileInstance> Reopen(
        const std::string& filename, AdditionType addtion) = 0;
    virtual int Close(NebdFileInstance* fd) = 0;
    virtual int Extend(NebdFileInstance* fd, int64_t newsize) = 0;
    virtual int GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) = 0;
    virtual int StatFile(NebdFileInstance* fd, NebdFileInfo* fileInfo) = 0;
    virtual int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;  // NOLINT
    virtual int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int InvalidCache(NebdFileInstance* fd) = 0;
};

class NebdRequestExecutorFactory {
 public:
    NebdRequestExecutorFactory() = default;
    ~NebdRequestExecutorFactory() = default;

    static NebdRequestExecutor* GetExecutor(NebdFileType type);
};

class NebdFileInstanceFactory {
 public:
    NebdFileInstanceFactory() = default;
    ~NebdFileInstanceFactory() = default;

    static NebdFileInstancePtr GetInstance(NebdFileType type);
};

// for test
extern NebdRequestExecutor* g_test_executor;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_REQUEST_EXECUTOR_H_
