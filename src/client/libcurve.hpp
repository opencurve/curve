/*
 * Project: curve
 * File Created: Saturday, 29th September 2018 5:15:04 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_H
#define CURVE_LIBCURVE_H

#include <unistd.h>
#include <string>
#include <atomic>
#include <unordered_map>

#include "include/client/libcurve.h"
#include "src/common/rw_lock.h"
#include "src/client/session.h"

using curve::common::RWLock;

namespace curve {
namespace client {

class LibCurve {
 public:
    LibCurve();
    ~LibCurve();

    static int Init(std::string configpath);
    static void UnInit(); 
    static CreateFileErrorType CreateFile(std::string name, size_t size); 
    static int Open(std::string filename);
    static int Read(int fd, char* buf, off_t offset, size_t length);
    static int Write(int fd, const char* buf, off_t offset, size_t length);
    static int AioRead(int fd, CurveAioContext* aioctx);
    static int AioWrite(int fd, CurveAioContext* aioctx);
    static FInfo GetInfo(std::string filename);
    static void Close(int fd);
    static void Unlink(std::string filename);

 private:
    static std::unordered_map<int, Session*> sessionMap_;
    static std::atomic<uint64_t>    fdcount_;
    static RWLock rwlock_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_H
