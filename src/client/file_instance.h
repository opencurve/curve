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
#include <condition_variable>   // NOLINT

#include "src/client/mds_client.h"
#include "src/client/libcurve_define.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/service_helper.h"
#include "src/client/iomanager4file.h"
#include "src/client/lease_excutor.h"

namespace curve {
namespace client {
class CURVE_CACHELINE_ALIGNMENT FileInstance {
 public:
    FileInstance();
    ~FileInstance() = default;

    /**
     * 初始化
     * @param: fileservicopt fileclient的配置选项
     * @return: 成功返回true、否则返回false
     */
    bool Initialize(FileServiceOption_t fileservicopt);
    /**
     * 打开文件
     * @param: filename为文件名
     * @param: size为文件长度
     * @param: create是创建flag
     * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR Open(std::string filename, size_t size, bool create);
    /**
     * 同步模式读
     * @param: buf为当前待读取的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return： 成功返回读取真实长度，-1为失败
     */
    LIBCURVE_ERROR  Read(char* buf, off_t offset, size_t length);
    /**
     * 同步模式写
     * @param: buf为当前待写入的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return： 成功返回写入真实长度，-1为失败
     */
    LIBCURVE_ERROR  Write(const char* buf, off_t offset, size_t length);
    /**
     * 异步模式读
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     */
    void AioRead(CurveAioContext* aioctx);
    /**
     * 异步模式写
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     */
    void AioWrite(CurveAioContext* aioctx);
    /**
     * 获取文件信息
     * @param: filename为文件名
     * @param: finfo是出参，携带当前文件的基础信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR StatFs(std::string filename, FileStatInfo* finfo);
    LIBCURVE_ERROR Close();

    void UnInitialize();

    IOManager4File* GetIOManager4File() {return &iomanager4file_;}

 private:
    LIBCURVE_ERROR GetFileInfo(std::string filename, FInfo_t* fi);
    LIBCURVE_ERROR CreateFile(std::string filename, size_t size);

 private:
    // 保存当前file的文件信息
    FInfo_t                 finfo_;

    // 当前FileInstance的初始化配置信息
    FileServiceOption_t     fileopt_;

    // MDSClient是FileInstance与mds通信的唯一出口
    MDSClient               mdsclient_;

    // 每个文件都持有与MDS通信的lease，leaseexcutor是续约执行者
    LeaseExcutor*           leaseexcutor_;

    // IOManager4File用于管理所有向chunkserver端发送的IO
    IOManager4File          iomanager4file_;
};
}   // namespace client
}   // namespace curve
#endif   // !CURVE_LIBCURVE_SESSION_H
