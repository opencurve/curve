/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 4:58:12 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef SRC_CLIENT_FILE_INSTANCE_H_
#define SRC_CLIENT_FILE_INSTANCE_H_

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
     * @param: filename文件名用于初始化iomanager的metric信息
     * @param: mdsclient为全局的mds client
     * @param: userinfo为user信息
     * @param: fileservicopt fileclient的配置选项
     * @param: clientMetric为client端要统计的metric信息
     * @param: readonly是否以只读方式打开
     * @return: 成功返回true、否则返回false
     */
    bool Initialize(const std::string& filename,
                    MDSClient* mdsclient,
                    const UserInfo_t& userinfo,
                    FileServiceOption_t fileservicopt,
                    bool readonly = false);
    /**
     * 打开文件
     * @param: filename为文件名
     * @param: userinfo为user信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
     */
    int Open(const std::string& filename,
             const UserInfo& userinfo,
             std::string* sessionId = nullptr);

    /**
     * 重新打开文件
     * @param filename为文件名
     * @param sessionId为上次打开文件时返回的sessionid
     * @param userInfo为user信息
     * @param[out] newSessionId为ReOpen成功时返回的新sessionid
     * @return 成功返回LIBCURVE_ERROR::OK, 否则LIBCURVE_ERROR::FAILED
     */
    int ReOpen(const std::string& filenam,
               const std::string& sessionId,
               const UserInfo& userInfo,
               std::string* newSessionId);
    /**
     * 同步模式读
     * @param: buf为当前待读取的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return： 成功返回读取真实长度，-1为失败
     */
    int Read(char* buf, off_t offset, size_t length);
    /**
     * 同步模式写
     * @param: buf为当前待写入的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return： 成功返回写入真实长度，-1为失败
     */
    int Write(const char* buf, off_t offset, size_t length);
    /**
     * 异步模式读
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     * @return: 0为成功，小于0为失败
     */
    int AioRead(CurveAioContext* aioctx);
    /**
     * 异步模式写
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     * @return: 0为成功，小于0为失败
     */
    int AioWrite(CurveAioContext* aioctx);

    int Close();

    void UnInitialize();

    IOManager4File* GetIOManager4File() {return &iomanager4file_;}

    /**
     * 获取lease, 测试代码使用
     */
    LeaseExcutor* GetLeaseExcutor() {
       return leaseexcutor_;
    }

    int GetFileInfo(const std::string& filename, FInfo_t* fi);

 private:
    // 保存当前file的文件信息
    FInfo_t                 finfo_;

    // 当前FileInstance的初始化配置信息
    FileServiceOption_t     fileopt_;

    // MDSClient是FileInstance与mds通信的唯一出口
    MDSClient*              mdsclient_;

    // 每个文件都持有与MDS通信的lease，leaseexcutor是续约执行者
    LeaseExcutor*           leaseexcutor_;

    // IOManager4File用于管理所有向chunkserver端发送的IO
    IOManager4File          iomanager4file_;

    // 是否为只读方式
    bool                   readonly_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_FILE_INSTANCE_H_
