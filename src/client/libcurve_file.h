/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: Monday, 13th February 2019 9:46:54 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_LIBCURVE_FILE_H_
#define SRC_CLIENT_LIBCURVE_FILE_H_

#include <unistd.h>
#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/client/chunkserver_broadcaster.h"

// TODO(tongguangxun) :添加关键函数trace功能
namespace curve {
namespace client {

using curve::common::BthreadRWLock;

class FileClient {
 public:
    FileClient();
    virtual ~FileClient() = default;

    /**
     * file对象初始化函数
     * @param: 配置文件路径
     */
    virtual int Init(const std::string& configpath);

    /**
     * 打开或创建文件
     * @param: filename文件名
     * @param: userinfo是操作文件的用户信息
     * @return: 返回文件fd
     */
    virtual int Open(const std::string& filename,
                     const UserInfo_t& userinfo,
                     const OpenFlags& openflags = {});

    /**
     * 打开文件，这个打开只是创建了一个fd，并不与mds交互，没有session续约
     * 这个Open接口主要是提供给快照克隆镜像系统做数据拷贝使用
     * @param: filename文件名
     * @param: userinfo当前用户信息
     * @param disableStripe enable/disable stripe feature for a stripe file
     * @return: 返回文件fd
     */
    virtual int Open4ReadOnly(const std::string& filename,
                              const UserInfo_t& userinfo,
                              bool disableStripe = false);

    /**
     * @brief increase epoch
     *
     * @param filename file name
     * @param userinfo user info
     *
     * @return 0 for success, -1 for fail
     */
    int IncreaseEpoch(const std::string& filename,
                      const UserInfo_t& userinfo);

    /**
     * 创建文件
     * @param: filename文件名
     * @param: userinfo是当前打开或创建时携带的user信息
     * @param: size文件长度，当create为true的时候以size长度创建文件
     * @return: 成功返回0, 失败可能有多种可能
     *          比如内部错误，或者文件已存在
     */
    virtual int Create(const std::string& filename,
                       const UserInfo_t& userinfo,
                       size_t size);

    /**
     * Create file with parameters
     * @return: success return 0,  fail return less than 0
     */
    virtual int Create2(const CreateFileContext& context);

    /**
     * 同步模式读
     * @param: fd为当前open返回的文件描述符
     * @param: buf为当前待读取的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return: 成功返回读取字节数,否则返回小于0的错误码
     */
    virtual int Read(int fd, char* buf, off_t offset, size_t length);

    /**
     * 同步模式写
     * @param: fd为当前open返回的文件描述符
     * @param: buf为当前待写入的缓冲区
     * @param：offset文件内的便宜
     * @parma：length为待读取的长度
     * @return: 成功返回写入字节数,否则返回小于0的错误码
     */
    virtual int Write(int fd, const char* buf, off_t offset, size_t length);

    /**
     * @brief Synchronous discard operation
     * @param fd file descriptor
     * @param offset discard offset
     * @param length discard length
     * @return On success, returns 0.
     *         On error, returns a negative value.
     */
    virtual int Discard(int fd, off_t offset, size_t length);

    /**
     * 异步模式读
     * @param: fd为当前open返回的文件描述符
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return: 成功返回读取字节数,否则返回小于0的错误码
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx,
                        UserDataType dataType = UserDataType::RawBuffer);

    /**
     * 异步模式写
     * @param: fd为当前open返回的文件描述符
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return: 成功返回写入字节数,否则返回小于0的错误码
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType = UserDataType::RawBuffer);

    /**
     * @brief Asynchronous discard operation
     * @param fd file descriptor
     * @param aioctx async request context
     * @return 0 means success, otherwise it means failure
     */
    virtual int AioDiscard(int fd, CurveAioContext* aioctx);

    /**
     * 重命名文件
     * @param: userinfo是用户信息
     * @param: oldpath源路劲
     * @param: newpath目标路径
     */
    virtual int Rename(const UserInfo_t& userinfo,
                       const std::string& oldpath,
                       const std::string& newpath);

    /**
     * 扩展文件
     * @param: userinfo是用户信息
     * @param: filename文件名
     * @param: newsize新的size
     */
    virtual int Extend(const std::string& filename,
                       const UserInfo_t& userinfo,
                       uint64_t newsize);

    /**
     * 删除文件
     * @param: userinfo是用户信息
     * @param: filename待删除的文件名
     * @param: deleteforce=true只能用于从回收站删除,false为放入垃圾箱
     */
    virtual int Unlink(const std::string& filename,
                       const UserInfo_t& userinfo,
                       bool deleteforce = false);

    /**
     * recycle file
     * @param: userinfo
     * @param: filename
     * @param: fileId
     */
    virtual int Recover(const std::string& filename,
                        const UserInfo_t& userinfo,
                        uint64_t fileId);

    /**
     * 枚举目录内容
     * @param: userinfo是用户信息
     * @param: dirpath是目录路径
     * @param[out]: filestatVec当前文件夹内的文件信息
     */
    virtual int Listdir(const std::string& dirpath,
                        const UserInfo_t& userinfo,
                        std::vector<FileStatInfo>* filestatVec);

    /**
     * 创建目录
     * @param: userinfo是用户信息
     * @param: dirpath是目录路径
     */
    virtual int Mkdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * 删除目录
     * @param: userinfo是用户信息
     * @param: dirpath是目录路径
     */
    virtual int Rmdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * 获取文件信息
     * @param: filename文件名
     * @param: userinfo是用户信息
     * @param: finfo是出参，携带当前文件的基础信息
     * @return: 成功返回int::OK,否则返回小于0的错误码
     */
    virtual int StatFile(const std::string& filename,
                         const UserInfo_t& userinfo,
                         FileStatInfo* finfo);

     /**
      * stat file
      * @param: fd is file descriptor.
      * @param: finfo is an output para, carry the base info of current file.
      * @return: returns int::ok if success, 
      *          otherwise returns an error code less than 0
      */
    virtual int StatFile(int fd, FileStatInfo* finfo);

    /**
     * 变更owner
     * @param: filename待变更的文件名
     * @param: newOwner新的owner信息
     * @param: userinfo执行此操作的user信息，只有root用户才能执行变更
     * @return: 成功返回0，
     *          否则返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
     */
    virtual int ChangeOwner(const std::string& filename,
                            const std::string& newOwner,
                            const UserInfo_t& userinfo);
    /**
     * close通过fd找到对应的instance进行删除
     * @param: fd为当前open返回的文件描述符
     * @return: 成功返回int::OK,否则返回小于0的错误码
     */
    virtual int Close(int fd);

    /**
     * 析构，回收资源
     */
    virtual void UnInit();

    /**
     * @brief: 获取集群id
     * @param: buf存放集群id
     * @param: buf的长度
     * @return: 成功返回0, 失败返回-LIBCURVE_ERROR::FAILED
     */
    int GetClusterId(char* buf, int len);

    /**
     * @brief 获取集群id
     * @return 成功返回集群id，失败返回空
     */
    std::string GetClusterId();

    /**
     * @brief 获取文件信息，测试使用
     * @param fd 文件句柄
     * @param[out] finfo 文件信息
     * @return 成功返回0，失败返回-LIBCURVE_ERROR::FAILED
     */
    int GetFileInfo(int fd, FInfo* finfo);

    // List all poolsets' name in cluster
    std::vector<std::string> ListPoolset();

    /**
     * 测试使用，获取当前挂载文件数量
     * @return 返回当前挂载文件数量
     */
    uint64_t GetOpenedFileNum() const {
        return openedFileNum_.get_value();
    }

 private:
    static void BuildFileStatInfo(const FInfo_t &fi, FileStatInfo *finfo);

    bool StartDummyServer();

    bool CheckAligned(off_t offset, size_t length) const;

 private:
    BthreadRWLock rwlock_;

    // 向上返回的文件描述符，对于QEMU来说，一个vdisk对应一个文件描述符
    std::atomic<uint64_t> fdcount_;

    // 每个vdisk都有一个FileInstance，通过返回的fd映射到对应的instance
    std::unordered_map<int, FileInstance*> fileserviceMap_;
    // <filename, FileInstance>
    std::unordered_map<std::string, FileInstance*> fileserviceFileNameMap_;

    // FileClient配置
    ClientConfig clientconfig_;

    // fileclient对应的全局mdsclient
    std::shared_ptr<MDSClient> mdsClient_;

    // chunkserver client
    std::shared_ptr<ChunkServerClient> csClient_;
    // chunkserver broadCaster
    std::shared_ptr<ChunkServerBroadCaster> csBroadCaster_;

    // 是否初始化成功
    bool inited_;

    // 挂载文件数量
    bvar::Adder<uint64_t> openedFileNum_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_LIBCURVE_FILE_H_
