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
 * File Created: Tuesday, 25th September 2018 4:58:12 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_FILE_INSTANCE_H_
#define SRC_CLIENT_FILE_INSTANCE_H_

#include <memory>
#include <string>

#include "src/client/mds_client.h"
#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/service_helper.h"
#include "src/client/iomanager4file.h"
#include "src/client/lease_executor.h"

namespace curve {
namespace client {

class CURVE_CACHELINE_ALIGNMENT FileInstance {
 public:
    FileInstance() = default;
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
                    const std::shared_ptr<MDSClient>& mdsclient,
                    const UserInfo& userinfo,
                    const OpenFlags& openflags,
                    const FileServiceOption& fileservicopt,
                    bool readonly = false);
    /**
     * 打开文件
     * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
     */
    int Open(std::string* sessionId = nullptr);

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
     * @param: dataType type of user buffer
     * @return: 0为成功，小于0为失败
     */
    int AioRead(CurveAioContext* aioctx, UserDataType dataType);
    /**
     * 异步模式写
     * @param: aioctx为异步读写的io上下文，保存基本的io信息
     * @param: dataType type of user buffer
     * @return: 0为成功，小于0为失败
     */
    int AioWrite(CurveAioContext* aioctx, UserDataType dataType);

    int Close();

    void UnInitialize();

    IOManager4File* GetIOManager4File() {
        return &iomanager4file_;
    }

    /**
     * 获取lease, 测试代码使用
     */
    LeaseExecutor* GetLeaseExecutor() const {
        return leaseExecutor_.get();
    }

    int GetFileInfo(const std::string& filename,
        FInfo_t* fi, FileEpoch_t *fEpoch);

    void UpdateFileEpoch(const FileEpoch_t &fEpoch) {
        iomanager4file_.UpdateFileEpoch(fEpoch);
    }

    /**
     * @brief 获取当前instance对应的文件信息
     *
     * @return 当前instance对应文件的信息
     */
    FInfo GetCurrentFileInfo() const {
        return finfo_;
    }

    static FileInstance* NewInitedFileInstance(
        const FileServiceOption& fileServiceOption,
        const std::shared_ptr<MDSClient>& mdsclient,
        const std::string& filename,
        const UserInfo& userInfo,
        const OpenFlags& openflags,
        bool readonly);

    static FileInstance* Open4Readonly(
        const FileServiceOption& opt,
        const std::shared_ptr<MDSClient>& mdsclient,
        const std::string& filename,
        const UserInfo& userInfo,
        const OpenFlags& openflags = DefaultReadonlyOpenFlags());

 private:
    void StopLease();

 private:
    // 保存当前file的文件信息
    FInfo finfo_;

    // 当前FileInstance的初始化配置信息
    FileServiceOption       fileopt_;

    // MDSClient是FileInstance与mds通信的唯一出口
    std::shared_ptr<MDSClient> mdsclient_;

    // 每个文件都持有与MDS通信的lease，LeaseExecutor是续约执行者
    std::unique_ptr<LeaseExecutor> leaseExecutor_;

    // IOManager4File用于管理所有向chunkserver端发送的IO
    IOManager4File          iomanager4file_;

    // 是否为只读方式
    bool                   readonly_ = false;

    // offset and length must align with `blocksize_`
    // 4096 for backward compatibility
    size_t blocksize_ = 4096;
};

bool CheckAlign(off_t off, size_t length, size_t blocksize);

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_FILE_INSTANCE_H_
