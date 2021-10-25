/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/*
 * Project: curve
 * Created Date: Friday April 24th 2020
 * Author: yangyaokai
 */

#ifndef NBD_SRC_NBDTOOL_H_
#define NBD_SRC_NBDTOOL_H_

#include <vector>
#include <string>
#include <memory>
#include "nbd/src/define.h"
#include "nbd/src/NBDController.h"
#include "nbd/src/NBDServer.h"
#include "nbd/src/ImageInstance.h"
#include "nbd/src/NBDWatchContext.h"

namespace curve {
namespace nbd {

extern ImagePtr g_test_image;
struct DeviceInfo {
    int pid;
    NBDConfig config;
};
std::ostream& operator<<(std::ostream& os, const DeviceInfo& info);

// nbd-curve工具的管理模块，负责与nbd server的启动，与nbd内核模块的通信建立
class NBDTool {
 public:
    NBDTool() {}
    ~NBDTool() {}

    // 与nbd内核建立通信，将文件映射到本地
    int Connect(NBDConfig *cfg);
    // 根据配置卸载已经映射的文件
    int Disconnect(const NBDConfig* config);
    // 获取已经映射文件的映射信息，包括进程pid、文件名、设备路径
    int List(std::vector<DeviceInfo>* infos);
    // 阻塞直到nbd server退出
    void RunServerUntilQuit();

 private:
    // 获取指定类型的nbd controller
    NBDControllerPtr GetController(bool tryNetlink);
    // 启动nbd server
    NBDServerPtr StartServer(int sockfd, NBDControllerPtr nbdCtrl,
                             ImagePtr imageInstance);
    // 生成image instance
    ImagePtr GenerateImage(const std::string& imageName, NBDConfig* config);

    // wait curve-nbd process to exit
    int WaitForTerminate(pid_t pid, const NBDConfig* config);

 private:
    class NBDSocketPair {
     public:
        NBDSocketPair() : inited_(false) {}
        ~NBDSocketPair() {
            Uninit();
        }

        int Init() {
            if (inited_) {
                return 0;
            }
            int ret = socketpair(AF_UNIX, SOCK_STREAM, 0, fd_);
            if (ret < 0) {
                return -errno;
            }
            inited_ = true;
            return 0;
        }

        void Uninit() {
            if (inited_) {
                close(fd_[0]);
                close(fd_[1]);
                inited_ = false;
            }
        }

        int First() {
            return fd_[0];
        }

        int Second() {
            return fd_[1];
        }

     private:
        bool inited_;
        int fd_[2];
    };

    NBDSocketPair socketPair_;
    NBDServerPtr nbdServer_;
    std::shared_ptr<NBDWatchContext> nbdWatchCtx_;
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_SRC_NBDTOOL_H_
