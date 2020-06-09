/*
 * Project: curve
 * Created Date: Friday April 24th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_TOOLS_NBD_NBDTOOL_H_
#define SRC_TOOLS_NBD_NBDTOOL_H_

#include <vector>
#include <string>
#include <memory>
#include "src/tools/nbd/define.h"
#include "src/tools/nbd/NBDController.h"
#include "src/tools/nbd/NBDServer.h"
#include "src/tools/nbd/ImageInstance.h"
#include "src/tools/nbd/NBDWatchContext.h"

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
    // 根据设备路径卸载已经映射的文件
    int Disconnect(const std::string& devpath);
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
    ImagePtr GenerateImage(const std::string& imageName);

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

#endif  // SRC_TOOLS_NBD_NBDTOOL_H_
