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
 * Created Date: Tuesday April 21st 2020
 * Author: yangyaokai
 */

#include <glog/logging.h>
#include <limits.h>
#include <memory>
#include <string>
#include "nbd/src/NBDTool.h"
#include "nbd/src/argparse.h"
#include "nbd/src/texttable.h"

namespace curve {
namespace nbd {

std::ostream& operator<<(std::ostream& os, const DeviceInfo& info) {
    TextTable tbl;
    tbl.define_column("pid", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("options", TextTable::LEFT, TextTable::LEFT);
    tbl << info.pid << info.config.imgname << info.config.devpath
        << info.config.MapOptions() << TextTable::endrow;
    os << tbl;
    return os;
}

NBDControllerPtr NBDTool::GetController(bool tryNetlink) {
    if (tryNetlink) {
        auto ctrl = std::make_shared<NetLinkController>();
        bool supportNetLink = ctrl->Support();
        if (supportNetLink) {
            return ctrl;
        }
    }
    return std::make_shared<IOController>();
}

int NBDTool::Connect(NBDConfig *cfg) {
    // loadmodule 到时候放到外面做

    // init socket pair
    int ret = socketPair_.Init();
    if (ret < 0) {
        return ret;
    }

    // 初始化打开文件
    ImagePtr imageInstance = GenerateImage(cfg->imgname, cfg);
    bool openSuccess = imageInstance->Open();
    if (!openSuccess) {
        cerr << "curve-nbd: Could not open image." << std::endl;
        return -1;
    }

    // 判断文件大小是否符合预期
    int64_t fileSize = imageInstance->GetImageSize();
    if (fileSize <= 0) {
        cerr << "curve-nbd: Get file size failed." << std::endl;
        return -1;
    } else if ( (uint64_t)fileSize > ULONG_MAX ) {
        cerr << "curve-nbd: image is too large (" << (uint64_t)fileSize
             << ", max is " << ULONG_MAX << ")" << std::endl;
        return -1;
    }

    // load nbd module
    ret = load_module(cfg);
    if (ret < 0) {
        return ret;
    }

    NBDControllerPtr nbdCtrl = GetController(cfg->try_netlink);
    nbdServer_ = std::make_shared<NBDServer>(socketPair_.Second(), nbdCtrl,
                                             imageInstance);

    // setup controller
    uint64_t flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM |
                     NBD_FLAG_HAS_FLAGS;
    if (cfg->readonly) {
        flags |= NBD_FLAG_READ_ONLY;
    }
    ret = nbdCtrl->SetUp(cfg, socketPair_.First(), fileSize, flags);
    if (ret < 0) {
        return -1;
    }

    nbdWatchCtx_ =
        std::make_shared<NBDWatchContext>(nbdCtrl, imageInstance, fileSize);

    return 0;
}

int NBDTool::Disconnect(const NBDConfig* config) {
    pid_t devpid = -1;
    std::vector<DeviceInfo> devices;

    int ret = List(&devices);
    for (const auto& device : devices) {
        if (device.config.devpath == config->devpath) {
            devpid = device.pid;
            break;
        }
    }

    NBDControllerPtr nbdCtrl = GetController(false);
    ret = nbdCtrl->DisconnectByPath(config->devpath);
    if (ret != 0) {
        return ret;
    }

    ret = WaitForTerminate(devpid, config);

    return 0;
}

int NBDTool::List(std::vector<DeviceInfo>* infos) {
    DeviceInfo info;
    NBDListIterator it;
    while (it.Get(&info.pid, &info.config)) {
        infos->push_back(info);
    }
    return 0;
}

void NBDTool::RunServerUntilQuit() {
    // start nbd server
    nbdServer_->Start();

    // start watch context
    nbdWatchCtx_->WatchImageSize();

    NBDControllerPtr ctrl = nbdServer_->GetController();
    if (ctrl->IsNetLink()) {
        nbdServer_->WaitForDisconnect();
    } else {
        ctrl->RunUntilQuit();
    }

    nbdWatchCtx_->StopWatch();
}

ImagePtr g_test_image = nullptr;
ImagePtr NBDTool::GenerateImage(const std::string& imageName,
                                NBDConfig* config) {
    ImagePtr result = nullptr;
    if (imageName.compare(0, 4, "test") == 0) {
        result = g_test_image;
    } else {
        result = std::make_shared<ImageInstance>(imageName, config);
    }
    return result;
}

int NBDTool::WaitForTerminate(pid_t pid, const NBDConfig* config) {
    if (pid < 0) {
        return 0;
    }

    int times = config->retry_times;
    while (times-- > 0) {
        if (kill(pid, 0) == -1) {
            if (errno == ESRCH) {
                return 0;
            }
            std::cerr << "curve-nbd test device failed, dev: "
                      << config->devpath << ", err = " << cpp_strerror(-errno)
                      << std::endl;
            return -errno;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(config->sleep_ms));
    }

    return -ETIMEDOUT;
}

}  // namespace nbd
}  // namespace curve
