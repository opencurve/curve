/*
 * Project: curve
 * Created Date: Tuesday April 21st 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <glog/logging.h>
#include <limits.h>
#include <memory>
#include <string>
#include "src/tools/nbd/NBDTool.h"
#include "src/tools/nbd/argparse.h"
#include "src/tools/nbd/texttable.h"

namespace curve {
namespace nbd {

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


std::ostream& operator<<(std::ostream& os, const DeviceInfo& info) {
    TextTable tbl;
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
    tbl << info.pid << info.config.imgname
        << info.config.devpath << TextTable::endrow;
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
    NBDSocketPair sockPair;
    int ret = sockPair.Init();
    if (ret < 0) {
        return ret;
    }

    // 初始化打开文件
    ImagePtr imageInstance = GenerateImage(cfg->imgname);
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

    // start NBDServer
    NBDControllerPtr nbdCtrl = GetController(cfg->try_netlink);
    auto server = StartServer(sockPair.Second(), nbdCtrl, imageInstance);

    // setup controller
    uint64_t flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM |
                     NBD_FLAG_HAS_FLAGS;
    if (cfg->readonly) {
        flags |= NBD_FLAG_READ_ONLY;
    }
    ret = nbdCtrl->SetUp(cfg, sockPair.First(), fileSize, flags);
    if (ret < 0) {
        return -1;
    }

    // watch context start
    NBDWatchContext watchCtx(nbdCtrl, imageInstance, fileSize);
    watchCtx.WatchImageSize();

    // run server until quit
    RunServerUntilQuit(server);
    return 0;
}

int NBDTool::Disconnect(const std::string& devpath) {
    NBDControllerPtr nbdCtrl = GetController(false);
    return nbdCtrl->DisconnectByPath(devpath);
}

int NBDTool::List(std::vector<DeviceInfo>* infos) {
    DeviceInfo info;
    NBDListIterator it;
    while (it.Get(&info.pid, &info.config)) {
        infos->push_back(info);
    }
    return 0;
}

NBDServerPtr NBDTool::StartServer(int sockfd, NBDControllerPtr nbdCtrl,
                                  ImagePtr imageInstance) {
    auto server = std::make_shared<NBDServer>(sockfd, nbdCtrl, imageInstance);
    server->Start();

    return server;
}

void NBDTool::RunServerUntilQuit(NBDServerPtr server) {
    NBDControllerPtr ctrl = server->GetController();
    if (ctrl->IsNetLink()) {
        server->WaitForDisconnect();
    } else {
        ctrl->RunUntilQuit();
    }
}

ImagePtr g_test_image = nullptr;
ImagePtr NBDTool::GenerateImage(const std::string& imageName) {
    ImagePtr result = nullptr;
    if (imageName.compare(0, 4, "test") == 0) {
        result = g_test_image;
    } else {
        result = std::make_shared<ImageInstance>(imageName);
    }
    return result;
}

}  // namespace nbd
}  // namespace curve
