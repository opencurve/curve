/**
 * Project: curve
 * Date: Wed Apr 29 14:31:07 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#include <sys/wait.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "nbd/src/ImageInstance.h"
#include "nbd/src/NBDController.h"
#include "nbd/src/NBDServer.h"
#include "nbd/src/NBDTool.h"
#include "nbd/src/NBDWatchContext.h"
#include "nbd/src/argparse.h"
#include "nbd/src/define.h"
#include "nbd/src/util.h"

namespace curve {
namespace nbd {

std::shared_ptr<NBDTool> nbdTool;
std::shared_ptr<NBDConfig> nbdConfig;

static std::string Version() {
    static const std::string version =
#ifdef CURVEVERSION
#define STR(val) #val
#define XSTR(val) STR(val)
        std::string(XSTR(CURVEVERSION));
#else
        std::string("unknown");
#endif
    return version;
}

static void HandleSignal(int signum) {
    int ret = 0;

    if (signum != SIGINT && signum != SIGTERM) {
        std::cerr << "Catch unexpected signal : " << signum;
        return;
    }

    std::cout << "Got signal " << sys_siglist[signum] << "\n"
              << ", disconnect now" << std::endl;

    ret = nbdTool->Disconnect(nbdConfig->devpath);
    if (ret != 0) {
        std::cout << "curve-nbd: disconnect failed. Error: " << ret
                  << std::endl;
    } else {
        std::cout << "curve-nbd: disconnected";
    }
}

static void Usage() {
    std::cout
        << "Usage: curve-nbd [options] map <image>  Map an image "
           "to nbd device\n"  // NOLINT
        << "                 unmap <device|image>   Unmap nbd "
           "device\n"  // NOLINT
        << "                 [options] list-mapped  List mapped "
           "nbd devices\n"  // NOLINT
        << "Map options:\n"
        << "  --device <device path>  Specify nbd device path (/dev/nbd{num})\n"
        << "  --read-only             Map read-only\n"
        << "  --nbds_max <limit>      Override for module param nbds_max\n"
        << "  --max_part <limit>      Override for module param max_part\n"
        << "  --timeout <seconds>     Set nbd request timeout\n"
        << "  --try-netlink           Use the nbd netlink interface\n"
        << std::endl;
}

static int NBDConnect() {
    int waitConnectPipe[2];

    if (0 != pipe(waitConnectPipe)) {
        std::cout << "create pipe failed";
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        std::cout << "fork failed, " << cpp_strerror(errno);
        return -1;
    }

    if (pid > 0) {
        int connectRes = -1;
        int nr = read(waitConnectPipe[0], &connectRes, sizeof(connectRes));
        if (nr != sizeof(connectRes)) {
            std::cout << "Read from child failed, " << cpp_strerror(errno)
                      << ", nr = " << nr
                      << std::endl;
        }

        if (connectRes != 0) {
            // wait child process exit
            wait(nullptr);
        }

        return connectRes == 0 ? 0 : -1;
    }

    // in child
    setsid();
    chdir("/");
    umask(0);

    // set signal handler
    signal(SIGTERM, HandleSignal);
    signal(SIGINT, HandleSignal);

    int ret = nbdTool->Connect(nbdConfig.get());
    int connectionRes = -1;
    if (ret < 0) {
        ::write(waitConnectPipe[1], &connectionRes, sizeof(connectionRes));
    } else {
        connectionRes = 0;
        ::write(waitConnectPipe[1], &connectionRes, sizeof(connectionRes));
        nbdTool->RunServerUntilQuit();
    }

    ::exit(ret);
}

static int CurveNbdMain(int argc, const char* argv[]) {
    int r = 0;
    Command command;
    std::ostringstream errMsg;
    std::vector<const char*> args;

    nbdConfig = std::make_shared<NBDConfig>();
    nbdTool = std::make_shared<NBDTool>();

    argv_to_vec(argc, argv, args);
    r = parse_args(args, &errMsg, &command, nbdConfig.get());

    if (r == HELP_INFO) {
        Usage();
        return 0;
    } else if (r == VERSION_INFO) {
        std::cout << "curve-nbd version : " << Version() << std::endl;
        return 0;
    } else if (r < 0) {
        std::cerr << errMsg.str() << std::endl;
        return r;
    }

    switch (command) {
        case Command::Connect: {
            if (nbdConfig->imgname.empty()) {
                std::cerr << "curve-nbd: image name was not specified"
                          << std::endl;
                return -EINVAL;
            }

            r = NBDConnect();
            if (r < 0) {
                return -EINVAL;
            }

            break;
        }
        case Command::Disconnect: {
            r = nbdTool->Disconnect(nbdConfig->devpath);
            if (r < 0) {
                return -EINVAL;
            }

            break;
        }
        case Command::List: {
            std::vector<DeviceInfo> devices;
            nbdTool->List(&devices);
            for (const auto& dev : devices) {
                std::cout << dev << std::endl;
            }

            break;
        }
        case Command::None:
        default: {
            Usage();
            break;
        }
    }

    return 0;
}

}  // namespace nbd
}  // namespace curve

int main(int argc, const char* argv[]) {
    int r = curve::nbd::CurveNbdMain(argc, argv);
    if (r < 0) {
        return EXIT_FAILURE;
    }

    return 0;
}
