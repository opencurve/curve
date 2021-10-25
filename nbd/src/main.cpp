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

/**
 * Project: curve
 * Date: Wed Apr 29 14:31:07 CST 2020
 * Author: wuhanqing
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
        dout << "Catch unexpected signal : " << signum;
        return;
    }

    dout << "Got signal " << sys_siglist[signum] << "\n"
              << ", disconnect now" << std::endl;

    ret = nbdTool->Disconnect(nbdConfig.get());
    if (ret != 0) {
        dout << "curve-nbd: disconnect failed. Error: " << ret
                  << std::endl;
    } else {
        dout << "curve-nbd: disconnected";
    }
}

static void Usage() {
    std::cout
        << "Usage: curve-nbd [options] map <image>  Map an image to nbd device\n"  // NOLINT
        << "                 [options] unmap <device|image>   Unmap nbd device\n"  // NOLINT
        << "                 [options] list-mapped  List mapped nbd devices\n"     // NOLINT
        << "Map options:\n"
        << "  --device <device path>  Specify nbd device path (/dev/nbd{num})\n"
        << "  --read-only             Map read-only\n"
        << "  --nbds_max <limit>      Override for module param nbds_max\n"
        << "  --max_part <limit>      Override for module param max_part\n"
        << "  --timeout <seconds>     Set nbd request timeout\n"
        << "  --try-netlink           Use the nbd netlink interface\n"
        << "Unmap options:\n"
        << "  -f, --force                 Force unmap even if the device is mounted\n"              // NOLINT
        << "  --retry_times <limit>       The number of retries waiting for the process to exit\n"  // NOLINT
        << "                              (default: " << nbdConfig->retry_times << ")\n"            // NOLINT
        << "  --sleep_ms <milliseconds>   Retry interval in milliseconds\n"                         // NOLINT
        << "                              (default: " << nbdConfig->sleep_ms << ")\n"               // NOLINT
        << std::endl;
}

// use for record image and device info to auto map when boot
static int AddRecord(int flag) {
    std::string record;
    int fd = open(CURVETAB_PATH, O_WRONLY | O_APPEND);
    if (fd < 0) {
        std::cerr << "curve-nbd: open curvetab file failed.";
        return -EINVAL;
    }
    if (1 == flag) {
        record = "+\t" + nbdConfig->devpath + "\t" + nbdConfig->imgname + "\n";
    } else if (-1 == flag) {
        record = "-\t" + nbdConfig->devpath + "\n";
    }
    auto nr = ::write(fd, record.c_str(), record.size());
    (void)nr;
    close(fd);
    return 0;
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
    auto nr = chdir("/");
    (void)nr;
    umask(0);

    // set signal handler
    signal(SIGTERM, HandleSignal);
    signal(SIGINT, HandleSignal);

    int ret = nbdTool->Connect(nbdConfig.get());
    int connectionRes = -1;
    if (ret < 0) {
        auto nr =
            ::write(waitConnectPipe[1], &connectionRes, sizeof(connectionRes));
        (void)nr;
    } else {
        ret = AddRecord(1);
        if (0 != ret) {
            return ret;
        }
        connectionRes = 0;
        auto nr =
            ::write(waitConnectPipe[1], &connectionRes, sizeof(connectionRes));
        (void)nr;
        nbdTool->RunServerUntilQuit();
    }

    return 0;
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
            r = nbdTool->Disconnect(nbdConfig.get());
            if (r < 0) {
                return -EINVAL;
            }

            r = AddRecord(-1);
            if (0 != r) {
                return r;
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

    nbdTool.reset();
    nbdConfig.reset();

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
