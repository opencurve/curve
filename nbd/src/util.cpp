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
 * Created Date: Thursday April 23rd 2020
 * Author: yangyaokai
 */

/*
 * rbd-nbd - RBD in userspace
 *
 * Copyright (C) 2015 - 2016 Kylin Corporation
 *
 * Author: Yunchuan Wen <yunchuan.wen@kylin-cloud.com>
 *         Li Wang <li.wang@kylin-cloud.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <linux/nbd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <libgen.h>
#include <string.h>
#include <sstream>
#include "nbd/src/define.h"
#include "nbd/src/util.h"
#include "nbd/src/argparse.h"

namespace curve {
namespace nbd {

constexpr uint32_t kSectorSize = 512;

std::string cpp_strerror(int err) {
    char buf[128];
    if (err < 0)
        err = -err;
    std::ostringstream oss;
    oss << strerror_r(err, buf, sizeof(buf));
    return oss.str();
}

int parse_nbd_index(const std::string& devpath) {
    int index, ret;

    ret = sscanf(devpath.c_str(), "/dev/nbd%d", &index);
    if (ret <= 0) {
        // mean an early matching failure. But some cases need a negative value.
        if (ret == 0) {
            ret = -EINVAL;
        }
        cerr << "curve-nbd: invalid device path: " <<  devpath
             << " (expected /dev/nbd{num})" << std::endl;
        return ret;
    }

    return index;
}

int get_nbd_max_count() {
    int nbds_max = -1;
    if (access(NBD_MAX_PATH, F_OK) == 0) {
        std::ifstream ifs;
        ifs.open(NBD_MAX_PATH, std::ifstream::in);
        if (ifs.is_open()) {
            ifs >> nbds_max;
            ifs.close();
        }
    }
    return nbds_max;
}

static bool find_mapped_dev_by_spec(NBDConfig *cfg) {
    int pid;
    NBDConfig c;
    NBDListIterator it;
    while (it.Get(&pid, &c)) {
        if (c.imgname == cfg->imgname) {
            *cfg = c;
            return true;
        }
    }
    return false;
}

static int parse_imgpath(const std::string &imgpath, NBDConfig *cfg,
                         std::ostream *err_msg) {
    return 0;
}

// TODO(all): replace this function with gflags
//            but, currently, gflags `--help` command is messy
int parse_args(std::vector<const char*>& args, std::ostream *err_msg,   // NOLINT
               Command *command, NBDConfig *cfg) {
    std::vector<const char*>::iterator i;
    std::ostringstream err;
    for (i = args.begin(); i != args.end(); ) {
        if (argparse_flag(args, i, "-h", "--help", (char*)NULL)) {  // NOLINT
            return HELP_INFO;
        } else if (argparse_flag(args, i, "-v", "--version", (char*)NULL)) {    // NOLINT
            return VERSION_INFO;
        } else if (argparse_witharg(args, i, &cfg->devpath, err,
                                    "--device", (char *)NULL)) {    // NOLINT
        } else if (argparse_witharg(args, i, &cfg->nbds_max, err,
                                    "--nbds_max", (char *)NULL)) {  // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
            if (cfg->nbds_max < 0) {
                *err_msg << "curve-nbd: Invalid argument for nbds_max!";
                return -EINVAL;
            }
        } else if (argparse_witharg(args, i, &cfg->max_part, err,
                                    "--max_part", (char *)NULL)) {  // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
            if ((cfg->max_part < 0) || (cfg->max_part > 255)) {
                *err_msg << "curve-nbd: Invalid argument for max_part(0~255)!";
                return -EINVAL;
            }
            cfg->set_max_part = true;
        } else if (argparse_flag(args, i, "--read-only", (char *)NULL)) {   // NOLINT
            cfg->readonly = true;
        } else if (argparse_witharg(args, i, &cfg->timeout, err,
                                    "--timeout", (char *)NULL)) {   // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
            if (cfg->timeout < 0) {
                *err_msg << "curve-nbd: Invalid argument for timeout!";
                return -EINVAL;
            }
        } else if (argparse_flag(args, i, "--try-netlink", (char *)NULL)) { // NOLINT
            cfg->try_netlink = true;
        } else if (argparse_flag(args, i, "-f", "--force", (char *)NULL)) {  // NOLINT
            cfg->force_unmap = true;
        } else if (argparse_flag(args, i, "--no-exclusive", (char*)NULL)) {   // NOLINT
            cfg->exclusive = false;
        }else if (argparse_witharg(args, i, &cfg->retry_times, err, "--retry_times", (char*)(NULL))) {  // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
            if (cfg->retry_times < 0) {
                *err_msg << "curve-nbd: Invalid argument for retry_times!";
                return -EINVAL;
            }
        } else if (argparse_witharg(args, i, &cfg->sleep_ms, err, "--sleep_ms", (char*)(NULL))) {  // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
            if (cfg->sleep_ms < 0) {
                *err_msg << "curve-nbd: Invalid argument for sleep_ms!";
                return -EINVAL;
            }
        } else if (argparse_witharg(args, i, &cfg->nebd_conf, err, "--nebd-conf", (char*)(NULL))) {  // NOLINT
            if (!err.str().empty()) {
                *err_msg << "curve-nbd: " << err.str();
                return -EINVAL;
            }
        } else {
            ++i;
        }
    }

    Command cmd = Command::None;
    if (args.begin() != args.end()) {
        if (strcmp(*args.begin(), "map") == 0) {
            cmd = Command::Connect;
        } else if (strcmp(*args.begin(), "unmap") == 0) {
            cmd = Command::Disconnect;
        } else if (strcmp(*args.begin(), "list-mapped") == 0) {
            cmd = Command::List;
        } else {
            *err_msg << "curve-nbd: unknown command: " <<  *args.begin();
            return -EINVAL;
        }
        args.erase(args.begin());
    }

    if (cmd == Command::None) {
        *err_msg << "curve-nbd: must specify command";
        return -EINVAL;
    }

    switch (cmd) {
        case Command::Connect:
            if (args.begin() == args.end()) {
                *err_msg << "curve-nbd: must specify image spec";
                return -EINVAL;
            }
            cfg->imgname = *args.begin();
            args.erase(args.begin());
            break;
        case Command::Disconnect:
            if (args.begin() == args.end()) {
                *err_msg << "curve-nbd: must specify nbd device or image spec";
                return -EINVAL;
            }
            if (strncmp(*args.begin(), "/dev/", 4) == 0) {
                cfg->devpath = *args.begin();
            } else {
                cfg->imgname = *args.begin();
                if (!find_mapped_dev_by_spec(cfg)) {
                    *err_msg << "curve-nbd: " << *args.begin()
                             << " is not mapped";
                    return -ENOENT;
                }
            }
            args.erase(args.begin());
            break;
        default:
            // shut up gcc;
            break;
    }

    if (args.begin() != args.end()) {
        *err_msg << "curve-nbd: unknown args: " << *args.begin();
        return -EINVAL;
    }

    *command = cmd;
    return 0;
}

int get_mapped_info(int pid, NBDConfig *cfg) {
    int r;
    std::string path = "/proc/" + std::to_string(pid) + "/cmdline";
    std::ifstream ifs;
    std::string cmdline;
    std::vector<const char*> args;

    ifs.open(path.c_str(), std::ifstream::in);
    if (!ifs.is_open()) {
        return -1;
    }
    ifs >> cmdline;

    for (int i = 0; i < cmdline.size(); i++) {
        char *arg = &cmdline[i];
        if (i == 0) {
            if (strcmp(basename(arg) , PROCESS_NAME) != 0) {
                return -EINVAL;
            }
        } else {
            args.push_back(arg);
        }

        while (cmdline[i] != '\0') {
            i++;
        }
    }

    std::ostringstream err_msg;
    Command command;
    r = parse_args(args, &err_msg, &command, cfg);
    if (r < 0) {
        return r;
    }

    if (command != Command::Connect) {
        return -ENOENT;
    }

    return 0;
}

int check_dev_can_unmap(const NBDConfig *cfg) {
    std::ifstream ifs("/proc/mounts", std::ifstream::in);

    if (!ifs.is_open()) {
        cerr << "curve-nbd: failed to open /proc/mounts" << std::endl;
        return -EINVAL;
    }

    std::string line, device, mountPath;
    bool mounted = false;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        iss >> device >> mountPath;
        if (device == cfg->devpath) {
            mounted = true;
            break;
        }
    }

    if (!mounted) {
        return 0;
    } else if (cfg->force_unmap) {
        cerr << "curve-nbd: the " << device << " is still mount on "
             << mountPath << ", force unmap it" << std::endl;
        return 0;
    }

    cerr << "curve-nbd: the " << device << " is still mount on " << mountPath
         << ", you can't unmap it or specify -f parameter" << std::endl;
    return -EINVAL;
}

int check_size_from_file(const std::string &path, uint64_t expected_size,
                         bool sizeInSector = false) {
    std::ifstream ifs;
    ifs.open(path.c_str(), std::ifstream::in);
    if (!ifs.is_open()) {
        cerr << "curve-nbd: failed to open " << path << std::endl;
        return -EINVAL;
    }

    uint64_t size = 0;
    ifs >> size;
    size *= CURVE_NBD_BLKSIZE;

    if (size == 0) {
        // Newer kernel versions will report real size only after nbd
        // connect. Assume this is the case and return success.
        return 0;
    }

    if (sizeInSector) {
        size *= kSectorSize;
    }

    if (size != expected_size) {
        cerr << "curve-nbd: kernel reported invalid size (" << size
            << ", expected " << expected_size << ")" << std::endl;
        return -EINVAL;
    }

    return 0;
}

int check_block_size(int nbd_index, uint64_t expected_size) {
    std::string path = "/sys/block/nbd" + std::to_string(nbd_index)
                     + "/queue/hw_sector_size";
    int ret = check_size_from_file(path, expected_size);
    if (ret < 0) {
        return ret;
    }
    path = "/sys/block/nbd" + std::to_string(nbd_index)
         + "/queue/minimum_io_size";
    ret = check_size_from_file(path, expected_size);
    if (ret < 0) {
        return ret;
    }
    return 0;
}

int check_device_size(int nbd_index, uint64_t expected_size) {
    // There are bugs with some older kernel versions that result in an
    // overflow for large image sizes. This check is to ensure we are
    // not affected.

    std::string path = "/sys/block/nbd" + std::to_string(nbd_index) + "/size";
    return check_size_from_file(path, expected_size, true);
}

static int run_command(const char *command) {
    int status;

    status = system(command);
    if (status >= 0 && WIFEXITED(status))
        return WEXITSTATUS(status);

    if (status < 0) {
        char error_buf[80];
        strerror_r(errno, error_buf, sizeof(error_buf));
        fprintf(stderr, "couldn't run '%s': %s\n", command,
            error_buf);
    } else if (WIFSIGNALED(status)) {
        fprintf(stderr, "'%s' killed by signal %d\n", command,
            WTERMSIG(status));
    } else {
        fprintf(stderr, "weird status from '%s': %d\n", command,
            status);
    }

    return -1;
}

static int module_load(const char *module, const char *options) {
    char command[128];

    snprintf(command, sizeof(command), "/sbin/modprobe %s %s",
             module, (options ? options : ""));

    return run_command(command);
}

int load_module(NBDConfig *cfg) {
    std::ostringstream param;
    int ret;

    if (cfg->nbds_max) {
        param << "nbds_max=" << cfg->nbds_max;
    }

    if (cfg->max_part) {
        param << " max_part=" << cfg->max_part;
    }

    if (!access("/sys/module/nbd", F_OK)) {
        if (cfg->nbds_max || cfg->set_max_part) {
            cerr << "curve-nbd: ignoring kernel module parameter options:"
                 << " nbd module already loaded. "
                 << "nbds_max: " << cfg->nbds_max
                 << ", set_max_part: " << cfg->set_max_part << std::endl;
        }
        return 0;
    }

    ret = module_load("nbd", param.str().c_str());
    if (ret < 0) {
        cerr << "curve-nbd: failed to load nbd kernel module: "
             << cpp_strerror(-ret) << std::endl;
    }

    return ret;
}

bool NBDListIterator::Get(int *pid, NBDConfig *cfg) {
    while (true) {
        std::string nbd_path = NBD_PATH_PREFIX + std::to_string(curIndex_);
        if (access(nbd_path.c_str(), F_OK) != 0) {
            return false;
        }

        *cfg = NBDConfig();
        cfg->devpath = DEV_PATH_PREFIX + std::to_string(curIndex_++);

        std::ifstream ifs;
        ifs.open(nbd_path + "/pid", std::ifstream::in);
        if (!ifs.is_open()) {
            continue;
        }
        ifs >> *pid;

        int r = get_mapped_info(*pid, cfg);
        if (r < 0) {
            continue;
        }

        return true;
    }
}


ssize_t safe_read(int fd, void* buf, size_t count) {
    size_t cnt = 0;

    while (cnt < count) {
        ssize_t r = read(fd, buf, count - cnt);
        if (r <= 0) {
            if (r == 0) {  // EOF
                return cnt;
            }
            if (errno == EINTR) {
                continue;
            }
            return -errno;
        }
        cnt += r;
        buf = (char*)buf + r;  // NOLINT
    }
    return cnt;
}

ssize_t safe_read_exact(int fd, void* buf, size_t count) {
    ssize_t ret = safe_read(fd, buf, count);
    if (ret < 0) {
        return ret;
    }
    if (static_cast<size_t>(ret) != count) {
        return -EDOM;
    }
    return 0;
}

ssize_t safe_write(int fd, const void* buf, size_t count) {
    while (count > 0) {
        ssize_t r = write(fd, buf, count);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -errno;
        }
        count -= r;
        buf = (char*)buf + r;  // NOLINT
    }
    return 0;
}

}  // namespace nbd
}  // namespace curve
