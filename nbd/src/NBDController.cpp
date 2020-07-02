/*
 * Project: curve
 * Created Date: Tuesday April 21st 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include "nbd/src/NBDController.h"

namespace curve {
namespace nbd {

int IOController::InitDevAttr(int devfd, NBDConfig* config, int sockfd,
                              uint64_t size, uint64_t flags) {
    int ret = ioctl(devfd, NBD_SET_SOCK, sockfd);
    if (ret < 0) {
        cerr << "curve-ndb: the device " << config->devpath
             << " is busy" << std::endl;
        return -errno;
    }

    do {
        ret = ioctl(devfd, NBD_SET_BLKSIZE, CURVE_NBD_BLKSIZE);
        if (ret < 0) {
            break;
        }

        ret = ioctl(devfd, NBD_SET_SIZE, size);
        if (ret < 0) {
            break;
        }

        ioctl(devfd, NBD_SET_FLAGS, flags);

        ret = CheckSetReadOnly(devfd, flags);
        if (ret < 0) {
            cerr << "curve-nbd: Check and set read only flag failed."
                 << cpp_strerror(ret) << std::endl;
            break;
        }

        if (config->timeout >= 0) {
            ret = ioctl(devfd, NBD_SET_TIMEOUT, (unsigned long)config->timeout);  // NOLINT
            if (ret < 0) {
                cerr << "curve-ndb: failed to set timeout: "
                     << cpp_strerror(ret) << std::endl;
                break;
            }
        }
    } while (false);

    if (ret < 0) {
        ret = -errno;
        ioctl(devfd, NBD_CLEAR_SOCK);
    }
    return ret;
}

int IOController::SetUp(NBDConfig* config, int sockfd,
                        uint64_t size, uint64_t flags) {
    if (config->devpath.empty()) {
        config->devpath = find_unused_nbd_device();
    }

    if (config->devpath.empty()) {
        return -1;
    }

    int ret = parse_nbd_index(config->devpath);
    if (ret < 0) {
        return ret;
    }
    int index = ret;

    ret = open(config->devpath.c_str(), O_RDWR);
    if (ret < 0) {
        cerr << "curve-ndb: failed to open device: "
             << config->devpath << std::endl;
        return ret;
    }
    int devfd = ret;

    ret = InitDevAttr(devfd, config, sockfd, size, flags);
    if (ret == 0) {
        ret = check_device_size(index, size);
    }
    if (ret < 0) {
        cerr << "curve-ndb: failed to map, status: "
             << cpp_strerror(ret) << std::endl;
        close(devfd);
        return ret;
    }

    nbdFd_ = devfd;
    nbdIndex_ = index;
    return 0;
}

int IOController::DisconnectByPath(const std::string& devpath) {
    int devfd = open(devpath.c_str(), O_RDWR);
    if (devfd < 0) {
        cerr << "curve-ndb: failed to open device: "
             << devpath << ", error = " << cpp_strerror(errno) << std::endl;
        return devfd;
    }

    int ret = ioctl(devfd, NBD_DISCONNECT);
    if (ret < 0) {
        cerr << "curve-ndb: the device is not used. "
             << cpp_strerror(errno) << std::endl;
    }

    close(devfd);
    return ret;
}

int IOController::Resize(uint64_t size) {
    if (nbdFd_ < 0) {
        cerr << "resize failed: nbd controller is not setup." << std::endl;
        return -1;
    }
    int ret = ioctl(nbdFd_, NBD_SET_SIZE, size);
    if (ret < 0) {
        cerr << "resize failed: " << cpp_strerror(errno) << std::endl;
    }
    return ret;
}

int NetLinkController::Init() {
    if (sock_ != nullptr) {
        cerr << "curve-nbd: Could not allocate netlink socket." << std::endl;
        return 0;
    }

    struct nl_sock* sock = nl_socket_alloc();
    if (sock == nullptr) {
        cerr << "curve-nbd: Could not alloc netlink socket. Error "
             << cpp_strerror(errno) << std::endl;
        return -1;
    }

    int ret = genl_connect(sock);
    if (ret < 0) {
        cerr << "curve-nbd: Could not connect netlink socket. Error "
             << nl_geterror(ret) << std::endl;
        nl_socket_free(sock);
        return -1;
    }

    nlId_ = genl_ctrl_resolve(sock, "nbd");
    if (nlId_ < 0) {
        cerr << "curve-nbd: Could not resolve netlink socket. Error "
             << nl_geterror(nlId_) << std::endl;
        nl_close(sock);
        nl_socket_free(sock);
        return -1;
    }
    sock_ = sock;
    return 0;
}

void NetLinkController::Uninit() {
    if (sock_ == nullptr)
        return;

    nl_close(sock_);
    nl_socket_free(sock_);
    sock_ = nullptr;
    nlId_ = -1;
}

int NetLinkController::SetUp(NBDConfig* config, int sockfd,
                             uint64_t size, uint64_t flags) {
    int ret = Init();
    if (ret < 0) {
        cerr << "curve-nbd: Netlink interface not supported."
             << " Using ioctl interface." << std::endl;
        return ret;
    }

    ret = ConnectInternal(config, sockfd, size, flags);
    Uninit();
    if (ret < 0) {
        return ret;
    }

    int index = parse_nbd_index(config->devpath);
    if (index < 0) {
        return index;
    }
    ret = check_block_size(index, CURVE_NBD_BLKSIZE);
    if (ret < 0) {
        return ret;
    }
    ret = check_device_size(index, size);
    if (ret < 0) {
        return ret;
    }

    int fd = open(config->devpath.c_str(), O_RDWR);
    if (fd < 0) {
        cerr << "curve-nbd: failed to open device: "
             << config->devpath << std::endl;
        return fd;
    }

    ret = CheckSetReadOnly(fd, flags);
    if (ret < 0) {
        cerr << "curve-nbd: Check and set read only flag failed."
             << std::endl;
        close(fd);
        return ret;
    }

    nbdFd_ = fd;
    nbdIndex_ = index;
    return 0;
}

int NetLinkController::DisconnectByPath(const std::string& devpath) {
    int index = parse_nbd_index(devpath);
    if (index < 0) {
        return index;
    }

    int ret = Init();
    if (ret < 0) {
        cerr << "curve-nbd: Netlink interface not supported."
             << " Using ioctl interface." << std::endl;
        return ret;
    }

    ret = DisconnectInternal(index);
    Uninit();
    return ret;
}

int NetLinkController::Resize(uint64_t size) {
    if (nbdIndex_ < 0) {
        cerr << "resize failed: nbd controller is not setup." << std::endl;
        return -1;
    }

    int ret = Init();
    if (ret < 0) {
        cerr << "curve-nbd: Netlink interface not supported."
             << " Using ioctl interface." << std::endl;
        return ret;
    }

    ret = ResizeInternal(nbdIndex_, size);
    Uninit();
    return ret;
}

bool NetLinkController::Support() {
    int ret = Init();
    if (ret < 0) {
        cerr << "curve-nbd: Netlink interface not supported."
             << " Using ioctl interface." << std::endl;
        return false;
    }
    Uninit();
    return true;
}

static int netlink_connect_cb(struct nl_msg *msg, void *arg) {
    struct genlmsghdr *gnlh = (struct genlmsghdr *)nlmsg_data(nlmsg_hdr(msg));
    NBDConfig *cfg = reinterpret_cast<NBDConfig *>(arg);
    struct nlattr *msg_attr[NBD_ATTR_MAX + 1];
    int ret;

    ret = nla_parse(msg_attr, NBD_ATTR_MAX, genlmsg_attrdata(gnlh, 0),
                    genlmsg_attrlen(gnlh, 0), NULL);
    if (ret) {
        cerr << "curve-nbd: Unsupported netlink reply" << std::endl;
        return -NLE_MSGTYPE_NOSUPPORT;
    }

    if (!msg_attr[NBD_ATTR_INDEX]) {
        cerr << "curve-nbd: netlink connect reply missing device index."
             << std::endl;
        return -NLE_MSGTYPE_NOSUPPORT;
    }

    uint32_t index = nla_get_u32(msg_attr[NBD_ATTR_INDEX]);
    cfg->devpath = "/dev/nbd" + std::to_string(index);

    return NL_OK;
}

int NetLinkController::ConnectInternal(NBDConfig* config, int sockfd,
                                       uint64_t size, uint64_t flags) {
    struct nlattr *sock_attr = nullptr;
    struct nlattr *sock_opt = nullptr;
    struct nl_msg *msg = nullptr;
    int ret;

    nl_socket_modify_cb(sock_, NL_CB_VALID, NL_CB_CUSTOM,
                        netlink_connect_cb, config);
    msg = nlmsg_alloc();
    if (msg == nullptr) {
        cerr << "curve-nbd: Could not allocate netlink message." << std::endl;
        return -ENOMEM;
    }

    auto user_hdr = genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ,
                                nlId_, 0, 0, NBD_CMD_CONNECT, 0);
    if (user_hdr == nullptr) {
        cerr << "curve-nbd: Could not setup message." << std::endl;
        goto nla_put_failure;
    }

    if (!config->devpath.empty()) {
        int index = parse_nbd_index(config->devpath);
        if (index < 0) {
            goto nla_put_failure;
        }
        NLA_PUT_U32(msg, NBD_ATTR_INDEX, index);
    }
    if (config->timeout >= 0) {
        NLA_PUT_U64(msg, NBD_ATTR_TIMEOUT, config->timeout);
    }
    NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, size);
    NLA_PUT_U64(msg, NBD_ATTR_BLOCK_SIZE_BYTES, CURVE_NBD_BLKSIZE);
    NLA_PUT_U64(msg, NBD_ATTR_SERVER_FLAGS, flags);

    sock_attr = nla_nest_start(msg, NBD_ATTR_SOCKETS);
    if (sock_attr == nullptr) {
        cerr << "curve-nbd: Could not init sockets in netlink message."
             << std::endl;
        goto nla_put_failure;
    }

    sock_opt = nla_nest_start(msg, NBD_SOCK_ITEM);
    if (sock_opt == nullptr) {
        cerr << "curve-nbd: Could not init sock in netlink message."
             << std::endl;
        goto nla_put_failure;
    }

    NLA_PUT_U32(msg, NBD_SOCK_FD, sockfd);
    nla_nest_end(msg, sock_opt);
    nla_nest_end(msg, sock_attr);

    ret = nl_send_sync(sock_, msg);
    if (ret < 0) {
        cerr << "curve-nbd: netlink connect failed: " << nl_geterror(ret)
             << std::endl;
        return -EIO;
    }
    return 0;

nla_put_failure:
    nlmsg_free(msg);
    return -EIO;
}

int NetLinkController::DisconnectInternal(int index) {
    struct nl_msg *msg = nullptr;
    int ret;

    nl_socket_modify_cb(sock_, NL_CB_VALID, NL_CB_CUSTOM,
                        genl_handle_msg, NULL);
    msg = nlmsg_alloc();
    if (msg == nullptr) {
        cerr << "curve-ndb: Could not allocate netlink message." << std::endl;
        return -EIO;
    }

    auto user_hdr = genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ,
                                nlId_, 0, 0, NBD_CMD_DISCONNECT, 0);
    if (user_hdr == nullptr) {
        cerr << "curve-nbd: Could not setup message." << std::endl;
        goto nla_put_failure;
    }

    NLA_PUT_U32(msg, NBD_ATTR_INDEX, index);

    ret = nl_send_sync(sock_, msg);
    if (ret < 0) {
        cerr << "curve-ndb: netlink disconnect failed: "
             << nl_geterror(ret) << std::endl;
        return -EIO;
    }

    return 0;

nla_put_failure:
    nlmsg_free(msg);
    return -EIO;
}

int NetLinkController::ResizeInternal(int nbdIndex, uint64_t size) {
    struct nl_msg *msg = nullptr;
    int ret;

    nl_socket_modify_cb(sock_, NL_CB_VALID, NL_CB_CUSTOM,
                        genl_handle_msg, NULL);
    msg = nlmsg_alloc();
    if (msg == nullptr) {
        cerr << "curve-ndb: Could not allocate netlink message." << std::endl;
        return -EIO;
    }

    auto user_hdr = genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ,
                                nlId_, 0, 0, NBD_CMD_RECONFIGURE, 0);
    if (user_hdr == nullptr) {
        cerr << "curve-nbd: Could not setup message." << std::endl;
        goto nla_put_failure;
    }

    NLA_PUT_U32(msg, NBD_ATTR_INDEX, nbdIndex);
    NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, size);

    ret = nl_send_sync(sock_, msg);
    if (ret < 0) {
        cerr << "curve-ndb: netlink resize failed: "
             << nl_geterror(ret) << std::endl;
        return -EIO;
    }

    return 0;

nla_put_failure:
    nlmsg_free(msg);
    return -EIO;
}

}  // namespace nbd
}  // namespace curve
