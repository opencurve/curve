/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <libconfig.h>
#include <map>
#include <string>
#include <utility>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include "src/part2/rados_interface.h"
#include "src/part2/common_type.h"
#include "src/part2/config.h"

int OpenImage(rados_t* cluster, const char* poolname, const char* volname,
               int fd, char* filename) {
    LOG(NOTICE) << "open image start. " << filename;

    rados_ioctx_t* io = new rados_ioctx_t;
    int ret;
    ret = rados_ioctx_create(*cluster, poolname, io);
    if (ret < 0) {
        LOG(ERROR) << "create ioctx failed. " << poolname << ", " << volname;
        delete filename;
        return ret;
    }
    rbd_image_t* image = new rbd_image_t;

    ret = rbd_open(*io, volname, image, NULL);
    if (ret < 0) {
        LOG(ERROR) << "open image failed.";
        delete image;
        delete filename;
        rados_ioctx_destroy(*io);
        return ret;
    }

    if (fd <= 0) {
        while (true) {
            srand((unsigned) time(NULL));
            unsigned int seed;
            fd = rand_r(&seed) % FD_CEPH_MAX;
            if (fd == 0) fd++;
            LOG(NOTICE) << "fd is: " << fd;
            if (!g_imageMap.count(fd)) {
                break;
            }
        }
    }
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    LOG(NOTICE) << "open image success.";
    return fd;
}

//关闭卷。该函数的调用需加文件锁保护
int CloseImage(int fd) {
    rbd_image_t* image;
    std::map<int, FdImage_t*>::iterator iter;
    iter = g_imageMap.find(fd);

    if (iter == g_imageMap.end()) {
        LOG(ERROR) << "close image failed, because fd is not exist, fd is: "
                   << fd;
        return -1;
    }
    LOG(NOTICE) << "fd is exist, fd is: " << fd;
    image = iter->second->image;

    rados_ioctx_t* io;
    rados_t* cluster;
    io = iter->second->io;
    cluster = iter->second->cluster;
    delete (iter->second->filename);
    rbd_close(*image);
    rados_ioctx_destroy(*io);
    CloseRados(cluster);
    // 删除fd内存镜像
    g_imageMap.erase(iter);

    LOG(NOTICE) << "close image success. fd is: " << fd;

    return 0;
}

rados_t* ConnectRados(const char* mon_host) {
    LOG(NOTICE) << "connect rados start. " << mon_host;
    int ret;
    rados_t* cluster = new rados_t;
    ret = rados_create(cluster, "admin");
    if (ret < 0) {
        delete cluster;
        LOG(ERROR) << "create cluster failed.";
        return NULL;
    }
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return NULL;
    }
    std::string ceph_conf = ReadCephConf(cfg);
    config_destroy(cfg);
    ret = rados_conf_read_file(*cluster, ceph_conf.c_str());
    if (ret < 0) {
        LOG(ERROR) << "read config file failed.";
    }
    ret = rados_conf_set(*cluster, "mon_host", mon_host);
    if (ret < 0) {
        LOG(ERROR) << "set conf failed.";
        CloseRados(cluster);
        return NULL;
    }
    ret = rados_connect(*cluster);
    if (ret < 0) {
        LOG(ERROR) << "connect rados failed.";
        CloseRados(cluster);
        return NULL;
    }

    LOG(NOTICE) << "connect rados success.";
    return cluster;
}

void CloseRados(rados_t* cluster) {
    rados_shutdown(*cluster);
    delete cluster;
}

int FilenameFdExist(char* filename) {
    std::map<int, FdImage_t*>::iterator iter;

    for (iter = g_imageMap.begin(); iter != g_imageMap.end(); iter++) {
        LOG(NOTICE) << " fd "
                    << "filename is:  " << iter->second->filename << "  "
                    << filename;
        if (strcmp(filename, iter->second->filename) == 0) {
            LOG(NOTICE) << " fd exist."
                        << "filename is:  " << iter->second->filename
                        << ", fd is: " << iter->first;
            return iter->first;
        }
    }
    LOG(NOTICE) << " fd is not exist. "
                << "filename is:  " << filename;
    return 0;
}

bool FdExist(int fd, rbd_image_t** image) {
    std::map<int, FdImage_t*>::iterator iter;
    for (iter = g_imageMap.begin(); iter != g_imageMap.end(); iter++) {
        if (iter->first == fd) {
            LOG(INFO) << "fd is exist: " << fd;
            *image = iter->second->image;
            return true;
        }
    }

    LOG(NOTICE) << "fd is not exist: " << fd;
    return false;
}
