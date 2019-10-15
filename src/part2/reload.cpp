/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include "src/part2/reload.h"
#include <string>
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include "src/part2/rados_interface.h"

/**
 * @brief 从持久化文件中加载ceph卷
 */
int ReloadCephVolume(int fd, char* filename) {
    LOG(NOTICE) << "reload ceph volume start, " << filename;
    rados_t* cluster = NULL;
    std::vector<std::string> split_firstly = split(filename, "=");
    if (split_firstly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }

    std::vector<std::string> split_secondly = split(split_firstly[0], ":");
    if (split_secondly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }

    std::vector<std::string> split_thirdly = split(split_secondly[1], "/");
    if (split_thirdly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }
    char* imagename = const_cast<char*>(split_thirdly[1].c_str());
    char* poolname = const_cast<char*>(split_thirdly[0].c_str());
    std::string mon_host = GetMonHost(filename);
    if (mon_host.empty()) {
        LOG(ERROR) << "mon host is null.";
        delete filename;
        return -1;
    }

    cluster = ConnectRados(mon_host.c_str());
    if (cluster == NULL) {
        LOG(ERROR) << "connect rados failed. " << mon_host;
        delete filename;
        return -1;
    }
    int ret = OpenImage(cluster, poolname, imagename, fd, filename);
    if (ret < 0) {
        LOG(ERROR) << "open image failed. " << poolname << ", " << imagename;
        CloseRados(cluster);
        delete filename;
        return ret;
    }

    LOG(NOTICE) << "reload vol success, " << poolname << ", " << imagename;
    return 0;
}

/**
 * @brief 从持久化文件中加载卷
 */
int Reload() {
    LOG(NOTICE) << "reload start.";
    std::string metadata_file = GetUuidFile();
    boost::property_tree::ptree root, items;
    try {
        boost::property_tree::read_json<
             boost::property_tree::ptree>(metadata_file, root);
    }
    catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "read json throw an exception. " << pt.what();
        return -1;
    }

    // 获取port
    auto port_exist = root.get_child_optional("port");
    if (!port_exist) {
        LOG(ERROR) << "port is not exist.";
        return -1;
    }
    int port = root.get<int>("port", 0);

    auto volumes_exist = root.get_child_optional("volumes");
    if (!volumes_exist) {
        return port;
    }

    // 遍历所有的volumes
    items = root.get_child("volumes");
    for (boost::property_tree::ptree::iterator it = items.begin();
        it != items.end(); ++it) {
        int fd = atoi(it->second.get<std::string>("fd").c_str());
        char* filename_tmp = NULL;
        filename_tmp =
            const_cast<char*>(it->second.get<std::string>("filename").c_str());
        if (filename_tmp == NULL) {
            LOG(ERROR) << "filename is null.";
            continue;
        }
        char* filename = new char[strlen(filename_tmp) + 1]();
        snprintf(filename, strlen(filename_tmp) + 1, "%s", filename_tmp);

        if (fd <= FD_CEPH_MAX) {
            ReloadCephVolume(fd, filename);
        }
    }

    LOG(NOTICE) << "reload end.";
    return port;
}
