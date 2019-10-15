/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include "src/part2/common_type.h"
#include <brpc/server.h>
#include <butil/logging.h>
#include <dirent.h>
#include <libconfig.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/config.h"
#include "src/part2/heartbeat.h"
#include "src/part2/reload.h"
#include "src/part2/rpc_server.h"

// qemu进程的uuid
char* g_uuid;
// qemu进程的pid
int64_t g_oldPid = -1;
// 该nebd-server程序使用的端口号
int g_port;
// 记录请求数量
std::atomic_long requestForRpcWrite(0);
std::atomic_long requestForCephWrite(0);
std::atomic_long requestForRpcWriteCounts(0);
// uuid持久化文件存放路径
const char* g_filePath;
// fd的内存映像
std::map<int, FdImage_t*> g_imageMap;

// 保存云主机当前挂载的卷信息
std::vector<std::string> g_qemuPoolVolumes;

// 文件锁函数
int LockFile(const char* file) {
    int lockfd = open(file, O_RDONLY | O_CREAT, 0644);
    if (lockfd < 0) {
        LOG(ERROR) << "open lock file failed, " << file;
        return -1;
    }
    int ret = flock(lockfd, LOCK_EX);
    if (ret < 0) {
        LOG(ERROR) << "add lock failed, " << file;
        close(lockfd);
        return -1;
    }
    return lockfd;
}

void UnlockFile(int lockfd) {
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

// 获取所有云主机当前挂载的卷信息
int ReadQemuXmls() {
    struct dirent* ent;
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return -1;
    }
    std::string qemu_xml_dir = ReadQemuXmlDir(cfg);
    config_destroy(cfg);
    DIR* dir = opendir(qemu_xml_dir.c_str());
    if (dir == NULL) {
        LOG(ERROR) << "qemu xml dir open failed, " << qemu_xml_dir;
        return -1;
    }
    while ((ent = readdir(dir))) {
        if ((ent->d_name[0] == '.' && ent->d_name[1] == '\0') ||
            (ent->d_name[1] == '.' && ent->d_name[2] == '\0'))
            continue;

        if (strstr(ent->d_name, ".xml") == NULL) continue;
        std::string qemu_xml_file = qemu_xml_dir + "/" + ent->d_name;
        ReadQemuXml(qemu_xml_file.c_str());
    }
    return 0;
}

// 获取一个云主机挂载的卷信息，存入qemu_xml_map
void ReadQemuXml(const char* xml_file) {
    LOG(NOTICE) << "xml file, " << xml_file;

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::xml_parser::read_xml(xml_file, pt);
    } catch (boost::property_tree::ptree_error pt_error) {
        LOG(ERROR) << "read xml throw an exception. " << pt_error.what();
        return;
    }

    std::string protocol, pool_vol_name;

    auto item = pt.get_child_optional("domstatus.domain.uuid");
    if (!item) {
        LOG(ERROR) << "domstatus.domain.uuid is not exist";
        return;
    }
    item = pt.get_child_optional("domstatus.domain.devices");
    if (!item) {
        LOG(ERROR) << "domstatus.domain.devices is not exist";
        return;
    }

    std::string uuid;
    uuid = pt.get<std::string>("domstatus.domain.uuid");
    if (uuid != g_uuid) {
        LOG(ERROR) << "uuid is not equal domain.uuid, " << g_uuid << ", "
                   << uuid;
        return;
    }

    BOOST_AUTO(child, pt.get_child("domstatus.domain.devices"));

    for (auto iter = child.begin(); iter != child.end(); ++iter) {
        if (iter->first != "disk") {
            continue;
        }
        BOOST_AUTO(pos, iter->second.get_child("source"));
        for (auto nextiter = pos.begin(); nextiter != pos.end(); ++nextiter) {
            if ("<xmlattr>" != nextiter->first) {
                continue;
            }
            protocol = nextiter->second.get<std::string>("protocol");
            pool_vol_name = nextiter->second.get<std::string>("name");

            if (protocol == "rbd") {
                g_qemuPoolVolumes.push_back(pool_vol_name);
            } else {
                LOG(ERROR) << "protocol is: " << protocol
                           << ", pool_vol_name is: " << pool_vol_name;
            }
        }
    }
    for (auto name : g_qemuPoolVolumes) {
        LOG(NOTICE) << "pool and vol is:" << name;
    }
}

// 生成port信息并存入持久化文件
int GeneratePort(int port) {
    std::string metadata_file = GetUuidFile();
    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", port);
        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return -1;
    }

    return 0;
}

// 生成fd信息并存入持久化文件
int GenerateFd(char* filename, int fd) {
    std::string metadata_file = GetUuidFile();
    LOG(NOTICE) << "generate fd start. " << filename << ", " << fd;
    boost::property_tree::ptree root, items;

    try {
        boost::property_tree::read_json<boost::property_tree::ptree>(
            metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "read json throw an exception. " << pt.what();
        return -1;
    }

    // 第一次生成volumes
    auto volumes_exist = root.get_child_optional("volumes");
    if (!volumes_exist) {
        boost::property_tree::ptree item1;
        item1.put("filename", filename);
        item1.put("fd", fd);
        items.push_back(std::make_pair("", item1));
        root.put("port", g_port);
        root.put_child("volumes", items);
        try {
            boost::property_tree::write_json(metadata_file, root);
        } catch (boost::property_tree::ptree_error pt) {
            LOG(ERROR) << "write json throw an exception. " << pt.what();
            return -1;
        }
        return 0;
    }

    items = root.get_child("volumes");
    int count = 0;
    for (boost::property_tree::ptree::iterator it = items.begin();
         it != items.end(); ++it) {
        count++;
    }

    // boost::property_tree::ptree item[count + 1];
    std::vector<boost::property_tree::ptree> item;
    item.resize(count + 1);
    int item_index = 0;
    for (boost::property_tree::ptree::iterator it = items.begin();
         it != items.end(); ++it) {
        std::string filename_json = it->second.get<std::string>("filename");
        std::string fd_json = it->second.get<std::string>("fd");
        item[item_index].put("filename", filename_json);
        item[item_index].put("fd", fd_json);
        item_index++;
    }

    item[count].put("filename", filename);
    item[count].put("fd", fd);

    boost::property_tree::ptree items_write;
    for (int i = 0; i <= count; i++) {
        items_write.push_back(std::make_pair("", item[i]));
    }

    root.put("port", g_port);
    root.put_child("volumes", items_write);

    try {
        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return -1;
    }
    LOG(NOTICE) << "generate fd end. " << filename << ", " << fd;
    return 0;
}

// 从持久化文件删除fd相关信息
int RmFd(int fd) {
    std::string metadata_file = GetUuidFile();
    boost::property_tree::ptree root, items;

    try {
        boost::property_tree::read_json<boost::property_tree::ptree>(
            metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "read json throw an exception. " << pt.what();
        return -1;
    }

    auto volumes_exist = root.get_child_optional("volumes");
    if (!volumes_exist) {
        LOG(ERROR) << "rm fd failed, no volume is exist. fd is: " << fd;
        return -1;
    }

    items = root.get_child("volumes");

    std::vector<boost::property_tree::ptree> vector_items;

    bool FdExist = false;
    for (boost::property_tree::ptree::iterator it = items.begin();
         it != items.end(); ++it) {
        std::string filename = it->second.get<std::string>("filename");
        int fd_tmp = atoi(it->second.get<std::string>("fd").c_str());
        if (fd == fd_tmp) {
            FdExist = true;
            continue;
        }

        boost::property_tree::ptree item_tmp;
        item_tmp.put("filename", filename);
        item_tmp.put("fd", fd_tmp);

        vector_items.push_back(item_tmp);
    }
    if (!FdExist) {
        LOG(ERROR) << "rm fd failed, fd is not exist. fd is: " << fd;
        return -1;
    }

    boost::property_tree::ptree items_write;
    for (std::vector<boost::property_tree::ptree>::iterator iter =
             vector_items.begin();
         iter != vector_items.end(); iter++) {
        items_write.push_back(std::make_pair("", *iter));
    }

    root.put("port", g_port);
    root.put_child("volumes", items_write);

    try {
        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return -1;
    }

    LOG(NOTICE) << "rm fd success. " << fd;
    return 0;
}

void Squeeze(char s[], char c) {
    int i, j;
    for (i = 0, j = 0; s[i] != '\0'; i++) {
        if (s[i] != c) {
            s[j++] = s[i];
        }
    }
    s[j] = '\0';
}

// 获得mon host
std::string GetMonHost(const std::string& str) {
    char* mon_host_out = NULL;
    std::vector<std::string> result;
    if (*str.begin() == ':' || *str.end() == ':') {
        LOG(ERROR) << "wrong format. " << str;
        std::string mon_host;
        return mon_host;
    }

    auto sub_begin = str.begin();
    auto sub_end = str.begin();
    for (auto it = str.begin(); it != str.end(); ++it, ++sub_end) {
        if (*it == ':' && std::isalpha(*(it + 1))) {
            result.push_back(std::string(sub_begin, sub_end));
            sub_begin = it + 1;
        }
    }
    result.push_back(std::string(sub_begin, sub_end));

    for (auto piece : result) {
        if (strstr(piece.c_str(), "mon_host") != NULL) {
            std::vector<std::string> mon_host = split(piece, "=");
            mon_host_out = const_cast<char*>(mon_host[1].c_str());
            Squeeze(mon_host_out, '\\');
            LOG(ERROR) << "mon host is. " << mon_host_out;
        }
    }
    return mon_host_out;
}

// 字符串拆分，pattern为分隔符
std::vector<std::string> split(const std::string& str,
                               const std::string& pattern) {
    std::vector<std::string> res;
    if (str == "") return res;
    std::string strs = str + pattern;
    size_t pos = strs.find(pattern);

    while (pos != strs.npos) {
        std::string temp = strs.substr(0, pos);
        res.push_back(temp);
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(pattern);
    }

    return res;
}

// 找到可用的port
int FindPort(config_t* cfg, int port) {
    LOG(NOTICE) << "find port start.";

    struct dirent* ent;
    std::set<int> port_inuse;

    DIR* proc = opendir(g_filePath);
    if (proc == NULL) {
        LOG(ERROR) << "opendir file_path is error";
        return -1;
    }

    while ((ent = readdir(proc))) {
        if ((ent->d_name[0] == '.' && ent->d_name[1] == '\0') ||
            (ent->d_name[1] == '.' && ent->d_name[2] == '\0'))
            continue;
        std::string tmp = "/tmp/";
        std::string ent_name = ent->d_name;
        std::string filepath_tmp = g_filePath;
        std::string uuid_file_tmp = filepath_tmp + "/" + ent_name;
        std::string uuid_lockfile_tmp = tmp + ent_name;
        char* uuid_lockfile = const_cast<char*>(uuid_lockfile_tmp.c_str());
        char* uuid_file = const_cast<char*>(uuid_file_tmp.c_str());
        int lockfd = LockFile(uuid_lockfile);
        if (lockfd < 0) {
            closedir(proc);
            LOG(ERROR) << "lock file failed." << uuid_lockfile;
            return -1;
        }

        boost::property_tree::ptree root;
        boost::property_tree::ptree items;
        int port;

        try {
            boost::property_tree::read_json<boost::property_tree::ptree>(
                uuid_file, root);
            auto port_exist = root.get_child_optional("port");
            if (!port_exist) {
                LOG(ERROR) << "port is not exist.";
                UnlockFile(lockfd);
                continue;
            }
            port = root.get<int>("port", 0);
        } catch (boost::property_tree::ptree_error pt) {
            LOG(ERROR) << "read json throw an exception. " << pt.what();
            UnlockFile(lockfd);
            continue;
        }

        port_inuse.insert(port);

        UnlockFile(lockfd);
    }

    closedir(proc);

    port_option_t* port_option = new port_option_t;
    ReadPort(cfg, port_option);
    int min_port, max_port;
    min_port = port_option->min_port;
    max_port = port_option->max_port;
    delete port_option;
    int start_port;
    if (port == 0) {
        start_port = min_port;
    } else {
        start_port = port;
    }

    for (int i = start_port; i < max_port; i++) {
        if (!(port_inuse.find(i) != port_inuse.end())) {
            LOG(ERROR) << "port is not inuse, " << i;
            return i;
        } else {
            LOG(NOTICE) << " port is inuse, " << i;
        }
    }

    LOG(NOTICE) << "find port end.";
    return 0;
}

std::string GetUuidFile() {
    std::string uuid_file;
    std::string file_path_tmp = g_filePath;
    std::string g_uuid_tmp = g_uuid;
    uuid_file = file_path_tmp + "/" + g_uuid_tmp;
    return uuid_file;
}

std::string GetUuidLockfile() {
    std::string uuid_lockfile;
    std::string g_uuid_tmp = g_uuid;
    uuid_lockfile = "/tmp/" + g_uuid_tmp;
    return uuid_lockfile;
}

// 设置cpu亲和性
int SetCpuAffinity(pid_t qemu) {
    cpu_set_t mask;

    int r = sched_getaffinity(qemu, sizeof(cpu_set_t), &mask);
    if (r < 0) {
        LOG(NOTICE) << "getaffinity failed.";
        return r;
    }
    r = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
    if (r < 0) {
        LOG(NOTICE) << "setaffinity failed.";
        return r;
    }
    LOG(NOTICE) << "setaffinity success. " << qemu;
    return 0;
}

// 初始化
int Init() {
    // 初始化配置
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        char buffer[128];
        const char* err = "init config fialed.";
        snprintf(buffer, sizeof(buffer),
                 "echo %s>>/var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        return -1;
    }
    std::string file_path_tmp = ReadUuidPath(cfg);
    g_filePath = file_path_tmp.c_str();

    std::string log_path = ReadLogPath(cfg);
    std::string log_file = log_path + "/" + g_uuid + ".log";
    // 初始化日志模块
    logging::LoggingSettings log;
    log.logging_dest = logging::LOG_TO_FILE;
    log.log_file = log_file.c_str();
    logging::InitLogging(log);
    int loglevel = ReadLogLevel(cfg);
    logging::SetMinLogLevel(loglevel);
    if (NULL == opendir(g_filePath)) {
        if (mkdir(g_filePath, S_IRWXU | S_IRWXG) < 0) {
            LOG(ERROR) << "mkdir failed.";
            return -1;
        }
    }
    int ret = CheckProc(g_uuid);
    if (ret < 0) {
        LOG(ERROR) << "check proc failed.";
        return -1;
    }

    if (SetCpuAffinity(g_oldPid) < 0) {
        LOG(ERROR) << "set cpu affinity failed.";
        return -1;
    }

    // rpc option
    rpc_option_t* rpc_option = new rpc_option_t;
    ReadRpcOption(cfg, rpc_option);
    brpc::ServerOptions options;
    options.idle_timeout_sec = rpc_option->brpc_idle_timeout;
    options.num_threads = rpc_option->brpc_num_threads;
    delete rpc_option;
    brpc::Server brpc_server;
    QemuClientServiceImpl* qemuclient_service_impl = new QemuClientServiceImpl;
    if (brpc_server.AddService(qemuclient_service_impl,
                               brpc::SERVER_OWNS_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    bool new_port = false;
    std::string uuid_file = GetUuidFile();
    std::string uuid_lockfile = GetUuidLockfile();

    int lock_port_fd;
    int lockfd = LockFile(uuid_lockfile.c_str());
    if (lockfd < 0) {
        LOG(ERROR) << "lock file failed.";
        return -1;
    }

    // 如果持久化文件存在，则reload
    if (access(uuid_file.c_str(), F_OK) != -1) {
        int ret = Reload();
        UnlockFile(lockfd);
        if (ret <= 0) {
            LOG(ERROR) << "file is exist, but port is not exist.";
            return -1;
        } else {
            g_port = ret;
        }
    } else {
        UnlockFile(lockfd);
        std::string lock_port_file = ReadLockPortFile(cfg);
        lock_port_fd = LockFile(const_cast<char*>(lock_port_file.c_str()));
        if (lock_port_fd < 0) {
            LOG(ERROR) << "add port file lock failed.";
            return -1;
        }
        g_port = FindPort(cfg, 0);
        new_port = true;
    }
    LOG(NOTICE) << "port is: " << g_port;
    // 启动brpc server，重试retry_counts次
    int retry = 0;
    int retry_counts = ReadRetryCounts(cfg);
    std::string ip = "127.0.0.1:";
    while (++retry <= retry_counts) {
        std::string ip_port = ip + std::to_string(g_port);
        if (brpc_server.Start(ip_port.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to start QemuClientServer, retry time is: "
                       << retry;
            g_port = FindPort(cfg, g_port + 1);
        } else {
            LOG(ERROR) << "start QemuClientServer success.";
            break;
        }
    }
    if (retry >= retry_counts) {
        LOG(ERROR) << "Fail to start QemuClientServer.";
        return -1;
    }
    // 如果是新启动的，则保存进持久化文件
    if (new_port) {
        lockfd = LockFile(uuid_lockfile.c_str());
        if (lockfd < 0) {
            LOG(ERROR) << "add lock failed.";
            return -1;
        }

        int ret;
        ret = GeneratePort(g_port);
        if (ret < 0) {
            LOG(ERROR) << "generate port failed.";
            return -1;
        }
        UnlockFile(lockfd);
        UnlockFile(lock_port_fd);
    }
    config_destroy(cfg);
    // 启动心跳线程
    bthread_t th;
    if (bthread_start_background(&th, NULL, HeartbeatProcess, NULL) != 0) {
        LOG(ERROR) << "Fail to create bthread";
        return -1;
    }
    // 响应SIGINT以及SIGTERM
    signal(SIGTERM, SigProcess);
    signal(SIGINT, SigProcess);
    signal(SIGHUP, SigLogReplace);
    bthread_join(th, NULL);
    brpc_server.Stop(0);
    brpc_server.Join();
    return 0;
}
