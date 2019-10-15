/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include <stdio.h>
#include <stdlib.h>
#include <libconfig.h>
#include <butil/logging.h>
#include "src/part2/config.h"
#include "src/part2/common_type.h"

#define NEBD_CONF_FILE_DEFAULT "/etc/nebd/nebd-server.conf"
#define LOG_CONF_FILE_DEFAULT "/var/log/nebd/"
#define UUID_CONF_FILE_DEFAULT "/var/run/nebd-server/"
#define QEMU_XML_DIR_DEFAULT "/var/run/libvirt/qemu/"
#define LOCKPORT_CONF_FILE_DEFAULT "/tmp/nebd-server.port.file.lock"
#define NEBD_CEPH_CONF "/etc/ceph/ceph.conf"
#define BRPC_NUM_THREADS 2
#define BRPC_IDLE_TIMEOUT -1
#define BRPC_MAX_CONCURRENCY 0
#define BRPC_METHOD_MAX_CONCURRENCY 0
#define LOGLEVEL 1
#define MIN_PORT 6200
#define MAX_PORT 6500
#define RETRY_COUNTS 3
#define HEARTBEAT_INTERVAL 1
#define CHECK_ASSERT_TIMES 10
#define CHECHK_DETACHED_INTERVAL 5
#define CHECK_DETACHED_TIME 6

config_t* InitConfig() {
    config_t* cfg = new config_t;
    config_init(cfg);

    // Read the file. If there is an error, return NUll.
    if (!config_read_file(cfg, NEBD_CONF_FILE_DEFAULT)) {
        char buffer[128];
        const char *err = "read config file failed.";
        snprintf(buffer, sizeof(buffer),
                 "echo %s >> /var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        config_destroy(cfg);
        return NULL;
    }
    return cfg;
}

void ReadRpcOption(config_t* cfg, rpc_option_t* rpc_option) {
    if (!config_lookup_int(cfg, "brpc_num_threads",
                           &rpc_option->brpc_num_threads))
        rpc_option->brpc_num_threads = BRPC_NUM_THREADS;

    if (!config_lookup_int(cfg, "brpc_idle_timeout",
                           &rpc_option->brpc_idle_timeout))
        rpc_option->brpc_idle_timeout = BRPC_IDLE_TIMEOUT;

    if (!config_lookup_int(cfg, "brpc_max_concurrency",
                           &rpc_option->brpc_max_concurrency))
        rpc_option->brpc_max_concurrency = BRPC_MAX_CONCURRENCY;

    if (!config_lookup_int(cfg, "brpc_methodmax_concurrency",
                           &rpc_option->brpc_method_max_concurrency))
        rpc_option->brpc_method_max_concurrency = BRPC_METHOD_MAX_CONCURRENCY;
    return;
}

void ReadPort(config_t* cfg, port_option_t* port_option) {
    if (!config_lookup_int(cfg, "min_port", &port_option->min_port))
        port_option->min_port = MIN_PORT;

    if (!config_lookup_int(cfg, "max_port", &port_option->max_port))
        port_option->max_port = MAX_PORT;

    return;
}

int ReadLogLevel(config_t* cfg) {
    int loglevel;
    if (!config_lookup_int(cfg, "loglevel", &loglevel)) loglevel = LOGLEVEL;

    return loglevel;
}

std::string ReadUuidPath(config_t* cfg) {
    std::string uuid_path;
    const char* uuid_path_tmp;

    if (!config_lookup_string(cfg, "uuid_file_path", &uuid_path_tmp))
        uuid_path_tmp = UUID_CONF_FILE_DEFAULT;

    uuid_path = uuid_path_tmp;
    return uuid_path;
}

int ReadDetachedTimes(config_t* cfg) {
    int detached_times;
    if (!config_lookup_int(cfg, "detached_times", &detached_times))
        detached_times = CHECK_DETACHED_TIME;
    return detached_times;
}

int ReadRetryCounts(config_t* cfg) {
    int retry_counts;
    if (!config_lookup_int(cfg, "retry_counts", &retry_counts))
        retry_counts = RETRY_COUNTS;

    return retry_counts;
}

std::string ReadLockPortFile(config_t* cfg) {
    std::string lock_port_file;
    const char* lock_port_file_tmp;

    if (!config_lookup_string(cfg, "lock_port_file", &lock_port_file_tmp))
        lock_port_file_tmp = LOCKPORT_CONF_FILE_DEFAULT;

    lock_port_file = lock_port_file_tmp;
    return lock_port_file;
}

std::string ReadCephConf(config_t* cfg) {
    std::string ceph_conf;
    const char* ceph_conf_tmp;

    if (!config_lookup_string(cfg, "nebd_ceph_conf", &ceph_conf_tmp))
        ceph_conf_tmp = NEBD_CEPH_CONF;

    ceph_conf = ceph_conf_tmp;
    return ceph_conf;
}

std::string ReadLogPath(config_t* cfg) {
    std::string log_path;
    const char* log_path_tmp;
    if (!config_lookup_string(cfg, "log_path", &log_path_tmp))
        log_path_tmp = LOG_CONF_FILE_DEFAULT;

    log_path = log_path_tmp;
    return log_path;
}

std::string ReadQemuXmlDir(config_t* cfg) {
    std::string qemu_dir;
    const char* qemu_dir_tmp;
    if (!config_lookup_string(cfg, "qemu_xml_dir", &qemu_dir_tmp))
        qemu_dir_tmp = QEMU_XML_DIR_DEFAULT;

    qemu_dir = qemu_dir_tmp;
    return qemu_dir;
}

int ReadHeartbeatInterval(config_t* cfg) {
    int interval;
    if (!config_lookup_int(cfg, "heartbeat_interval", &interval))
        interval = HEARTBEAT_INTERVAL;

    return interval;
}

int ReadAssertTimes(config_t* cfg) {
    int check_assert_times;
    if (!config_lookup_int(cfg, "check_assert_times", &check_assert_times))
        check_assert_times = CHECK_ASSERT_TIMES;

    return check_assert_times;
}

int ReadCheckDetachedInterval(config_t* cfg) {
    int interval;
    if (!config_lookup_int(cfg, "check_detached_interval", &interval))
        interval = CHECHK_DETACHED_INTERVAL;

    return interval;
}

