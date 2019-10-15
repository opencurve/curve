/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_CONFIG_H_
#define SRC_PART2_CONFIG_H_

#include <libconfig.h>
#include <string>

/**
 * @brief 初始化配置
 */
config_t* InitConfig();

/**
 * @brief rpc相关配置
 */
typedef struct rpc_option {
    int brpc_num_threads;  // brpc默认线程数
    int brpc_idle_timeout;
    int brpc_max_concurrency;
    int brpc_method_max_concurrency;
} rpc_option_t;
void ReadRpcOption(config_t* cfg, rpc_option_t* rpc_option);

/**
 * @brief port相关配置
 */
typedef struct port_option {
    int min_port;
    int max_port;
} port_option_t;
void ReadPort(config_t* cfg, port_option_t* port_option);
// 或取日志级别
int ReadLogLevel(config_t* cfg);
// 获取持久化文件路径
std::string ReadUuidPath(config_t* cfg);
// 获取启动brpc server的重试次数
int ReadRetryCounts(config_t* cfg);
// 获取qemu xml文件的路径
std::string ReadQemuXmlDir(config_t* cfg);
// 获取心跳间隔
int ReadHeartbeatInterval(config_t* cfg);
// 获取心跳检查次数，超过该次数未检查到，则进程自杀
int ReadAssertTimes(config_t* cfg);
// 获取日志文件路径
std::string ReadLogPath(config_t* cfg);
// 获取检查detach卷的间隔
int ReadCheckDetachedInterval(config_t* cfg);
// 获取lock port文件
std::string ReadLockPortFile(config_t* cfg);
// 获取ceph配置文件路径
std::string ReadCephConf(config_t* cfg);
// 获取detach卷检查次数
int ReadDetachedTimes(config_t* cfg);
#endif  // SRC_PART2_CONFIG_H_
