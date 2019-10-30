/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_COMMON_TYPE_H_
#define SRC_PART2_COMMON_TYPE_H_

#include <rados/librados.h>
#include <rbd/librbd.h>
#include <brpc/server.h>
#include <libconfig.h>
#include <map>
#include <string>
#include <vector>
#include <atomic>

// brpc server
extern brpc::Server g_server;

typedef struct FdImage {
    rados_t* cluster;
    rados_ioctx_t* io;
    rbd_image_t* image;
    char* filename;
} FdImage_t;
// 主键为fd
extern std::map<int, FdImage_t*> g_imageMap;  // fd的内存镜像
// ceph的fd取值范围为[1，1000]，curve取值范围为[1001, 20000]
#define FD_CEPH_MAX 1000
#define FD_CURVE_MAX 20000

int CloseParentFd();
int LockFile(const char* file);
void UnlockFile(int lockfd);
std::vector<std::string> split(const std::string& str,
                              const std::string& pattern);
int ReadQemuXmls();
void ReadQemuXml(const char*);
int GenerateFd(char* filename, int fd);
int GeneratePort(int port);
int FindPort(config_t* cfg, int port);
int RmFd(int fd);
std::string GetUuidFile();
std::string GetUuidLockfile();
int SetCpuAffinity(pid_t qemu);
std::string GetMonHost(const std::string &str);
int Init();
void Squeeze(char s[], char c);
// qemu达[秾K潚~Duuid
extern char* g_uuid;
// qemu达[秾K潚~Dpid
extern int64_t g_oldPid;
// 该nebd-server秾K幾O使潔¨潚~D端住£住·
extern int g_port;
// 记弾U请氾B录°轇~O
extern std::atomic_long requestForRpcWrite;
extern std::atomic_long requestForCephWrite;
extern std::atomic_long requestForRpcWriteCounts;
// uuid彌~A举E佌~V彖~G件嬾X彔
extern const char* g_filePath;

// 侾]嬾X乾Q主彜º弾S佉~M彌~B载潚~D位·信彁¯
extern std::vector<std::string> g_qemuPoolVolumes;

#endif  // SRC_PART2_COMMON_TYPE_H_
