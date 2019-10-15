/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_HEARTBEAT_H_
#define SRC_PART2_HEARTBEAT_H_
#include <cstdint>
/**
 * @brief 心跳线程
 */
void* HeartbeatProcess(void*);
/**
 * @brief 响应SIGINT，SIGTERM信号
 */
void SigProcess(int sig_no);
/**
 * @brief 响应SIGHUP信号
 */
void SigLogReplace(int sig_no);
/**
 * @brief 关闭所有已经卸载的卷线程
 */
void* CloseDetachedVolumesProcess(void*);
/**
 * @brief 停止part2
 */
int StopProc(bool need_check_qemu_proc);
/**
 * @brief 关闭所有已经卸载的卷
 * @param detachedTimes 检查次数。检查次数超过其才会真的卸载卷
 */
int CloseQemuDetachedVolumes(int detachedTimes);
/**
 * @brief 检查qemu进程是否存在
 * @param uuid qemu进程对应的uuid
 */
int CheckProc(const char* uuid);
char* SkipSpace(char* in, int* offset, int len);
int CheckCmdline(int64_t pid, const char* proc_name, char* uuid, int uuid_len);
#endif  // SRC_PART2_HEARTBEAT_H_
