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

#ifndef NBD_SRC_UTIL_H_
#define NBD_SRC_UTIL_H_

#include <string>
#include <vector>
#include "nbd/src/define.h"

namespace curve {
namespace nbd {

class NBDListIterator {
 public:
    NBDListIterator() = default;
    ~NBDListIterator() = default;
    bool Get(int *pid, NBDConfig *cfg);

 private:
    int curIndex_ = 0;
};

// 根据errno打印错误信息，err值无论正负都能处理
extern std::string cpp_strerror(int err);
// 从nbd设备名中解析出nbd的index
extern int parse_nbd_index(const std::string& devpath);
// 获取当前系统能够支持的最大nbd设备数量
extern int get_nbd_max_count();
// 解析用户输入的命令参数
extern int parse_args(std::vector<const char*>& args,   // NOLINT
                      std::ostream *err_msg,
                      Command *command, NBDConfig *cfg);
// 获取指定nbd进程对应设备的挂载信息
extern int get_mapped_info(int pid, NBDConfig *cfg);
// 检查指定nbd设备的block size是否符合预期
extern int check_block_size(int nbd_index, uint64_t expected_size);
// 检查指定nbd设备的大小是否符合预期
extern int check_device_size(int nbd_index, uint64_t expected_size);
// 如果当前系统还未加载nbd模块，则进行加载；如果已经加载，则不作任何操作
extern int load_module(NBDConfig *cfg);
// Check whether the device can be unmap
int check_dev_can_unmap(const NBDConfig *cfg);

// 安全读写文件或socket，对异常情况进行处理后返回
ssize_t safe_read_exact(int fd, void* buf, size_t count);
ssize_t safe_read(int fd, void* buf, size_t count);
ssize_t safe_write(int fd, const void* buf, size_t count);

// 网络字节序转换
inline uint64_t ntohll(uint64_t val) {
    return ((val >> 56) |
            ((val >> 40) & 0xff00ull) |
            ((val >> 24) & 0xff0000ull) |
            ((val >> 8) & 0xff000000ull) |
            ((val << 8) & 0xff00000000ull) |
            ((val << 24) & 0xff0000000000ull) |
            ((val << 40) & 0xff000000000000ull) |
            ((val << 56)));
}

}  // namespace nbd
}  // namespace curve

#endif  // NBD_SRC_UTIL_H_
