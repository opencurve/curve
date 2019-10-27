/*
 * Project: curve
 * File Created: Thursday, 10th October 2019 4:45:23 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
 */
#include <glog/logging.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <dirent.h>
#include <linux/fs.h>
#include <sys/file.h>

#include <string.h>
#include <memory>

#include "src/client/session_map.h"

namespace curve {
namespace client {
int SessionMap::LoadSessionMap(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT);

    if (fd < 0) {
        LOG(WARNING) << "open file [" << path
                   << "] failed, ret = " << fd
                   << ", errno = " << errno;
        return -1;
    }

    if (flock(fd, LOCK_EX) == 0) {
        int ret = ParseJson(fd, path);
        flock(fd, LOCK_UN);
        ::close(fd);
        return ret;
    }
    ::close(fd);
    return -1;
}

int SessionMap::ParseJson(int fd, const std::string& path) {
    uint64_t filesize = 0;
    struct stat statbuff;
    if (::stat(path.c_str(), &statbuff) < 0) {
        return -1;
    } else {
        filesize = statbuff.st_size;
    }

    if (filesize == 0) {
        LOG(INFO) << "session map file is empty! " << path;
        return 0;
    }

    std::unique_ptr<char[]> readvalid(new char[filesize + 1]);
    memset(readvalid.get(), 0, filesize + 1);
    int ret = ::pread(fd, readvalid.get(), filesize, 0);
    if (ret != filesize) {
        LOG(WARNING) << "session file read failed! " << path;
        return -1;
    }

    try {
        j = json::parse(readvalid.get());
    } catch (...) {
        LOG(WARNING) << "session file parse got error! " << path
                   << ", content = [" << readvalid.get() << "]";
        return -1;
    }
    return 0;
}

std::string SessionMap::GetFileSessionID(const std::string& sessionpath,
                                         const std::string& filename) {
    int ret = LoadSessionMap(sessionpath);
    if (ret != 0) {
        LOG(WARNING) << "load session map failed! " << sessionpath;
        return "";
    }

    if (!j[filename].is_null()) {
        return j[filename];
    }
    return "";
}

int SessionMap::PersistSessionMapWithLock(const std::string& path,
                                           const std::string& filename,
                                           const std::string& sesssionID) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT);

    if (fd < 0) {
        LOG(WARNING) << "file [" << path << "] open failed!"
                   << "errno = " << errno;
        return -1;
    }

    if (flock(fd, LOCK_EX) == 0) {
        int ret = ParseJson(fd, path);
        if (ret != 0) {
            flock(fd, LOCK_UN);
            ::close(fd);
            return -1;
        }

        j[filename] = sesssionID;

        ret = PesistInternal(fd, path);
        flock(fd, LOCK_UN);
        ::close(fd);
        return ret;
    }
    ::close(fd);
    return -1;
}

int SessionMap::DelSessionID(const std::string& path,
                             const std::string& filepath) {
    int fd = ::open(path.c_str(), O_RDWR);

    if (fd < 0) {
        LOG(WARNING) << "file [" << path << "] open failed!"
                   << ", errno = " << errno;
        return -1;
    }

    if (flock(fd, LOCK_EX) == 0) {
        int ret = ParseJson(fd, path);
        if (ret != 0) {
            flock(fd, LOCK_UN);
            ::close(fd);
            return -1;
        }

        if (!j[filepath].is_null()) {
            try {
                j.erase(filepath);
            } catch (...) {
                LOG(WARNING) << "delete element failed!";
            }
        } else {
            LOG(WARNING) << "filepath not contained in " << path;
        }

        ret = PesistInternal(fd, path);
        flock(fd, LOCK_UN);
        ::close(fd);
        return ret;
    }

    ::close(fd);
    return -1;
}

int SessionMap::PesistInternal(int fd, const std::string& path) {
    std::string temp_path = path + "_temp";
    std::string data = j.dump();
    uint64_t size = data.size();

    ::ftruncate(fd, 0);

    int len = ::pwrite(fd, data.c_str(), size, 0);
    if (len != size) {
        ::ftruncate(fd, 0);
        LOG(WARNING) << "session id persist failed, errno = " << errno;
        return -1;
    }

    ::fsync(fd);
    return 0;
}

}   // namespace client
}   // namespace curve
