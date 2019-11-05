/*
 * Project: curve
 * File Created: Thursday, 10th October 2019 4:45:12 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
 */


#ifndef SRC_CLIENT_SESSION_MAP_H_
#define SRC_CLIENT_SESSION_MAP_H_

#include <string>

#include "thirdparties/mordenjson/json.hpp"


using json = nlohmann::json;

namespace curve {
namespace client {
class SessionMap {
 public:
    /**
     * 获取文件的sessionid
     * @param: sessionpath为sessionmap文件路径
     * @param: filenam为目标文件名
     * @return: 成功返回该文件的sessionid，否则返回空
     */
    std::string GetFileSessionID(const std::string& sessionpath,
                                 const std::string& filename);

    /**
     * 持久化文件的sessionid信息
     * @param: path为当前session文件路径
     * @param: filepath为目标文件路径
     * @param: sessionid为文件的session信息
     * @return: 成功返回0
     */
    int PersistSessionMapWithLock(const std::string& path,
                                  const std::string& filepath,
                                  const std::string& sesssionID);

    /**
     * 删除seesionid
     * @param: path为session文件路径
     * @param: filepath为目标文件路径
     * @return: 成功返回0
     */
    int DelSessionID(const std::string& path, const std::string& filepath);

 private:
    /**
     *  加载session文件信息
     * @param: path为session文件路径
     * @return: 成功返回0，否则返回-1
     */
    int LoadSessionMap(const std::string& path);

    /**
     * 解析json文件
     * @param: fd为调用方打开文件的fd，外围打开文件后对该文件加锁然后调用这个解析函数
     * @param: path为session文件路径，用于内部回去该文件信息
     * @return: 成功返回0
     */
    int ParseJson(int fd, const std::string& path);

    /**
     * 持久化当前的seesion信息，调用方打开session文件并获取文件锁
     * @param: fd为调用方打开的文件描述符
     * @param: path为当前session的路径
     * @return: 成功返回0
     */
    int PesistInternal(int fd, const std::string& path);

 private:
    json j;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_SESSION_MAP_H_
