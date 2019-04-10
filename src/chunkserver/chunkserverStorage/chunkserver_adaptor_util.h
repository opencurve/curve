/*
 * Project: curve
 * Created Date: Friday August 24th 2018
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_ADAPTOR_UTIL_H_
#define SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_ADAPTOR_UTIL_H_

#include <limits.h>
#include <string>
#include <list>

namespace curve {
namespace chunkserver {
class FsAdaptorUtil {
 public:
    // TODO(tongguangxun): we need a string piece util
    /**
     * @brief 从URI中解析出URI协议和URI路径,
     * 格式如下：${protocol}://${parameters}
     * @param[in] uri 待解析的URI
     * @param[out] param URI路径
     * @return URI协议
     */
    static std::string ParserUri(const std::string& uri, std::string * param);
    /**
     * @brief 从URI中解析出URI协议
     * 格式如下：${protocol}://${parameters}
     * @param[in] uri 待解析的URI
     * @return URI协议
     */
    static std::string GetProtocolFromUri(const std::string& uri);
    /**
     * @brief 从URI中解析出URI路径,
     * 格式如下：${protocol}://${parameters}
     * @param[in] uri 待解析的URI
     * @return URI路径
     */
    static std::string GetPathFromUri(const std::string& uri);
    static std::list<std::string> ParserDirPath(std::string path);
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_ADAPTOR_UTIL_H_
