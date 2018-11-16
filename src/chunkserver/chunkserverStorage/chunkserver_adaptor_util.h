/*
 * Project: curve
 * Created Date: Friday August 24th 2018
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_ADAPTOR_UTIL_H
#define CURVE_CHUNKSERVER_ADAPTOR_UTIL_H

#include <limits.h>
#include <string>
#include <list>

namespace curve {
namespace chunkserver {
class FsAdaptorUtil {
 public:
    // TODO(tongguangxun): we need a string piece util
    // ${protocol}://${parameters}
    static std::string ParserUri(const std::string& uri, std::string * param);
    static std::list<std::string> ParserDirPath(std::string path);
};
}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_ADAPTOR_UTIL_H
