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
    static std::string ParserUri(const std::string& uri, std::string * param) {
        // ${protocol}://${parameters}
        std::string protocol;
        do {
            size_t pos = uri.find("://");
            if (pos == std::string::npos) {
                break;
            }
            protocol = uri.substr(0, pos);
            *param = uri.substr(pos+3, uri.find_last_not_of(" "));
        } while (0);
        return protocol;
    }

    static std::list<std::string> ParserDirPath(std::string path) {
        std::list<std::string> dirpath;
        dirpath.clear();
        int startpos = NAME_MAX;
        while (path[path.length()-1] == '/') {
            path = path.substr(0, path.length()-1);
        }
        while (!(startpos <= 1)) {
            dirpath.push_front(path);
            startpos = path.find_last_of('/', startpos);
            path = path.substr(0, startpos);
        }
        return dirpath;
    }
};
}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_ADAPTOR_UTIL_H
