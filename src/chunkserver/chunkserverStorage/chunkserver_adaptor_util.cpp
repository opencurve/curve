/*
 * Project: curve
 * File Created: Thursday, 22nd November 2018 2:03:49 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

namespace curve {
namespace chunkserver {
    std::string FsAdaptorUtil::ParserUri(const std::string& uri,
                                        std::string * param) {
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

    std::list<std::string> FsAdaptorUtil::ParserDirPath(std::string path) {
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
}  // namespace chunkserver
}  // namespace curve
