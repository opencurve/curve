/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Thursday March 21st 2019
 * Author: yangyaokai
 */

#ifndef SRC_COMMON_URI_PARSER_H_
#define SRC_COMMON_URI_PARSER_H_

#include <limits.h>
#include <string>
#include <list>

namespace curve {
namespace common {

class UriParser {
 public:
    static std::string ParseUri(const std::string& uri,
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

    static std::string GetProtocolFromUri(const std::string& uri) {
        std::string path;
        return ParseUri(uri, &path);
    }

    static std::string GetPathFromUri(const std::string& uri) {
        std::string path;
        ParseUri(uri, &path);
        return path;
    }

    static std::list<std::string> ParseDirPath(std::string path) {
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

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_URI_PARSER_H_
