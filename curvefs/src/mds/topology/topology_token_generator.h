/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_

#include <ctime>
#include <string>

namespace curvefs {
namespace mds {
namespace topology {

class TopologyTokenGenerator {
 public:
    TopologyTokenGenerator() {}
    virtual ~TopologyTokenGenerator() {}

    virtual std::string GenToken() = 0;
};

class DefaultTokenGenerator : public TopologyTokenGenerator {
 public:
    DefaultTokenGenerator() { std::srand(std::time(nullptr)); }
    virtual ~DefaultTokenGenerator() {}
    virtual std::string GenToken();
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
