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
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_

#include <string>
#include <ctime>

#include "src/mds/topology/topology_token_generator.h"

namespace curve {
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
    DefaultTokenGenerator() {
        std::srand(std::time(nullptr));
    }
    virtual ~DefaultTokenGenerator() {}
    virtual std::string GenToken();
};

}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
