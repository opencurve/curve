/*
 * Project: curve
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include <string>
#include <cstdlib>
#include <ctime>

#include "src/mds/topology/topology_token_generator.h"

namespace curve {
namespace mds {
namespace topology {

// TODO(xuchaojie): 优化token设计
std::string DefaultTokenGenerator::GenToken() {
    std::string ret = "";
    for (int i = 0; i < 8; i++) {
        ret.push_back('a' + std::rand() % 26);
    }
    return ret;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
