/*
 * Project: curve
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_

#include <string>

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
    DefaultTokenGenerator() {}
    virtual ~DefaultTokenGenerator() {}
    virtual std::string GenToken();
};

}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_TOKEN_GENERATOR_H_
