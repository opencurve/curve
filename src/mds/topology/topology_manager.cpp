/*
 * Project: curve
 * Created Date: Thu Aug 23 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>

#include "src/mds/topology/topology_manager.h"
#include "glog/logging.h"
#include "src/repo/repo.h"


DEFINE_string(dbName, "curve_mds", "dbName");
DEFINE_string(user, "root", "user");
DEFINE_string(url, "localhost", "url");
DEFINE_string(password, "qwer", "password");

namespace curve {
namespace mds {
namespace topology {


TopologyManager::TopologyManager() {
    std::shared_ptr<TopologyIdGenerator> idGenerator_  =
        std::make_shared<DefaultIdGenerator>();
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
        std::make_shared<DefaultTokenGenerator>();

    std::shared_ptr<::curve::repo::RepoInterface> repo_ =
        std::make_shared<::curve::repo::Repo>();

    std::shared_ptr<TopologyStorage> storage_ =
        std::make_shared<DefaultTopologyStorage>(repo_);
    // TODO(xuchaojie): use data from config file to init storage
    if (!storage_->init(FLAGS_dbName,
            FLAGS_user,
            FLAGS_url,
            FLAGS_password)) {
        LOG(FATAL) << "init storage fail.";
        return;
    }

    topology_ = std::make_shared<Topology>(idGenerator_,
                                           tokenGenerator_,
                                           storage_);
    int errorCode = topology_->init();
    if (errorCode != kTopoErrCodeSuccess) {
        LOG(FATAL) << "init topology fail.";
        return;
    }

    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_ =
        std::make_shared<curve::mds::copyset::CopysetManager>();
    serviceManager_ = std::make_shared<TopologyServiceManager>(topology_,
         copysetManager_);
    topologyAdmin_ = std::make_shared<TopologyAdminImpl>(topology_);
}



}  // namespace topology
}  // namespace mds
}  // namespace curve

