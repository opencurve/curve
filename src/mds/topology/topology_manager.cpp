/*
 * Project: curve
 * Created Date: Thu Aug 23 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>

#include <string>

#include "src/mds/topology/topology_manager.h"
#include "glog/logging.h"

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

    std::shared_ptr<MdsRepo> repo_ = std::make_shared<MdsRepo>();

    std::shared_ptr<TopologyStorage> storage_ =
        std::make_shared<DefaultTopologyStorage>(repo_);
    // TODO(xuchaojie): use data from config file to init storage
    std::string dbName = FLAGS_dbName;
    std::string user = FLAGS_user;
    std::string url = FLAGS_url;
    std::string password = FLAGS_password;
    if (!storage_->init(dbName,
            user,
            url,
            password)) {
        LOG(FATAL) << "init storage fail. dbName = "
                   << dbName
                   << " , user = "
                   << user
                   << " , url = "
                   << url
                   << " , password = "
                   << password;
        return;
    }

    topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                           tokenGenerator_,
                                           storage_);
    int errorCode = topology_->init();
    if (errorCode != kTopoErrCodeSuccess) {
        LOG(FATAL) << "init topology fail. errorCode = "
                   << errorCode;
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

