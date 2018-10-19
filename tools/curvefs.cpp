/*
 * Project: curve
 * Created Date: Fri Oct 19 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <brpc/channel.h>



#include "proto/topology.pb.h"


DEFINE_string(op, "CreateLogicalPool", "operation.");
DEFINE_string(name, "defaultLogicalPool", "logical pool name.");
DEFINE_int32(physicalPoolID, 1, "physicalPool id.");
DEFINE_int32(logicalPoolType,
    ::curve::mds::topology::PAGEFILE,
    "logical pool type.");
DEFINE_int32(copysetNum, 100, "copyset num.");

DEFINE_string(mds_ip, "172.0.0.1", "mds ip");
DEFINE_int32(mds_port, 8000, "mds port");


namespace curve {
namespace mds {
namespace topology {

class CurvefsTools {
 public:
    CurvefsTools() {}
    ~CurvefsTools() {}

    bool HandleCreateLogicalPool();
};

bool CurvefsTools::HandleCreateLogicalPool() {
    bool ret = true;

    brpc::Channel channel;
    if (channel.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL) != 0) {
    LOG(FATAL) << "Fail to init channel to ip: "
               << FLAGS_mds_ip
               << " port "
               << FLAGS_mds_port
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(10);
    cntl.set_log_id(1);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname(FLAGS_name);
    request.set_physicalpoolid(FLAGS_physicalPoolID);
    request.set_type(static_cast<LogicalPoolType>(FLAGS_logicalPoolType));

    std::string copysetNumStr = std::to_string(FLAGS_copysetNum);

    std::string rapString = "{\"replicaNum\":3, \"copysetNum\":"
        + copysetNumStr + ", \"zoneNum\":3}";

    request.set_redundanceandplacementpolicy(rapString);
    request.set_userpolicy("{}");

    CreateLogicalPoolResponse response;
    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

     if (cntl.Failed()) {
        LOG(ERROR) << "Create file failed, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
    }
    if (response.statuscode() != 0) {
        LOG(ERROR) << "Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
    }

    return ret;
}


}  // namespace topology
}  // namespace mds
}  // namespace curve



int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    curve::mds::topology::CurvefsTools tools;

    std::string operation = FLAGS_op;
    if (operation == "CreateLogicalPool") {
        tools.HandleCreateLogicalPool();
    }

    return 0;
}


