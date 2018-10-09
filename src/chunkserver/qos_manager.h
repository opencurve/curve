/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/copyset_node_manager.h"

#ifndef SRC_CHUNKSERVER_QOS_MANAGER_H_
#define SRC_CHUNKSERVER_QOS_MANAGER_H_

namespace curve {
namespace chunkserver {

typedef int QOS_OP_TYPE;

struct QosRequestStruct{
    QOS_OP_TYPE type;
    void* request;
};

struct QosOptions {
    CopysetNodeManager* copysetNodeManager;
};

class QosManager {
 public:
    QosManager() {}
    ~QosManager() {}

    int Init(const QosOptions &options);
    int Run();
    int Fini();

    int AddRequest();

 private:
    int InitConfig();

    QosOptions          options_;
    CopysetNodeManager* copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_QOS_MANAGER_H_
