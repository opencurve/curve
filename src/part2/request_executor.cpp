/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include "src/part2/request_executor.h"
#include "src/part2/request_executor_ceph.h"
#include "src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

NebdRequestExecutor* g_test_executor = nullptr;

NebdRequestExecutor*
NebdRequestExecutorFactory::GetExecutor(NebdFileType type) {
    NebdRequestExecutor* executor = nullptr;
    switch (type) {
        case NebdFileType::CEPH:
            executor = &CephRequestExecutor::GetInstance();
            break;
        case NebdFileType::CURVE:
            executor = &CurveRequestExecutor::GetInstance();
            break;
        case NebdFileType::TEST:
            executor = g_test_executor;
            break;
        default:
            break;
    }
    return executor;
}

}  // namespace server
}  // namespace nebd

