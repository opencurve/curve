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
 * @Project: curve
 * @Date: 2021-11-23 19:25:41
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_
#define CURVEFS_SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_

#include <glog/logging.h>
#include <brpc/server.h>
#include <memory>
#include "curvefs/proto/schedule.pb.h"
#include "curvefs/src/mds/schedule/coordinator.h"

namespace curvefs {
namespace mds {
namespace schedule {
class ScheduleServiceImpl : public ScheduleService {
 public:
    explicit ScheduleServiceImpl(
        const std::shared_ptr<Coordinator> &coordinator)
        : coordinator_(coordinator) {}

    virtual ~ScheduleServiceImpl() {}

    virtual void QueryMetaServerRecoverStatus(
        google::protobuf::RpcController* cntl_base,
        const QueryMetaServerRecoverStatusRequest *request,
        QueryMetaServerRecoverStatusResponse *response,
        google::protobuf::Closure* done);

 private:
    std::shared_ptr<Coordinator> coordinator_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_
