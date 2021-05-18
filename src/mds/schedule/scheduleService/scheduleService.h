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
 * Created Date: 2020-01-02
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_
#define SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_

#include <glog/logging.h>
#include <brpc/server.h>
#include <memory>
#include "proto/schedule.pb.h"
#include "src/mds/schedule/coordinator.h"

namespace curve {
namespace mds {
namespace schedule {
class ScheduleServiceImpl : public ScheduleService {
 public:
    explicit ScheduleServiceImpl(
        const std::shared_ptr<Coordinator> &coordinator)
        : coordinator_(coordinator) {}

    virtual ~ScheduleServiceImpl() {}

    virtual void RapidLeaderSchedule(
        google::protobuf::RpcController* cntl_base,
        const RapidLeaderScheduleRequst* request,
        RapidLeaderScheduleResponse* response,
        google::protobuf::Closure* done);

    virtual void QueryChunkServerRecoverStatus(
        google::protobuf::RpcController* cntl_base,
        const QueryChunkServerRecoverStatusRequest *request,
        QueryChunkServerRecoverStatusResponse *response,
        google::protobuf::Closure* done);

    virtual void CancelScanSchedule(
        google::protobuf::RpcController* cntl_base,
        const CancelScanScheduleRequest* request,
        CancelScanScheduleResponse* response,
        google::protobuf::Closure* done);

 private:
    std::shared_ptr<Coordinator> coordinator_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_
