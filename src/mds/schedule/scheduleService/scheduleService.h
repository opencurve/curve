/*
 * Project: curve
 * Created Date: 2020-01-02
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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

 private:
    std::shared_ptr<Coordinator> coordinator_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULESERVICE_SCHEDULESERVICE_H_
