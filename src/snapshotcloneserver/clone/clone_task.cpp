/*
 * Project: curve
 * Created Date: Wed Mar 18 2020
 * Author: xuchaojie
 * Copyright (c) 2020 netease
 */
#include "src/snapshotcloneserver/clone/clone_task.h"

namespace curve {
namespace snapshotcloneserver {

std::ostream& operator<<(std::ostream& os, const CloneTaskInfo &taskInfo) {
    os << "{ CloneInfo : " << taskInfo.GetCloneInfo();
    os << ", Progress : " << taskInfo.GetProgress() << " }";
    return os;
}

}  // namespace snapshotcloneserver
}  // namespace curve
