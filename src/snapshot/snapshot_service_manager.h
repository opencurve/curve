/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "src/snapshot/snapshot_core.h"
#include "src/snapshot/snapshot_task.h"
#include "src/snapshot/snapshot_task_manager.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

class FileSnapshotInfo {
 public:
    FileSnapshotInfo(const SnapshotInfo &snapInfo,
        uint32_t snapProgress)
        : snapInfo_(snapInfo),
          snapProgress_(snapProgress) {}

    void SetSnapshotInfo(const SnapshotInfo &snapInfo) {
        snapInfo_ = snapInfo;
    }

    SnapshotInfo GetSnapshotInfo() const {
        return snapInfo_;
    }

    void SetSnapProgress(uint32_t progress) {
        snapProgress_ = progress;
    }

    uint32_t GetSnapProgress() const {
        return snapProgress_;
    }

 private:
    SnapshotInfo snapInfo_;  // 快照信息
    uint32_t snapProgress_;  // 快照处理进度百分比
};

class SnapshotServiceManager {
 public:
    SnapshotServiceManager(
        std::shared_ptr<SnapshotTaskManager> taskMgr,
        std::shared_ptr<SnapshotCore> core)
        : taskMgr_(taskMgr),
          core_(core) {}

    virtual ~SnapshotServiceManager() {}

    virtual int Init();
    virtual int Start();
    virtual int Stop();

    virtual int CreateSnapshot(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        UUID *uuid);
    virtual int DeleteSnapshot(UUID uuid,
        const std::string &user,
        const std::string &file);

    virtual int CancelSnapshot(UUID uuid,
        const std::string &user,
        const std::string &file);

    virtual int GetFileSnapshotInfo(const std::string &file,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info);

    virtual int RecoverSnapshotTask();

 private:
    std::shared_ptr<SnapshotTaskManager> taskMgr_;
    std::shared_ptr<SnapshotCore> core_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
