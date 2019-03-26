/*
 * Project: curve
 * Created Date: Tuesday December 11th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_CLEAN_MANAGER_H_
#define TEST_MDS_NAMESERVER2_MOCK_CLEAN_MANAGER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"


namespace curve {
namespace mds {
class MockCleanManager: public CleanManagerInterface {
 public:
    ~MockCleanManager() {}
    MOCK_METHOD2(SubmitDeleteSnapShotFileJob, bool(const FileInfo&,
        std::shared_ptr<AsyncDeleteSnapShotEntity>));
    MOCK_METHOD1(GetTask, std::shared_ptr<Task>(TaskIDType id));
};

}  // namespace mds
}  // namespace curve


#endif  // TEST_MDS_NAMESERVER2_MOCK_CLEAN_MANAGER_H_
