/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_
#define TEST_MDS_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include "src/mds/nameserver2/namespace_storage.h"

namespace curve {
namespace mds {

class MockNameServerStorage : public NameServerStorage {
 public:
    ~MockNameServerStorage() {}

    MOCK_METHOD1(PutFile, StoreStatus(const FileInfo &));

    MOCK_METHOD3(GetFile, StoreStatus(InodeID,
                                      const std::string &,
                                      FileInfo *));

    MOCK_METHOD2(DeleteFile, StoreStatus(InodeID,
                                        const std::string &));

    MOCK_METHOD2(DeleteSnapshotFile, StoreStatus(InodeID,
                                        const std::string &));

    MOCK_METHOD2(RenameFile, StoreStatus(const FileInfo &,
                                         const FileInfo &));

    MOCK_METHOD4(ReplaceFileAndRecycleOldFile, StoreStatus(const FileInfo &,
                                                        const FileInfo &,
                                                        const FileInfo &,
                                                        const FileInfo &));

    MOCK_METHOD2(MoveFileToRecycle, StoreStatus(const FileInfo &,
                                            const FileInfo &));

    MOCK_METHOD3(ListFile, StoreStatus(InodeID,
                                       InodeID,
                                       std::vector<FileInfo> * files));

    MOCK_METHOD3(ListSnapshotFile, StoreStatus(InodeID,
                                       InodeID,
                                       std::vector<FileInfo> * files));

    MOCK_METHOD3(GetSegment, StoreStatus(InodeID,
                                         uint64_t,
                                         PageFileSegment *segment));

    MOCK_METHOD3(PutSegment, StoreStatus(InodeID,
                                         uint64_t,
                                         const PageFileSegment *));

    MOCK_METHOD2(DeleteSegment, StoreStatus(InodeID, uint64_t));

    MOCK_METHOD2(SnapShotFile, StoreStatus(const FileInfo *,
                                    const FileInfo *));
    MOCK_METHOD1(LoadSnapShotFile,
        StoreStatus(std::vector<FileInfo> *snapShotFiles));
};

}  // namespace mds
}  // namespace curve

#endif   // TEST_MDS_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_
