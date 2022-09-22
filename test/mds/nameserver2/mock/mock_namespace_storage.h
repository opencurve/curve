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
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_MOCK_NAMESPACE_STORAGE_H_
#define  TEST_MDS_NAMESERVER2_MOCK_MOCK_NAMESPACE_STORAGE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <map>
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

    MOCK_METHOD4(PutSegment, StoreStatus(InodeID,
                                         uint64_t,
                                         const PageFileSegment *,
                                         int64_t *));

    MOCK_METHOD3(DeleteSegment, StoreStatus(InodeID, uint64_t, int64_t*));

    MOCK_METHOD2(SnapShotFile, StoreStatus(const FileInfo *,
                                    const FileInfo *));
    MOCK_METHOD1(LoadSnapShotFile,
        StoreStatus(std::vector<FileInfo> *snapShotFiles));
    MOCK_METHOD2(ListSegment,
        StoreStatus(InodeID, std::vector<PageFileSegment>*));

    MOCK_METHOD2(DiscardSegment,
                 StoreStatus(const FileInfo&, const PageFileSegment&));
    MOCK_METHOD3(CleanDiscardSegment,
                 StoreStatus(uint64_t, const std::string&, int64_t*));
    MOCK_METHOD1(ListDiscardSegment,
                 StoreStatus(std::map<std::string, DiscardSegmentInfo>*));
    MOCK_METHOD2(PutFilePermInfo, StoreStatus(const uint64_t,
                 const WriterLockInfo&));
    MOCK_METHOD2(GetFilePermInfo, StoreStatus(const uint64_t, WriterLockInfo*));
    MOCK_METHOD1(ClearPermInfo, StoreStatus(const uint64_t));
};

}  // namespace mds
}  // namespace curve

#endif   // TEST_MDS_NAMESERVER2_MOCK_MOCK_NAMESPACE_STORAGE_H_
