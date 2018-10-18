/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_
#define TEST_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include "src/nameserver2/namespace_storage.h"

namespace curve {
namespace mds {

class MockNameServerStorage : public NameServerStorage {
 public:
    ~MockNameServerStorage() {}

    MOCK_METHOD2(PutFile, StoreStatus(const std::string &,
                                      const FileInfo &));

    MOCK_METHOD2(GetFile, StoreStatus(const std::string &,
                                      FileInfo *));

    MOCK_METHOD1(DeleteFile, StoreStatus(const std::string &));

    MOCK_METHOD4(RenameFile, StoreStatus(const std::string &,
                                         const FileInfo &, const std::string &,
                                         const FileInfo &));

    MOCK_METHOD3(ListFile, StoreStatus(const std::string &,
                          const std::string &, std::vector<FileInfo> * files));

    MOCK_METHOD2(GetSegment, StoreStatus(const std::string &,
                                         PageFileSegment *segment));

    MOCK_METHOD2(PutSegment, StoreStatus(const std::string &,
                                         const PageFileSegment *));

    MOCK_METHOD1(DeleteSegment, StoreStatus(const std::string &));
};

}  // namespace mds
}  // namespace curve

#endif   // TEST_NAMESERVER2_MOCK_NAMESPACE_STORAGE_H_
