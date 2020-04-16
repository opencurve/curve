/*
 * Project: curve
 * Created Date: Tuesday December 24th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_RAFTSNAPSHOT_FILESYSTEM_ADAPTOR_H_
#define TEST_CHUNKSERVER_MOCK_RAFTSNAPSHOT_FILESYSTEM_ADAPTOR_H_

#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/chunkserver/raftsnapshot_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {

class MockRaftSnapshotFilesystemAdaptor : public RaftSnapshotFilesystemAdaptor {
 public:
    MockRaftSnapshotFilesystemAdaptor() {}
    ~MockRaftSnapshotFilesystemAdaptor() {}
    MOCK_METHOD4(open, braft::FileAdaptor*(const std::string&,
                                           int,
                                           const ::google::protobuf::Message*,
                                           butil::File::Error*));
    MOCK_METHOD2(delete_file, bool(const std::string&, bool));
    MOCK_METHOD2(rename, bool(const std::string&, const std::string&));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_RAFTSNAPSHOT_FILESYSTEM_ADAPTOR_H_
