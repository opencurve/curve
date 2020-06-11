/*
 * Project: curve
 * Created Date: 2020-06-17
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_
#define TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <string>
#include <vector>
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class MockSnapshotAttachment : public CurveSnapshotAttachment {
 public:
    MockSnapshotAttachment() : CurveSnapshotAttachment(nullptr) {}
    virtual ~MockSnapshotAttachment() {}
    MOCK_METHOD2(list_attach_files, void(std::vector<std::string>*,
                                 const std::string&));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_
