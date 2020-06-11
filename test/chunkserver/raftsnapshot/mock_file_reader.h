/*
 * Project: curve
 * Created Date: 2020-06-17
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_
#define TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <string>
#include <vector>
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class MockFileReader : public CurveSnapshotFileReader {
 public:
    MockFileReader(braft::FileSystemAdaptor* fs,
                   braft::SnapshotThrottle* throttle) :
                        CurveSnapshotFileReader(fs, "", throttle) {}
    virtual ~MockFileReader() {}
    MOCK_CONST_METHOD7(read_file, int(butil::IOBuf*, const std::string&,
                          off_t, size_t, bool, size_t*, bool*));
    MOCK_CONST_METHOD0(path, const std::string&());
};

}  // namespace chunkserver
}  // namespace curve

#endif  //  TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_FILE_READER_H_
