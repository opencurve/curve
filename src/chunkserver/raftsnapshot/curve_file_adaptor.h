/*
 * Project: curve
 * Created Date: 2020-06-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_

#include <braft/file_system_adaptor.h>

namespace curve {
namespace chunkserver {

class CurveFileAdaptor : public braft::PosixFileAdaptor {
 public:
    explicit CurveFileAdaptor(int fd) : PosixFileAdaptor(fd) {}
    // close之前必须先sync，保证数据落盘，其他逻辑不变
    bool close() override {
        return sync() && braft::PosixFileAdaptor::close();
    }
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_ADAPTOR_H_
