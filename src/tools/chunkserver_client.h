/*
 * Project: curve
 * Created Date: 2019-11-28
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CHUNKSERVER_CLIENT_H_
#define SRC_TOOLS_CHUNKSERVER_CLIENT_H_

#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <braft/builtin_service.pb.h>

#include <string>
#include <iostream>

#include "proto/chunk.pb.h"


namespace curve {
namespace tool {

class ChunkServerClient {
 public:
    virtual ~ChunkServerClient() = default;
    /**
    *  @brief 初始化channel，对一个地址，初始化一次就好
    *  @param csAddr chunkserver地址
    *  @return 成功返回0，失败返回-1
    */
    virtual int Init(const std::string& csAddr);

    /**
    *  @brief 调用braft的RaftStat接口获取复制组的详细信息，放到iobuf里面
    *  @param iobuf 复制组详细信息，返回值为0时有效
    *  @return 成功返回0，失败返回-1
    */
    virtual int GetCopysetStatus(butil::IOBuf* iobuf);

    /**
    *  @brief 检查chunkserver是否在线，只检查controller，不检查response
    *  @return 在线返回true，不在线返回false
    */
    virtual bool CheckChunkServerOnline();

 private:
    brpc::Channel channel_;
    std::string csAddr_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CHUNKSERVER_CLIENT_H_
