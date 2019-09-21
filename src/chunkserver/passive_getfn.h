/*
 * Project: curve
 * Created Date: Tuesday June 18th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef SRC_CHUNKSERVER_PASSIVE_GETFN_H_
#define SRC_CHUNKSERVER_PASSIVE_GETFN_H_

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

    /**
     * 获取chunkfilepool中剩余chunk的数量
     * @param arg: chunkfilepool的对象指针
     */
    uint32_t getChunkLeftFunc(void* arg);
    /**
     * 获取datastore中chunk文件的数量
     * @param arg: datastore的对象指针
     */
    uint32_t getDatastoreChunkCountFunc(void* arg);
    /**
     * 获取datastore中快照的数量
     * @param arg: datastore的对象指针
     */
    uint32_t getDatastoreSnapshotCountFunc(void* arg);
    /**
     * 获取datastore中clone chunk的数量
     * @param arg: datastore的对象指针
     */
    uint32_t getDatastoreCloneChunkCountFunc(void* arg);

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_PASSIVE_GETFN_H_
