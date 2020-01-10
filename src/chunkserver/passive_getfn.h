/*
 * Project: curve
 * Created Date: Tuesday June 18th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef SRC_CHUNKSERVER_PASSIVE_GETFN_H_
#define SRC_CHUNKSERVER_PASSIVE_GETFN_H_

#include "src/chunkserver/trash.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

    /**
     * 获取datastore中chunk文件的数量
     * @param arg: datastore的对象指针
     */
    uint32_t GetDatastoreChunkCountFunc(void* arg);
    /**
     * 获取datastore中快照chunk的数量
     * @param arg: datastore的对象指针
     */
    uint32_t GetDatastoreSnapshotCountFunc(void* arg);
    /**
     * 获取datastore中clone chunk的数量
     * @param arg: datastore的对象指针
     */
    uint32_t GetDatastoreCloneChunkCountFunc(void* arg);
    /**
     * 获取chunkserver上chunk文件的数量
     * @param arg: nullptr
     */
    uint32_t GetTotalChunkCountFunc(void* arg);
    /**
     * 获取chunkserver上快照chunk的数量
     * @param arg: nullptr
     */
    uint32_t GetTotalSnapshotCountFunc(void* arg);
    /**
     * 获取chunkserver上clone chunk的数量
     * @param arg: nullptr
     */
    uint32_t GetTotalCloneChunkCountFunc(void* arg);
    /**
     * 获取chunkfilepool中剩余chunk的数量
     * @param arg: chunkfilepool的对象指针
     */
    uint32_t GetChunkLeftFunc(void* arg);
    /**
     * 获取trash中chunk的数量
     * @param arg: trash的对象指针
     */
    uint32_t GetChunkTrashedFunc(void* arg);

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_PASSIVE_GETFN_H_
