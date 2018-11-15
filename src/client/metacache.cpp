/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:35 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "proto/cli.pb.h"

#include "src/client/metacache.h"
#include "src/client/client_common.h"

DECLARE_uint32(chunk_size);
DEFINE_uint32(get_leader_retry,
                3,
                "get leader from chunkserver, max try times.");

namespace curve {
namespace client {
    MetaCache::MetaCache(Session* session) {
        session_ = session;
    }

    MetaCache::~MetaCache() {
        chunkindex2idMap_.clear();
        chunkid2cslpMap_.clear();
        lpcsid2serverlistMap_.clear();
    }

    int MetaCache::GetChunkInfo(ChunkIndex chunkidx, Chunkinfo_t* chunxinfo ) {
        spinlock4ChunkInfo_.Lock();
        auto iter = chunkindex2idMap_.find(chunkidx);
        if (iter != chunkindex2idMap_.end()) {
            spinlock4ChunkInfo_.UnLock();
            *chunxinfo = iter->second;
            return 0;
        }
        spinlock4ChunkInfo_.UnLock();

        /**
         * offset + length > segment size, so IO has some chunkindex cross other segment
         */ 
        session_->GetOrAllocateSegment((off_t)chunkidx * FLAGS_chunk_size);

        spinlock4ChunkInfo_.Lock();
        auto it = chunkindex2idMap_.find(chunkidx);
        if (it != chunkindex2idMap_.end()) {
            spinlock4ChunkInfo_.UnLock();
            *chunxinfo = it->second;
            return 0;
        }
        spinlock4ChunkInfo_.UnLock();
        LOG(ERROR) << "chunk index  "
                    << chunkidx
                    << " does not exit in cache!";
        /**
         * TODO(tongguangxun) :MDS got no segment allocate, how to process this error.
         */ 
        return -1;
    }

    /**
     * 1. check the logicPoolId and CopysetID assosiate serverlist exists or not
     * 2. if server list exist, go next
     * 3. if server list do not exists, invoke session GetServerList
     * 4. check exists again, if not exists, return failed
     * 5. if exists, check if need refresh
     * 6. if need refresh, we invoke chunkserver cli getleader service
     * 7. if no need refresh, just return the leader id and server addr
     */
    int MetaCache::GetLeader(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkServerID* serverId,
                  EndPoint* serverAddr,
                  bool refresh) {
        std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

        spinlock4CopysetInfo_.Lock();
        auto iter = lpcsid2serverlistMap_.find(mapkey);
        if (iter == lpcsid2serverlistMap_.end()) {
            std::vector<CopysetID> temp;
            temp.push_back(copysetId);

            spinlock4CopysetInfo_.UnLock();
            session_->GetServerList(logicPoolId, temp);
            spinlock4CopysetInfo_.Lock();

            iter = lpcsid2serverlistMap_.find(mapkey);
            if (iter == lpcsid2serverlistMap_.end() ||
                !iter->second.IsValid()) {
                spinlock4CopysetInfo_.UnLock();
                return -1;
            }
        }
        spinlock4CopysetInfo_.UnLock();

        auto getleader = [&]() ->int {
            Configuration cfg;
            for (auto it : iter->second.csinfos_) {
                cfg.add_peer(it.peerid_);
            }
            PeerId  leaderid;
            int ret = GetLeader(logicPoolId,
                                copysetId,
                                cfg,
                                &leaderid);
            if (ret == -1) {
                return -1;
            }
            iter->second.ChangeLeaderID(leaderid);
            return 0;
        };

        int ret = 0;
        if (refresh) {
            uint32_t retry = 0;
            while (retry++ < FLAGS_get_leader_retry) {
                ret = getleader();
                if (ret != -1) {
                    break;
                }
            }
        }

        if (ret == -1) {
            return -1;
        }
        return iter->second.GetLeaderInfo(serverId, serverAddr);
    }

    CopysetInfo_t MetaCache::GetServerList(LogicPoolID logicPoolId,
                                            CopysetID copysetId) {
        std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
        CopysetInfo_t ret;
        spinlock4CopysetInfo_.Lock();
        auto iter = lpcsid2serverlistMap_.find(mapkey);
        if (iter == lpcsid2serverlistMap_.end()) {
            /**
             * it's impossible to get here,
             * if the IO comes here, we already get the 
             * server list from mds.
             */ 
            spinlock4CopysetInfo_.UnLock();
            return ret;
        }
        spinlock4CopysetInfo_.UnLock();
        return iter->second;
    }

    /**
     * when copyset client find that the leader is redirect,
     * the copyset client will call UpdateLeader.
     * return the ChunkServerID to invoker
     */ 
    int MetaCache::UpdateLeader(LogicPoolID logicPoolId,
                    CopysetID copysetId,
                    ChunkServerID* leaderId,
                    const EndPoint &leaderAddr) {
        std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

        int ret = 0;
        spinlock4CopysetInfo_.Lock();
        auto iter = lpcsid2serverlistMap_.find(mapkey);
        if (iter == lpcsid2serverlistMap_.end()) {
            /**
             * it's impossible to get here
             */ 
            spinlock4CopysetInfo_.UnLock();
            return -1;
        }
        ret = iter->second.UpdateLeaderAndGetChunkserverID(leaderId, leaderAddr); // NOLINT
        spinlock4CopysetInfo_.UnLock();
        return ret;
    }

    void MetaCache::UpdateChunkInfo(ChunkIndex cindex, Chunkinfo_t cinfo) {
        spinlock4ChunkInfo_.Lock();
        chunkindex2idMap_[cindex] = cinfo;
        spinlock4ChunkInfo_.UnLock();
    }

    void MetaCache::UpdateCopysetIDInfo(ChunkID cid,
                                        LogicPoolID logicPoolid,
                                        CopysetID copysetid) {
        CopysetIDInfo_t lpcsinfo(logicPoolid, copysetid);
        spinlock4CopysetIDInfo_.Lock();
        chunkid2cslpMap_[cid] = lpcsinfo;
        spinlock4CopysetIDInfo_.UnLock();
    }
    void MetaCache::UpdateCopysetInfo(LogicPoolID logicPoolid,
                                            CopysetID copysetid,
                                            CopysetInfo_t cslist) {
        auto key = LogicPoolCopysetID2Str(logicPoolid, copysetid);
        spinlock4CopysetInfo_.Lock();
        lpcsid2serverlistMap_[key] = cslist;
        spinlock4CopysetInfo_.UnLock();
    }


    int MetaCache::GetLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration &conf,
                        PeerId *leaderId) {
        if (conf.empty()) {
            LOG(ERROR) << "Empty group configuration";
            return -1;
        }
        /** Construct a brpc naming service to 
         * access all the nodes in this group 
         */
        leaderId->reset();
        for (Configuration::const_iterator
                iter = conf.begin(); iter != conf.end(); ++iter) {
            brpc::Channel channel;
            if (channel.Init(iter->addr, NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to"
                            << iter->to_string().c_str();
                return -1;
            }
            curve::chunkserver::CliService_Stub stub(&channel);
            curve::chunkserver::GetLeaderRequest request;
            curve::chunkserver::GetLeaderResponse response;
            brpc::Controller cntl;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_peer_id(iter->to_string());
            stub.get_leader(&cntl, &request, &response, NULL);
            if (cntl.Failed()) {
                LOG(ERROR) << "GetLeader failed, "
                            << cntl.ErrorText();
                continue;
            }
            leaderId->parse(response.leader_id());
        }
        if (leaderId->is_empty()) {
            return -1;
        }

        return 0;
    }
    void MetaCache::UpdateAppliedIndex(LogicPoolID logicPoolId,
                                    CopysetID copysetId,
                                    uint64_t appliedindex) {
        std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
        spinlock4CopysetInfo_.Lock();
        auto iter = lpcsid2serverlistMap_.find(mapkey);
        if (iter == lpcsid2serverlistMap_.end()) {
            spinlock4CopysetInfo_.UnLock();
            return;
        }
        iter->second.UpdateAppliedIndex(appliedindex);
        spinlock4CopysetInfo_.UnLock();
    }

    uint64_t MetaCache::GetAppliedIndex(LogicPoolID logicPoolId,
                                        CopysetID copysetId) {
        std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
        spinlock4CopysetInfo_.Lock();
        auto iter = lpcsid2serverlistMap_.find(mapkey);
        if (iter == lpcsid2serverlistMap_.end()) {
            spinlock4CopysetInfo_.UnLock();
            return 0;
        }
        int appliedindex = iter->second.GetAppliedIndex();
        spinlock4CopysetInfo_.UnLock();
        return appliedindex;
    }


}   // namespace client
}   // namespace curve
