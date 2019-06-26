/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:38 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include "src/client/client_config.h"
#include "src/client/service_helper.h"
#include "src/client/client_metric.h"

namespace curve {
namespace client {

void ServiceHelper::ProtoFileInfo2Local(curve::mds::FileInfo* finfo,
                                        FInfo_t* fi) {
    if (finfo->has_owner()) {
        fi->owner = finfo->owner();
    }
    if (finfo->has_filename()) {
        fi->filename = finfo->filename();
    }
    if (finfo->has_id()) {
        fi->id = finfo->id();
    }
    if (finfo->has_parentid()) {
        fi->parentid = finfo->parentid();
    }
    if (finfo->has_filetype()) {
        fi->filetype = static_cast<FileType>(finfo->filetype());
    }
    if (finfo->has_chunksize()) {
        fi->chunksize = finfo->chunksize();
    }
    if (finfo->has_length()) {
        fi->length = finfo->length();
    }
    if (finfo->has_ctime()) {
        fi->ctime = finfo->ctime();
    }
    if (finfo->has_chunksize()) {
        fi->chunksize = finfo->chunksize();
    }
    if (finfo->has_segmentsize()) {
        fi->segmentsize = finfo->segmentsize();
    }
    if (finfo->has_seqnum()) {
        fi->seqnum = finfo->seqnum();
    }
    if (finfo->has_filestatus()) {
        fi->filestatus = (FileStatus)finfo->filestatus();
    }
}

int ServiceHelper::GetLeader(const LogicPoolID &logicPoolId,
                            const CopysetID &copysetId,
                            const std::vector<CopysetPeerInfo_t> &conf,
                            ChunkServerAddr *leaderId,
                            int16_t currentleaderIndex,
                            uint32_t rpcTimeOutMs,
                            ChunkServerID* csid,
                            FileMetric_t* fm) {
    if (conf.empty()) {
        LOG(ERROR) << "Empty group configuration";
        return -1;
    }

    int16_t index = -1;
    leaderId->Reset();
    for (auto iter = conf.begin(); iter != conf.end(); ++iter) {
        ++index;
        if (index == currentleaderIndex) {
            LOG(INFO) << "skip current server address!";
            continue;
        }

        brpc::Channel channel;
        if (channel.Init(iter->csaddr_.addr_, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to"
                        << iter->csaddr_.ToString().c_str();
            return -1;
        }
        curve::chunkserver::CliService2_Stub stub(&channel);
        curve::chunkserver::GetLeaderRequest2 request;
        curve::chunkserver::GetLeaderResponse2 response;
        curve::common::Peer* peer = new (std::nothrow) curve::common::Peer;
        if (peer == nullptr) {
            LOG(ERROR) << "allocate peer failed!";
            return -1;
        }

        peer->set_id(iter->chunkserverid_);
        peer->set_address(iter->csaddr_.ToString());

        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeOutMs);

        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_peer(peer);

        stub.GetLeader(&cntl, &request, &response, NULL);
        MetricHelper::IncremGetLeaderRetryTime(fm);

        if (cntl.Failed()) {
            LOG(ERROR) << "GetLeader failed, "
                       << cntl.ErrorText();
            continue;
        }

        if (response.leader().has_id()) {
            *csid = response.leader().id();
        }

        if (response.leader().has_address()) {
            leaderId->Parse(response.leader().address());
            return leaderId->IsEmpty() ? -1 : 0;
        }
    }

    return -1;
}

bool ServiceHelper::GetUserInfoFromFilename(const std::string& filename,
                                            std::string* realfilename,
                                            std::string* user) {
    auto user_end = filename.find_last_of("_");
    auto user_begin = filename.find_last_of("_", user_end - 1);

    if (user_end == filename.npos || user_begin == filename.npos) {
        LOG(ERROR) << "get user info failed!";
        return false;
    }

    *realfilename = filename.substr(0, user_begin);
    *user = filename.substr(user_begin + 1, user_end - user_begin - 1);

    LOG(INFO) << "user info [" << *user << "]";
    return true;
}
}   // namespace client
}   // namespace curve
