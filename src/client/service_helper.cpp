/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:38 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <ostream>

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
                            uint32_t backupRequestMs,
                            ChunkServerID* csid,
                            FileMetric_t* fm) {
    if (conf.empty()) {
        LOG(ERROR) << "Empty group configuration";
        return -1;
    }

    int16_t index = -1;
    leaderId->Reset();

    // os中存放copyset所在的chunkserver的地址(不包括leader所在的chunkserver)
    // list://127.0.0.1:12345,127.0.0.1:12346,127.0.0.1:12347,
    std::ostringstream os;
    os << "list://";
    for (auto iter = conf.begin(); iter != conf.end(); ++iter) {
        ++index;
        if (index == currentleaderIndex) {
            LOG(INFO) << "refresh leader skip current leader address: "
                      << iter->csaddr_.ToString().c_str()
                      << ", copysetid = " << copysetId
                      << ", logicpoolid = " << logicPoolId;
            continue;
        }

        os << iter->csaddr_.addr_ << ",";
    }

    LOG(INFO) << "Send GetLeader request to " << os.str()
        << " logicpool id = " << logicPoolId
        << ", copyset id = " << copysetId;

    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.backup_request_ms = backupRequestMs;

    if (channel.Init(os.str().c_str(), "rr", &opts) != 0) {
        LOG(ERROR) << "Fail to init channel to " << os.str();
        return -1;
    }
    curve::chunkserver::CliService2_Stub stub(&channel);
    curve::chunkserver::GetLeaderRequest2 request;
    curve::chunkserver::GetLeaderResponse2 response;

    brpc::Controller cntl;
    cntl.set_timeout_ms(rpcTimeOutMs);

    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);

    stub.GetLeader(&cntl, &request, &response, NULL);
    MetricHelper::IncremGetLeaderRetryTime(fm);

    if (cntl.Failed()) {
        LOG(WARNING) << "GetLeader failed, "
                     << cntl.ErrorText()
                     << ", copyset id = " << copysetId
                     << ", logicpool id = " << logicPoolId;
        return -1;
    }

    bool has_id = response.leader().has_id();
    if (has_id) {
        *csid = response.leader().id();
    }

    bool has_address = response.leader().has_address();
    if (has_address) {
        leaderId->Parse(response.leader().address());
        return leaderId->IsEmpty() ? -1 : 0;
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
