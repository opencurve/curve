/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:38 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include "src/client/client_config.h"
#include "src/client/service_helper.h"

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
    if (finfo->has_fullpathname()) {
        fi->fullPathName = finfo->fullpathname();
    }
}

int ServiceHelper::GetLeader(const LogicPoolID &logicPoolId,
                            const CopysetID &copysetId,
                            const Configuration &conf,
                            PeerId *leaderId) {
    if (conf.empty()) {
        LOG(ERROR) << "Empty group configuration";
        return -1;
    }

    leaderId->reset();
    for (Configuration::const_iterator iter = conf.begin();
         iter != conf.end(); ++iter) {
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
