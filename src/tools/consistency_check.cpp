/*
 * Project: curve
 * File Created: Saturday, 29th June 2019 12:39:40 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>

#include "src/tools/consistency_check.h"

DEFINE_string(config_path, "./conf/client.conf", "config path");
DEFINE_string(filename, "", "filename to check consistency");
DEFINE_uint64(filesize, 10*1024*1024*1024ul, "filesize");
DEFINE_uint64(chunksize, 16*1024*1024, "chunksize");
DEFINE_uint64(segmentsize, 1*1024*1024*1024, "segmentsize");
DEFINE_string(username, "test", "user name");
DEFINE_bool(check_hash, true, R"(用户需要先确认copyset的applyindex一致之后
                        再去查copyset内容是不是一致。通常需要先设置
                        check_hash = false先检查copyset的applyindex是否一致
                        如果一致了再设置check_hash = true，
                        检查copyset内容是不是一致)");


bool CheckFileConsistency::Init() {
    curve::client::ClientConfig cc;
    LOG(INFO) << "config path = " << FLAGS_config_path.c_str();
    if (cc.Init(FLAGS_config_path.c_str()) != 0) {
        LOG(ERROR) << "load config failed!";
        return false;
    }

    FileServiceOption_t fsopt = cc.GetFileServiceOption();

    if (mdsclient_.Initialize(fsopt.metaServerOpt) != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "mds client init failed!";
        return false;
    }

    return true;
}

bool CheckFileConsistency::FetchFileCopyset() {
    curve::client::FInfo_t finfo;
    finfo.filename = FLAGS_filename;
    finfo.chunksize = FLAGS_chunksize;
    finfo.segmentsize = FLAGS_segmentsize;

    curve::client::UserInfo_t userinfo;
    userinfo.owner = FLAGS_username;

    for (int i = 0; i < FLAGS_filesize/FLAGS_segmentsize; i++) {
        curve::client::SegmentInfo_t segInfo;

        // 1. 获取文件的copyset id信息
        LIBCURVE_ERROR ret = mdsclient_.GetOrAllocateSegment(false,
                             userinfo, i * FLAGS_segmentsize, &finfo, &segInfo);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "get segment info failed, exit consistency check!";
            return false;
        }

        lpid_ = segInfo.lpcpIDInfo.lpid;

        // 2. 根据copyset id获取copyset server信息
        std::vector<curve::client::CopysetInfo_t> cpinfoVec;
        ret = mdsclient_.GetServerList(segInfo.lpcpIDInfo.lpid,
                                       segInfo.lpcpIDInfo.cpidVec,
                                       &cpinfoVec);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "getServerList info failed, exit consistency check!";
            return false;
        }
        copysetInfo.insert(copysetInfo.end(),
                           cpinfoVec.begin(),
                           cpinfoVec.end());
    }
    return true;
}

bool CheckFileConsistency::ReplicasConsistency() {
    for (auto cpinfo : copysetInfo) {
        std::vector<std::string> copysetHash;
        std::vector<uint64_t> applyIndexVec;

        for (auto iter : cpinfo.csinfos_) {
            int retryCount = 0;
            // 1. 创建到对应chunkserver的channel
            brpc::Channel channel;
            while (channel.Init(butil::endpoint2str(iter.csaddr_.addr_).c_str(),
                                nullptr) !=0) {
                retryCount++;
                if (retryCount > 5) {
                    LOG(ERROR) << "can not create channel to "
                               << iter.csaddr_.ToString().c_str();
                    return false;
                }
            }

            // 2. 发起getcopysetstatus rpc
            brpc::Controller cntl;
            cntl.set_timeout_ms(3000);

            curve::chunkserver::CopysetStatusRequest request;
            curve::chunkserver::CopysetStatusResponse response;

            curve::common::Peer *peer = new curve::common::Peer();
            peer->set_address(iter.csaddr_.ToString());

            request.set_logicpoolid(lpid_);
            request.set_copysetid(cpinfo.cpid_);
            request.set_allocated_peer(peer);;
            request.set_queryhash(FLAGS_check_hash);

            curve::chunkserver::CopysetService_Stub stub(&channel);
            stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                LOG(ERROR) << cntl.ErrorText() << std::endl;
                return false;
            }

            // 3. 存储要检查的内容
            if (FLAGS_check_hash) {
                copysetHash.push_back(response.hash());
            } else {
                applyIndexVec.push_back(response.knownappliedindex());
            }
        }

        // 4. 检查当前copyset的chunkserver内容是否一致
        if (FLAGS_check_hash) {
            if (copysetHash.empty()) {
                LOG(INFO) << "has no copyset info!";
                return true;
            }
            std::string hash = *copysetHash.begin();
            for (auto peerHash : copysetHash) {
                LOG(INFO) << "hash value = " << peerHash.c_str();
                if (peerHash.compare(hash) != 0) {
                    LOG(INFO) << "hash not equal!";
                    return false;
                }
            }
        } else {
            if (applyIndexVec.empty()) {
                LOG(INFO) << "has no copyset info!";
                return true;
            }
            uint64_t index = *applyIndexVec.begin();
            for (auto applyindex : applyIndexVec) {
                if (index != applyindex) {
                    LOG(INFO) << "apply index not equal!";
                    return false;
                }
            }
        }
        applyIndexVec.clear();
        copysetHash.clear();
    }
    return true;
}

void CheckFileConsistency::UnInit() {
    mdsclient_.UnInitialize();
}
