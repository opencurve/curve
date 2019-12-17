/*
 * Project: curve
 * File Created: Saturday, 29th June 2019 12:39:40 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>

#include "src/tools/consistency_check.h"

DEFINE_string(client_config_path, "./conf/client.conf", "config path");
DEFINE_string(filename, "", "filename to check consistency");
DEFINE_uint64(filesize, 10*1024*1024*1024ul, "filesize");
DEFINE_uint64(chunksize, 16*1024*1024, "chunksize");
DEFINE_uint64(segmentsize, 1*1024*1024*1024, "segmentsize");
DEFINE_uint64(retry_times, 5, "rpc retry times");
DEFINE_string(username, "test", "user name");
DEFINE_bool(check_hash, true, R"(用户需要先确认copyset的applyindex一致之后
                        再去查copyset内容是不是一致。通常需要先设置
                        check_hash = false先检查copyset的applyindex是否一致
                        如果一致了再设置check_hash = true，
                        检查copyset内容是不是一致)");


using curve::chunkserver::COPYSET_OP_STATUS;

bool CheckFileConsistency::Init() {
    curve::client::ClientConfig cc;
    if (cc.Init(FLAGS_client_config_path.c_str()) != 0) {
        std::cout << "Load config failed, config path: "
                  << FLAGS_client_config_path.c_str() << std::endl;
        return false;
    }

    FileServiceOption_t fsopt = cc.GetFileServiceOption();

    if (mdsclient_.Initialize(fsopt.metaServerOpt) != LIBCURVE_ERROR::OK) {
        std::cout << "Mds client init failed!" << std::endl;
        return false;
    }

    return true;
}

void CheckFileConsistency::PrintHelp() {
    std::cout << "Example: " << std::endl;
    std::cout << "curve_ops_tool check-consistency -client_config_path=conf/client.conf -filename=test -filesize=12345678 "  // NOLINT
    "-chunksize=1234 -segmentsize=123456 -retry_times=3 -username=test -check_hash=false"  << std::endl;  // NOLINT
}

bool CheckFileConsistency::FetchFileCopyset() {
    curve::client::FInfo_t finfo;
    finfo.fullPathName = FLAGS_filename;
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
            std::cout << "Get segment info failed, exit consistency check!"
                      << std::endl;
            return false;
        }

        lpid_ = segInfo.lpcpIDInfo.lpid;

        // 2. 根据copyset id获取copyset server信息
        std::vector<curve::client::CopysetInfo_t> cpinfoVec;
        ret = mdsclient_.GetServerList(segInfo.lpcpIDInfo.lpid,
                                       segInfo.lpcpIDInfo.cpidVec,
                                       &cpinfoVec);
        if (ret != LIBCURVE_ERROR::OK) {
            std::cout << "GetServerList info failed, exit consistency check!"
                      << std::endl;
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
                    std::cout << "Can not create channel to "
                              << iter.csaddr_.ToString().c_str() << std::endl;
                    return false;
                }
            }

            int retry = 0;
            while (retry < FLAGS_retry_times) {
                // 2. 发起getcopysetstatus rpc
                brpc::Controller cntl;
                cntl.set_timeout_ms(3000);

                curve::chunkserver::CopysetStatusRequest request;
                curve::chunkserver::CopysetStatusResponse response;

                curve::common::Peer *peer = new curve::common::Peer();
                peer->set_address(iter.csaddr_.ToString());

                request.set_logicpoolid(lpid_);
                request.set_copysetid(cpinfo.cpid_);
                request.set_allocated_peer(peer);
                request.set_queryhash(FLAGS_check_hash);

                curve::chunkserver::CopysetService_Stub stub(&channel);
                stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
                if (cntl.Failed()) {
                    std::cout << "GetCopysetStatus from "
                              << iter.csaddr_.ToString().c_str()
                              << " fail, error content: "
                              << cntl.ErrorText() << std::endl;
                } else {
                    if (response.status() !=
                            COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
                        std::cout << "GetCopysetStatus of " << cpinfo.cpid_
                                  << " from "
                                  << iter.csaddr_.ToString().c_str()
                                  << " fail, status code: "
                                  << response.status() << std::endl;
                        return false;
                    }
                    // 3. 存储要检查的内容
                    if (FLAGS_check_hash) {
                        copysetHash.push_back(response.hash());
                    } else {
                        applyIndexVec.push_back(response.knownappliedindex());
                    }
                    break;
                }
                retry++;
            }

            if (retry == FLAGS_retry_times) {
                std::cout << "GetCopysetStatus rpc timeout!" << std::endl;
                return false;
            }
        }

        // 4. 检查当前copyset的chunkserver内容是否一致
        if (FLAGS_check_hash) {
            if (copysetHash.empty()) {
                std::cout << "Has no copyset hash info!" << std::endl;
                return true;
            }
            std::string hash = *copysetHash.begin();
            for (auto peerHash : copysetHash) {
                if (peerHash.compare(hash) != 0) {
                    std::cout << "Hash not equal! previous hash = " << hash
                              << ", current hash = " << peerHash
                              << ", copyset id = " << cpinfo.cpid_
                              << ", logical pool id = " << lpid_ << std::endl;
                    return false;
                }
            }
        } else {
            if (applyIndexVec.empty()) {
                std::cout << "Has no copyset apply index info!" << std::endl;
                return true;
            }
            uint64_t index = *applyIndexVec.begin();
            for (auto applyindex : applyIndexVec) {
                if (index != applyindex) {
                    std::cout << "Apply index not equal! previous apply index "
                              << index << ", current index = " << applyindex
                              << ", copyset id = " << cpinfo.cpid_
                              << ", logical pool id = " << lpid_ << std::endl;
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
