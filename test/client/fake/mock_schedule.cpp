/*
 * Project: curve
 * File Created: Friday, 12th October 2018 10:17:45 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>
#include <vector>

#include "test/client/fake/mock_schedule.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"

using curve::client::SegmentInfo;
using curve::client::ChunkInfoDetail;

struct datastruct {
    uint32_t length;
    char* data;
};

char* writebuffer;
int Schedule::ScheduleRequest(
    const std::list<curve::client::RequestContext*> reqlist) {
        LOG(INFO) << "ENTER MOCK ScheduleRequest";
        char fakedate[10] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k'};
        curve::client::OpType type = curve::client::OpType::UNKNOWN;
        int size = reqlist.size();
        int processed = 0;
        int totallength = 0;
        std::vector<datastruct> datavec;
        LOG(ERROR) << size;

        for (auto iter : reqlist) {
            auto req = iter->done_->GetReqCtx();
            if (iter->optype_ == curve::client::OpType::READ_SNAP) {
                memset(const_cast<char*>(iter->data_),
                        fakedate[processed%10],
                        iter->rawlength_);
            }

            if (iter->optype_ == curve::client::OpType::GET_CHUNK_INFO) {
                req->seq_ = 1111;
                req->chunkinfodetail_->chunkSn.push_back(2222);
            }

            if (iter->optype_ == curve::client::OpType::READ) {
                memset(const_cast<char*>(iter->data_),
                        fakedate[processed%10],
                        iter->rawlength_);
                LOG(ERROR)  << "request split"
                            << ", off = " << iter->offset_
                            << ", len = " << iter->rawlength_
                            << ", seqnum = " << iter->seq_
                            << ", chunkindex = " << iter->idinfo_.cid_
                            << ", content = " << fakedate[processed%10]
                            << ", address = " << &(iter->data_);
            }

            if (iter->optype_ == curve::client::OpType::WRITE) {
                type = curve::client::OpType::WRITE;
                datastruct datas;
                datas.length = iter->rawlength_;
                datas.data = const_cast<char*>(iter->data_);
                totallength += iter->rawlength_;
                datavec.push_back(datas);
            }
            processed++;
            LOG(INFO) << "current request context chunkID : "
                       << iter->idinfo_.cid_
                       << ", copyset id = "
                       << iter->idinfo_.cpid_
                       << ", logic pool id ="
                       << iter->idinfo_.lpid_
                       << ", offset = "
                       << iter->offset_
                       << ", length = "
                       << iter->rawlength_;
            if (processed >= size) {
                if (type == curve::client::OpType::WRITE) {
                    writebuffer = new char[totallength];
                    uint32_t tempoffert = 0;
                    for (auto it : datavec) {
                        memcpy(writebuffer + tempoffert, it.data, it.length);
                        tempoffert += it.length;
                    }
                }
                iter->done_->SetFailed(0);
                iter->done_->Run();
                break;
            }
            iter->done_->SetFailed(0);
            iter->done_->Run();
        }
        return 0;
}
