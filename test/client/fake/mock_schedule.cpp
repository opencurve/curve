/*
 * Project: curve
 * File Created: Friday, 12th October 2018 10:17:45 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>
#include <vector>
#include "test/client/fake/mock_schedule.h"


struct datastruct {
    uint32_t length;
    char* data;
};

char* writebuffer;
int Schedule::ScheduleRequest(
    const std::list<curve::client::RequestContext*>& reqlist) {
        LOG(INFO) << "ENTER MOCK ScheduleRequest";
        char fakedate[10] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k'};
        curve::client::OpType type = curve::client::OpType::UNKNOWN;
        int size = reqlist.size();
        int processed = 0;
        int totallength = 0;
        std::vector<datastruct> datavec;
        // assume less than 10 request after split
        for (auto iter : reqlist) {
            if (iter->optype_ == curve::client::OpType::READ) {
                memset(const_cast<char*>(iter->data_),
                        fakedate[processed],
                        iter->rawlength_);
            } else {
                type = curve::client::OpType::WRITE;
                datastruct datas;
                datas.length = iter->rawlength_;
                datas.data = const_cast<char*>(iter->data_);
                totallength += iter->rawlength_;
                datavec.push_back(datas);
            }
            processed++;
            LOG(INFO) << "current request context chunkID : "
                       << iter->chunkid_
                       << ", copyset id = "
                       << iter->copysetid_
                       << ", logic pool id ="
                       << iter->logicpoolid_
                       << ", offset = "
                       << iter->offset_
                       << ", length = "
                       << iter->rawlength_
                       << ", first data = "
                       << iter->data_[0];
            iter->done_->SetFailed(0);
            iter->done_->Run();
            if (processed >= size) {
                if (type == curve::client::OpType::WRITE) {
                    writebuffer = new char[totallength];
                    uint32_t tempoffert = 0;
                    for (auto it : datavec) {
                        memcpy(writebuffer + tempoffert, it.data, it.length);
                        tempoffert += it.length;
                    }
                }
                break;
            }
        }
        return 0;
}
