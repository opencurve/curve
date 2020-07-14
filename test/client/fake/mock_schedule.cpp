/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Friday, 12th October 2018 10:17:45 am
 * Author: tongguangxun
 */

#include <glog/logging.h>
#include <vector>

#include "test/client/fake/mock_schedule.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"

using curve::client::SegmentInfo;
using curve::client::ChunkInfoDetail;
uint16_t sleeptimeMS = 500;
struct datastruct {
    uint32_t length;
    char* data;
};

char* writebuffer;
int Schedule::ScheduleRequest(
    const std::list<curve::client::RequestContext*> reqlist) {
        // LOG(INFO) << "ENTER MOCK ScheduleRequest";
        char fakedate[10] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k'};
        curve::client::OpType type = curve::client::OpType::UNKNOWN;
        int size = reqlist.size();
        int processed = 0;
        int totallength = 0;
        std::vector<datastruct> datavec;
        LOG(ERROR) << size;

        if (enableScheduleFailed) {
            return -1;
        }

        fiu_do_on("client_request_schedule_sleep",
                  auto func = [&] () {
                    LOG(INFO) << "start sleep! " << sleeptimeMS << " ms";
                    std::this_thread::sleep_for(
                    std::chrono::milliseconds(sleeptimeMS));
                  };
                  func(););

        for (auto iter : reqlist) {
            auto req = iter->done_->GetReqCtx();
            if (iter->optype_ == curve::client::OpType::READ_SNAP) {
                memset(iter->readBuffer_,
                        fakedate[processed%10],
                        iter->rawlength_);
            }

            if (iter->optype_ == curve::client::OpType::GET_CHUNK_INFO) {
                req->seq_ = 1111;
                req->chunkinfodetail_->chunkSn.push_back(2222);
            }

            if (iter->optype_ == curve::client::OpType::READ) {
                memset(iter->readBuffer_,
                        fakedate[processed%10],
                        iter->rawlength_);
                // LOG(ERROR)  << "request split"
                //            << ", off = " << iter->offset_
                //            << ", len = " << iter->rawlength_
                //            << ", seqnum = " << iter->seq_
                //            << ", chunkindex = " << iter->idinfo_.cid_
                //            << ", content = " << fakedate[processed%10]
                //            << ", address = " << &(iter->readBuffer_);
            }

            if (iter->optype_ == curve::client::OpType::WRITE) {
                type = curve::client::OpType::WRITE;
                datastruct datas;
                datas.length = iter->rawlength_;
                datas.data = const_cast<char*>(iter->writeBuffer_);
                totallength += iter->rawlength_;
                datavec.push_back(datas);
            }
            processed++;
            // LOG(INFO) << "current request context chunkID : "
            //            << iter->idinfo_.cid_
            //            << ", copyset id = "
            //            << iter->idinfo_.cpid_
            //            << ", logic pool id ="
            //            << iter->idinfo_.lpid_
            //            << ", offset = "
            //            << iter->offset_
            //            << ", length = "
            //            << iter->rawlength_;
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
