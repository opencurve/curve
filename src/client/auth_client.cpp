/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-06-09
 * Author: wanghai (SeanHai)
*/

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <memory>
#include <string>
#include <utility>
#include "src/client/auth_client.h"
#include "proto/auth.pb.h"
#include "src/common/authenticator.h"

namespace curve {
namespace client {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::mds::auth::ClientIdentity;
using ::curve::mds::auth::AuthStatusCode;
using ::curve::mds::auth::TicketAttach;
using ::curve::common::NameLockGuard;

#define RPCTask                                                          \
    [&](int addrindex, uint64_t rpctimeoutMS, brpc::Channel *channel,    \
        brpc::Controller *cntl) -> int

int AuthClient::MapSize() const {
    ReadLockGuard lg(ticketMapMutex_);
    return ticketMap_.size();
}

void AuthClient::Init(const MetaServerOption &rpcOption,
    const AuthClientOption &authOption) {
    if (isInit_.exchange(true)) {
        LOG(INFO) << "AuthClient has been initialized";
        return;
    }
    rpcOpt_ = rpcOption;
    authOpt_ = authOption;
    rpcExcutor_.SetOption(rpcOpt_.rpcRetryOpt);
    if (authOpt_.enable && isStop_.exchange(false)) {
        LOG(INFO) << "start refresh ticket thread, intervalSec = "
                  << authOpt_.ticketRefreshIntervalSec
                  << ", threshold = " << authOpt_.ticketRefreshThresholdSec;
        refreshThread_ = curve::common::Thread(
            &AuthClient::Refresh, this);
    }
    LOG(INFO) << "Init AuthClient " << authOpt_.clientId << " success";
}

void AuthClient::Uninit() {
    if (!isInit_.exchange(false)) {
        return;
    }
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stopping refresh ticket thread";
        sleeper_.interrupt();
        refreshThread_.join();
        LOG(INFO) << "stopped refresh ticket thread";
    }
    ticketMap_.clear();
    authOpt_ = AuthClientOption{};
    rpcOpt_ = MetaServerOption{};
}

void AuthClient::Refresh() {
    while (sleeper_.wait_for(
        std::chrono::seconds(authOpt_.ticketRefreshIntervalSec))) {
        if (MapSize() == 0) {
            continue;
        }
        std::unordered_map<std::string, std::pair<TicketAttach, std::string>>
            tmpMap;
        {
            ReadLockGuard lg(ticketMapMutex_);
            tmpMap = ticketMap_;
        }
        for (const auto &it : tmpMap) {
            auto now = curve::common::TimeUtility::GetTimeofDaySec();
            if (it.second.first.expiration() < now +
                authOpt_.ticketRefreshThresholdSec) {
                RefreshTicket(it.first);
            }
        }
    }
}

bool AuthClient::BuildToken(
    const std::pair<TicketAttach, std::string> &ticketInfo, Token *token) {
    auto now = curve::common::TimeUtility::GetTimeofDaySec();
    // encrypt client id and timestamp
    ClientIdentity cInfo;
    cInfo.set_cid(authOpt_.clientId);
    cInfo.set_timestamp(now);
    std::string cInfoStr;
    if (!cInfo.SerializeToString(&cInfoStr)) {
        LOG(ERROR) << "BuildToken failed when SerializeToString ClientIdentity"
                   << ", cInfo = " << cInfo.ShortDebugString();
        return false;
    }
    std::string encInfoStr;
    int ret = curve::common::Encryptor::AESEncrypt(
        ticketInfo.first.sessionkey(), curve::common::ZEROIV,
        cInfoStr, &encInfoStr);
    if (ret != 0) {
        LOG(ERROR) << "BuildToken failed when encrypt cInfo";
        return false;
    }
    token->set_encticket(ticketInfo.second);
    token->set_encclientidentity(encInfoStr);
    return true;
}

bool AuthClient::DecTicketAttach(const std::string &encTicketAttach,
    TicketAttach *info) {
    // decrypt ticket attach
    bool needRedecByLastKey = false;
    std::string decAttach;
    auto dec = curve::common::Encryptor::AESDecrypt(authOpt_.key,
        curve::common::ZEROIV, encTicketAttach, &decAttach);
    if (dec != 0) {
        needRedecByLastKey = true;
        LOG_IF(ERROR, authOpt_.lastKey.empty())
            << "decrypt ticket attach info failed"
            << ", dec key = " << authOpt_.key;
    }
    // parse from string
    TicketAttach attchInfo;
    if (!info->ParseFromString(decAttach)) {
        needRedecByLastKey = true;
        LOG_IF(ERROR, authOpt_.lastKey.empty())
            << "parse ticket info from string failed"
            << ", dec key = " << authOpt_.key;
    }
    if (!needRedecByLastKey) {
        return true;
    }
    if (authOpt_.lastKey.empty()) {
        LOG(ERROR) << "decrypt ticket attach info failed and no last key";
        return false;
    }
    LOG(INFO) << "decrypt ticket attach info failed and retry with last key"
              << ", key = " << authOpt_.key
              << ", last key = " << authOpt_.lastKey;
    dec = curve::common::Encryptor::AESDecrypt(authOpt_.lastKey,
        curve::common::ZEROIV, encTicketAttach, &decAttach);
    if (dec != 0) {
        LOG(ERROR) << "decrypt ticket attach info failed with last key";
        return false;
    }
    if (!info->ParseFromString(decAttach)) {
        LOG(ERROR) << "parse ticket info from string failed with last key";
        return false;
    }
    return true;
}

bool AuthClient::RefreshTicket(std::string serverId) {
    LOG(INFO) << "RefreshTicket: serverId = " << serverId;
    NameLockGuard nameLG(nameLock_, serverId);
    {
        ReadLockGuard lg(ticketMapMutex_);
        auto now = curve::common::TimeUtility::GetTimeofDaySec();
        auto it = ticketMap_.find(serverId);
        if (it != ticketMap_.end() && it->second.first.expiration() >
            now + authOpt_.ticketRefreshThresholdSec) {
            return true;
        }
    }
    // refresh from remote
    auto task = RPCTask {
        (void)addrindex;
        (void)rpctimeoutMS;
        mds::auth::GetTicketRequest request;
        mds::auth::GetTicketResponse response;
        request.set_sid(serverId);
        request.set_cid(authOpt_.clientId);
        curve::mds::auth::AuthService_Stub stub(channel);
        stub.GetTicket(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            return -cntl->ErrorCode();
        }

        AuthStatusCode retcode = response.status();
        if (retcode == AuthStatusCode::AUTH_OK) {
            TicketAttach attachInfo;
            auto dec = DecTicketAttach(response.encticketattach(), &attachInfo);
            if (!dec) {
                return AuthStatusCode::AUTH_DECRYPT_FAILED;
            }
            // update ticket map
            WriteLockGuard lg(ticketMapMutex_);
            ticketMap_[serverId] = std::make_pair(attachInfo,
                response.encticket());
        } else {
            LOG(ERROR) << "RefreshTicket: serverId = " << serverId
                    << ", errocde = " << retcode
                    << ", error msg = " << AuthStatusCode_Name(retcode);
        }
        return retcode;
    };
    return rpcExcutor_.DoRPCTask(task, rpcOpt_.mdsMaxRetryMS) ==
        AuthStatusCode::AUTH_OK;
}

bool AuthClient::GetToken(const std::string &serverId, Token *token) {
    if (!authOpt_.enable) {
        return true;
    }
    bool find = false;
    std::pair<TicketAttach, std::string> ticketInfo;
    {
        ReadLockGuard lg(ticketMapMutex_);
        auto now = curve::common::TimeUtility::GetTimeofDaySec();
        auto it = ticketMap_.find(serverId);
        if (it != ticketMap_.end()) {
            find = now < it->second.first.expiration();
            ticketInfo = it->second;
        }
    }
    if (find) {
        return BuildToken(ticketInfo, token);
    }
    if (RefreshTicket(serverId)) {
        ReadLockGuard lg(ticketMapMutex_);
        auto now = curve::common::TimeUtility::GetTimeofDaySec();
        auto it = ticketMap_.find(serverId);
        if (it != ticketMap_.end() && now < it->second.first.expiration()) {
            return BuildToken(it->second, token);
        }
        LOG(WARNING) << "Refresh ticket success, but ticket expired"
                     << ", serverId = " << serverId
                     << ", now = " << now
                     << ", expiration = " << it->second.first.expiration();
    }
    return false;
}

}  // namespace client
}  // namespace curve
