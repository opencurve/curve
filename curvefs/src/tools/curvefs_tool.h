/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_

#include <brpc/channel.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <json/json.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/curvefs_tool_metric.h"
#include "src/common/configuration.h"

DECLARE_string(confPath);
DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curvefs {
namespace tools {

class CurvefsTool {
 public:
    CurvefsTool() {}
    CurvefsTool(const std::string& command,
                const std::string& programe = std::string(kProgrameName),
                bool show = true)
        : command_(command), programe_(programe), show_(show) {}
    virtual ~CurvefsTool() {}

    virtual void PrintHelp() {
        std::cout << "Example :" << std::endl;
        std::cout << programe_ << " " << command_ << " " << kConfPathHelp;
    }

    virtual int Run() {
        if (Init() < 0) {
            return -1;
        }
        int ret = RunCommand();
        return ret;
    }

    /**
     * @brief configure the environment for the command
     *
     * @details
     */
    virtual int Init() = 0;
    virtual int RunCommand() = 0;

 protected:
    std::string command_;
    std::string programe_;
    bool show_;  // Control whether the command line is output
};

/**
 * @brief this base class used for curvefs tool with rpc
 *
 * @tparam ChannelT
 * @tparam ControllerT
 * @tparam RequestT
 * @tparam ResponseT
 * @tparam ServiceT
 * @details
 * you can take umountfs as example
 */
template <class ChannelT, class ControllerT, class RequestT, class ResponseT,
          class ServiceT>
class CurvefsToolRpc : public CurvefsTool {
 public:
    CurvefsToolRpc(const std::string& command,
                   const std::string& programe = std::string(kProgrameName),
                   bool show = true)
        : CurvefsTool(command, programe, show) {}

    int Init(const std::shared_ptr<ChannelT>& channel,
             const std::shared_ptr<ControllerT>& controller,
             const std::queue<RequestT>& requestQueue,
             const std::shared_ptr<ResponseT>& response,
             const std::shared_ptr<ServiceT>& service_stub,
             std::function<void(ControllerT*, RequestT*, ResponseT*)>
                 service_stub_func) {
        channel_ = channel;
        controller_ = controller;
        requestQueue_ = requestQueue;
        response_ = response;
        service_stub_ = service_stub;
        service_stub_func_ = service_stub_func;
        InitHostsAddr();
        return 0;
    }

    virtual int Init() {
        channel_ = std::make_shared<ChannelT>();
        controller_ = std::make_shared<ControllerT>();
        // request[0] could be re-assigned by subclass
        requestQueue_.push(RequestT());
        response_ = std::make_shared<ResponseT>();
        service_stub_ = std::make_shared<ServiceT>(channel_.get());

        // add need update FlagInfos
        AddUpdateFlags();
        UpdateFlags();
        InitHostsAddr();
        return 0;
    }

    virtual void SetRequestQueue(const std::queue<RequestT>& requestQueue) {
        requestQueue_ = std::move(requestQueue);
    }

    virtual const std::shared_ptr<ResponseT>& GetResponse() {
        return response_;
    }

    virtual int RunCommand() {
        int ret = 0;
        while (!requestQueue_.empty()) {
            if (!SendRequestToServices()) {
                std::cerr << "send request to host: [ ";
                for (const auto& i : hostsAddr_) {
                    std::cerr << i << " ";
                }
                std::cerr << "] failed." << std::endl;
                ret = -1;
            }

            requestQueue_.pop();
        }

        return ret;
    }

    virtual void InitHostsAddr() {}

 protected:
    /**
     * @brief send request to host in hostsAddr_
     *
     * @return true
     * @return false
     * @details
     * as long as one succeeds, it returns true and ends sending
     */
    virtual bool SendRequestToServices() {
        for (const std::string& host : hostsAddr_) {
            if (channel_->Init(host.c_str(), nullptr) != 0) {
                std::cerr << "fail init channel to host: " << host << std::endl;
                continue;
            }
            // if service_stub_func_ does not assign a value
            // it will crash in there
            service_stub_func_(controller_.get(), &requestQueue_.front(),
                               response_.get());
            if (AfterSendRequestToHost(host) == true) {
                return true;
            }
            controller_->Reset();
        }
        // send request to all host failed
        return false;
    }

    void AddUpdateFlagsFunc(
        const std::function<void(curve::common::Configuration*,
                                 google::CommandLineFlagInfo*)>& func) {
        updateFlagsFunc_.push_back(func);
    }

    virtual void UpdateFlags() {
        curve::common::Configuration conf;
        conf.SetConfigPath(FLAGS_confPath);
        if (!conf.LoadConfig()) {
            std::cerr << "load configure file " << FLAGS_confPath << " failed!"
                      << std::endl;
        }
        google::CommandLineFlagInfo info;

        for (auto& i : updateFlagsFunc_) {
            i(&conf, &info);
        }
    }

    /**
     * @brief add AddUpdateFlagsFunc in Subclass
     *
     * @details
     * use AddUpdateFlagsFunc to add UpdateFlagsFunc into updateFlagsFunc_;
     * add this function will be called in UpdateFlags;
     * this function should be called before UpdateFlags (like Init()).
     */
    virtual void AddUpdateFlags() = 0;

    /**
     * @brief deal with response info, include output err info
     *
     * @param host
     * @return true: send one request success
     * @return false send one request failed
     * @details
     */
    virtual bool AfterSendRequestToHost(const std::string& host) = 0;

    /**
     * @brief
     *
     * @details
     * If necessary, you can override RunCommand in a subclass:
     *      CurvefsToolRpc::RunCommand();
     *      RemoveFailHostFromHostAddr();
     * Add the fail host in AfterSendRequestToHost:
     *      failHostsAddr_.push_back();
     */
    void RemoveFailHostFromHostAddr() {
        for (auto const& i : failHostsAddr_) {
            auto pos = std::find(hostsAddr_.begin(), hostsAddr_.end(), i);
            while (pos != hostsAddr_.end()) {
                hostsAddr_.erase(pos);
                pos = std::find(hostsAddr_.begin(), hostsAddr_.end(), i);
            }
        }
    }

 protected:
    /**
     * @brief save the host who will be sended request
     * like ip:port
     *
     * @details
     */
    std::vector<std::string> hostsAddr_;
    /**
     * @brief The hosts that failed to send the request
     *
     * @details
     */
    std::vector<std::string> failHostsAddr_;
    std::shared_ptr<ChannelT> channel_;
    std::shared_ptr<ControllerT> controller_;
    std::queue<RequestT> requestQueue_;  // should be defined in Init()
    std::shared_ptr<ResponseT> response_;
    std::shared_ptr<ServiceT> service_stub_;
    /**
     * @brief this functor will called in SendRequestToService
     * Generally need to be assigned to the service_stub_'s request
     * If service_stub_func_ does not assign a value
     * it will crash in SendRequestToService
     *
     * @details
     * it is core function of this class
     * make sure uint test cover SendRequestToServices
     */
    std::function<void(ControllerT*, RequestT*, ResponseT*)> service_stub_func_;
    /**
     * @brief save the functor which defined in curvefs_tool_define.h
     *
     * @details
     */
    std::vector<std::function<void(curve::common::Configuration*,
                                   google::CommandLineFlagInfo*)>>
        updateFlagsFunc_;
};

class CurvefsToolMetric : public CurvefsTool {
 public:
    explicit CurvefsToolMetric(const std::string& command,
                               const std::string& programe, bool show = true)
        : CurvefsTool(command, programe, show) {
        metricClient_ = std::make_shared<MetricClient>();
    }

    int Init(const std::shared_ptr<MetricClient>& metricClient) {
        metricClient_ = metricClient;
        return 0;
    }
    virtual void PrintHelp() {
        CurvefsTool::PrintHelp();
        std::cout << " [-rpcTimeout=" << FLAGS_rpcTimeout << "]"
                  << " [-rpcRetryTimes=" << FLAGS_rpcRetryTimes << "]";
    }

    virtual void InitHostsAddr() {}

 protected:
    void AddUpdateFlagsFunc(
        const std::function<void(curve::common::Configuration*,
                                 google::CommandLineFlagInfo*)>& func) {
        updateFlagsFunc_.push_back(func);
    }

    virtual int RunCommand() {
        int ret = 0;
        for (auto const& i : addr2SubUri) {
            std::string value;
            MetricStatusCode statusCode =
                metricClient_->GetMetric(i.first, i.second, &value);
            if (statusCode != MetricStatusCode::kOK) {
                std::cerr << "get metricName \"" << i.second << "\" from "
                          << i.first << " fail!" << std::endl;
                ret = -1;
            }
            AfterGetMetric(i.first, i.second, value, statusCode);
        }

        if (ProcessMetrics() != 0) {
            ret = -1;
        }
        return ret;
    }

    virtual int Init() {
        // add need update flags
        AddUpdateFlags();
        UpdateFlags();
        InitHostsAddr();
        return 0;
    }

    /**
     * @brief add AddUpdateFlagsFunc in Subclass
     *
     * @details
     * use AddUpdateFlagsFunc to add UpdateFlagsFunc into updateFlagsFunc_;
     * add this function will be called in UpdateFlags;
     * this function should be called before UpdateFlags (like Init()).
     *
     */
    virtual void AddUpdateFlags() {
        // rpcTimeout and rpcRetrytimes is default
        AddUpdateFlagsFunc(SetRpcTimeout);
        AddUpdateFlagsFunc(SetRpcRetryTimes);
    }

    virtual void UpdateFlags() {
        curve::common::Configuration conf;
        conf.SetConfigPath(FLAGS_confPath);
        if (!conf.LoadConfig()) {
            std::cerr << "load configure file " << FLAGS_confPath << " failed!"
                      << std::endl;
        }
        google::CommandLineFlagInfo info;

        for (auto& i : updateFlagsFunc_) {
            i(&conf, &info);
        }
    }

    virtual void AfterGetMetric(const std::string mdsAddr,
                                const std::string& subUri,
                                const std::string& Value,
                                const MetricStatusCode& statusCode) = 0;

    /**
     * @brief
     *
     * @return int
     * 0: no error
     * -1: error
     * @details
     */
    virtual int ProcessMetrics() = 0;

    void AddAddr2Suburi(const std::pair<std::string, std::string>& addrSubUri) {
        addr2SubUri.push_back(addrSubUri);
    }

 protected:
    std::shared_ptr<MetricClient> metricClient_;
    /**
     * @brief get metricName from addr
     *
     * @details
     * first: addr  second: MetricName
     */
    std::vector<std::pair<std::string, std::string>> addr2SubUri;
    /**
     * @brief save the functor which defined in curvefs_tool_define.h
     *
     * @details
     */
    std::vector<std::function<void(curve::common::Configuration*,
                                   google::CommandLineFlagInfo*)>>
        updateFlagsFunc_;
};
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
