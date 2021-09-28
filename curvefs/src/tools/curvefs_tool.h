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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_

#include <brpc/channel.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/configuration.h"

DECLARE_string(confPath);

namespace curvefs {
namespace tools {

class CurvefsTool {
 public:
    CurvefsTool() {}
    CurvefsTool(const std::string& command, const std::string& programe)
        : command_(command), programe_(programe) {}
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

 private:
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
    CurvefsToolRpc(const std::string& command, const std::string& programe)
        : CurvefsTool(command, programe) {}

    int Init(const std::shared_ptr<ChannelT>& channel,
             const std::shared_ptr<ControllerT>& controller,
             const std::shared_ptr<RequestT>& request,
             const std::shared_ptr<ResponseT>& response,
             const std::shared_ptr<ServiceT>& service_stub) {
        channel_ = channel;
        controller_ = controller;
        request_ = request;
        response_ = response;
        service_stub_ = service_stub;
        return 0;
    }

 protected:
    /**
     * @brief send request to host in hostsAddressStr_
     *
     * @return true
     * @return false
     * @details
     * as long as one succeeds, it returns true and ends sending
     */
    virtual bool SendRequestToServices() {
        for (const std::string& host : hostsAddressStr_) {
            if (channel_->Init(host.c_str(), nullptr) != 0) {
                std::cerr << "Fail init channel to host: " << host << std::endl;
                continue;
            }
            // if service_stub_func_ does not assign a value
            // it will crash in there
            service_stub_func_(controller_.get(), request_.get(),
                               response_.get());
            if (AfterSendRequestToService(host) == true) {
                return true;
            }
            controller_->Reset();
        }
        // send request to all host failed
        return false;
    }

    virtual int Init() {
        channel_ = std::make_shared<ChannelT>();
        controller_ = std::make_shared<ControllerT>();
        request_ = std::make_shared<RequestT>();
        response_ = std::make_shared<ResponseT>();
        service_stub_ = std::make_shared<ServiceT>(channel_.get());
        AddUpdateFlagsFuncs();
        UpdateFlagsFromConf();
        return 0;
    }

    virtual void AddUpdateFlagsFunc(
        const std::function<void(curve::common::Configuration*,
                                 google::CommandLineFlagInfo*)>& func) {
        updateFlagsFunc_.push_back(func);
    }

    virtual void UpdateFlagsFromConf() {
        std::string confPath = FLAGS_confPath.c_str();
        curve::common::Configuration conf;
        conf.SetConfigPath(confPath);
        if (!conf.LoadConfig()) {
            std::cerr << "load configure file " << confPath << " failed!"
                      << std::endl;
        }
        google::CommandLineFlagInfo info;

        for (auto i : updateFlagsFunc_) {
            i(&conf, &info);
        }
    }

    /**
     * @brief add AddUpdateFlagsFunc in Subclass
     *
     * @details
     * use AddUpdateFlagsFunc to add UpdateFlagsFunc into updateFlagsFunc_;
     * add this function will be called in UpdateFlagsFromConf;
     * this function should be called before UpdateFlagsFromConf (like Init()).
     */
    virtual void AddUpdateFlagsFuncs() = 0;

    /**
     * @brief deal with response info, include output err info
     *
     * @param host
     * @return true: send request success
     * @return false send request failed
     * @details
     */
    virtual bool AfterSendRequestToService(const std::string& host) = 0;

 protected:
    /**
     * @brief save the host who will be sended request
     * like ip:port
     *
     * @details
     */
    std::vector<std::string> hostsAddressStr_;
    std::shared_ptr<ChannelT> channel_;
    std::shared_ptr<ControllerT> controller_;
    std::shared_ptr<RequestT> request_;
    std::shared_ptr<ResponseT> response_;
    std::shared_ptr<ServiceT> service_stub_;
    /**
     * @brief this functor will called in SendRequestToService
     * Generally need to be assigned to the service_stub_'s request
     * If service_stub_func_ does not assign a value
     * it will crash in SendRequestToService
     *
     * @details
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

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
