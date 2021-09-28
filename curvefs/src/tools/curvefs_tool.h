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

#include "curvefs/src/tools/curvefs_tool_define.h"

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
    virtual void SendRequestToService() {
        service_stub_func_(controller_.get(), request_.get(), response_.get());
    }

    virtual int Init() {
        channel_ = std::make_shared<ChannelT>();
        controller_ = std::make_shared<ControllerT>();
        request_ = std::make_shared<RequestT>();
        response_ = std::make_shared<ResponseT>();
        service_stub_ = std::make_shared<ServiceT>(channel_.get());
        return 0;
    }

 protected:
    std::shared_ptr<ChannelT> channel_;
    std::shared_ptr<ControllerT> controller_;
    std::shared_ptr<RequestT> request_;
    std::shared_ptr<ResponseT> response_;
    std::shared_ptr<ServiceT> service_stub_;
    std::function<void(ControllerT*, RequestT*, ResponseT*)> service_stub_func_;
};

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_H_
