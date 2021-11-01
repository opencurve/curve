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
 * Created Date: 2021-10-25
 * Author: chengyi01
 */

#include "curvefs/src/tools/curvefs_tool_metric.h"

DECLARE_uint32(rpcTimeoutMs);
DECLARE_uint64(rpcRetryTimes);

namespace curvefs {
namespace tools {

MetricStatusCode MetricClient::GetMetric(const std::string& addr,
                                         const std::string& subUri,
                                         std::string* value) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    brpc::Controller cntl;
    options.protocol = brpc::PROTOCOL_HTTP;
    int res = httpChannel.Init(addr.c_str(), &options);
    if (res != 0) {
        std::cerr << "init httpChannel to " << addr << " fail!" << std::endl;
        return MetricStatusCode::kOtherErr;
    }

    cntl.http_request().uri() = addr + subUri;

    // TODO(chengyi01): move to while
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);
    httpChannel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (!cntl.Failed()) {
        *value = cntl.response_attachment().to_string();

        return MetricStatusCode::kOK;
    }

    bool needRetry =
        (cntl.Failed() && cntl.ErrorCode() != EHOSTDOWN &&
         cntl.ErrorCode() != ETIMEDOUT && cntl.ErrorCode() != brpc::ELOGOFF &&
         cntl.ErrorCode() != brpc::ERPCTIMEDOUT);
    uint64_t retryTimes = 0;
    while (needRetry && retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.http_request().uri() = addr + subUri;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);
        httpChannel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
        if (cntl.Failed()) {
            retryTimes++;
            continue;
        }
        *value = cntl.response_attachment().to_string();
    }
    // Hand over the printing process to the outside
    bool notExist = cntl.ErrorCode() == brpc::EHTTP &&
                    cntl.http_response().status_code() == kHttpCodeNotFound;
    return notExist ? MetricStatusCode::kNotFound : MetricStatusCode::kOtherErr;
}

int MetricClient::GetKeyValueFromJson(const std::string& strJson,
                                      const std::string& key,
                                      std::string* value) {
    Json::CharReaderBuilder reader;
    std::stringstream ss(strJson);
    std::string err;
    Json::Value json;
    bool parseCode = Json::parseFromStream(reader, ss, &json, &err);
    if (!parseCode) {
        std::cerr << "parse " << ss.str() << " fail " << err << std::endl;
        return -1;
    }

    int ret = -1;

    if (json[key].isNull()) {
        std::cerr << "there is no " << key << " in " << json.asString()
                  << std::endl;
    } else if (json[key].isString()) {
        *value = json[key].asString();
        ret = 0;
    } else {
        std::cerr << "the key " << key << " in " << json.asString()
                  << " is not a string value." << std::endl;
    }
    return ret;
}

int MetricClient::GetKeyValueFromString(const std::string& str,
                                        const std::string& key,
                                        std::string* value) {
    auto pos = str.find(":");
    if (pos == std::string::npos) {
        std::cout << "parse response attachment fail!" << std::endl;
        return -1;
    }
    *value = str.substr(pos + 1);
    TrimMetricString(value);
    return 0;
}

void MetricClient::TrimMetricString(std::string* str) {
    str->erase(0, str->find_first_not_of(" "));
    str->erase(str->find_last_not_of("\r\n") + 1);
    str->erase(0, str->find_first_not_of("\""));
    str->erase(str->find_last_not_of("\"") + 1);
}

}  // namespace tools
}  // namespace curvefs
