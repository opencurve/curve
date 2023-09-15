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
 * Created Date: 2020-02-06
 * Author: charisu
 */

#include "src/tools/metric_client.h"

#include <memory>

DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curve {
namespace tool {

MetricRet MetricClient::GetMetric(const std::string &addr,
                                  const std::string &metricName,
                                  std::string *value) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    brpc::Controller cntl;
    options.protocol = brpc::PROTOCOL_HTTP;
    int res = httpChannel.Init(addr.c_str(), &options);
    if (res != 0) {
        std::cout << "Init httpChannel to " << addr << " fail!" << std::endl;
        return MetricRet::kOtherErr;
    }

    cntl.http_request().uri() = addr + kVars + metricName;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (!cntl.Failed()) {
        std::string attachment = cntl.response_attachment().to_string();
        res = GetValueFromAttachment(attachment, value);
        return (res == 0) ? MetricRet::kOK : MetricRet::kOtherErr;
    }

    bool needRetry =
        (cntl.Failed() && cntl.ErrorCode() != EHOSTDOWN &&
         cntl.ErrorCode() != ETIMEDOUT && cntl.ErrorCode() != brpc::ELOGOFF &&
         cntl.ErrorCode() != brpc::ERPCTIMEDOUT);
    uint64_t retryTimes = 0;
    while (needRetry && retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.http_request().uri() = addr + kVars + metricName;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        if (cntl.Failed()) {
            retryTimes++;
            continue;
        }
        std::string attachment = cntl.response_attachment().to_string();
        res = GetValueFromAttachment(attachment, value);
        return (res == 0) ? MetricRet::kOK : MetricRet::kOtherErr;
    }
    // There is no output error here, as there is a possibility of switching between mds, and the printing process is handed over to external parties
    bool notExist = cntl.ErrorCode() == brpc::EHTTP &&
                    cntl.http_response().status_code() == kHttpCodeNotFound;
    return notExist ? MetricRet::kNotFound : MetricRet::kOtherErr;
}

MetricRet MetricClient::GetMetricUint(const std::string &addr,
                                      const std::string &metricName,
                                      uint64_t *value) {
    std::string str;
    MetricRet res = GetMetric(addr, metricName, &str);
    if (res != MetricRet::kOK) {
        std::cout << "get metric " << metricName << " from " << addr << " fail";
        return res;
    }
    if (!curve::common::StringToUll(str, value)) {
        std::cout << "parse metric as uint64_t fail!" << std::endl;
        return MetricRet::kOtherErr;
    }
    return MetricRet::kOK;
}

MetricRet MetricClient::GetConfValueFromMetric(const std::string &addr,
                                               const std::string &metricName,
                                               std::string *confValue) {
    std::string jsonString;
    brpc::Controller cntl;
    MetricRet res = GetMetric(addr, metricName, &jsonString);
    if (res != MetricRet::kOK) {
        return res;
    }

    Json::CharReaderBuilder builder;
    Json::CharReaderBuilder::strictMode(&builder.settings_);
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value value;
    JSONCPP_STRING errormsg;
    if (!reader->parse(jsonString.data(),
                       jsonString.data() + jsonString.length(), &value,
                       &errormsg)) {
        std::cout << "Parse metric as json fail: " << errormsg << std::endl;
        return MetricRet::kOtherErr;
    }

    *confValue = value[kConfValue].asString();
    return MetricRet::kOK;
}

int MetricClient::GetValueFromAttachment(const std::string &attachment,
                                         std::string *value) {
    auto pos = attachment.find(":");
    if (pos == std::string::npos) {
        std::cout << "parse response attachment fail!" << std::endl;
        return -1;
    }
    *value = attachment.substr(pos + 1);
    TrimMetricString(value);
    return 0;
}

}  // namespace tool
}  // namespace curve
