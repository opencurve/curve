/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include "src/tools/metric_client.h"

DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curve {
namespace tool {

int MetricClient::GetMetric(const std::string& addr,
                         const std::string& metricName,
                         std::string* value) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    brpc::Controller cntl;
    options.protocol = brpc::PROTOCOL_HTTP;
    int res = httpChannel.Init(addr.c_str(), &options);
    if (res != 0) {
        std::cout << "Init httpChannel to " << addr << " fail!"
                  << std::endl;
        return -1;
    }

    cntl.http_request().uri() = addr + "/vars/" + metricName;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (!cntl.Failed()) {
        std::string attachment =
                cntl.response_attachment().to_string();
        return GetValueFromAttachment(attachment, value);
    }

    bool needRetry = (cntl.Failed() &&
                      cntl.ErrorCode() != EHOSTDOWN &&
                      cntl.ErrorCode() != ETIMEDOUT &&
                      cntl.ErrorCode() != brpc::ELOGOFF &&
                      cntl.ErrorCode() != brpc::ERPCTIMEDOUT);
    uint64_t retryTimes = 0;
    while (needRetry && retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.http_request().uri() = addr + "/vars/" + metricName;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        if (cntl.Failed()) {
            retryTimes++;
            continue;
        }
        std::string attachment =
                cntl.response_attachment().to_string();
        return GetValueFromAttachment(attachment, value);
    }
    // 这里不输出错误，因为对mds有切换的可能，把打印的处理交给外部
    return -1;
}

int MetricClient::GetMetricUint(const std::string& addr,
                  const std::string& metricName,
                  uint64_t* value) {
    std::string str;
    int res = GetMetric(addr, metricName, &str);
    if (res != 0) {
        std::cout << "get metric " << metricName << " from "
                  << addr << " fail";
        return -1;
    }
    if (!curve::common::StringToUll(str, value)) {
        std::cout << "parse metric as uint64_t fail!" << std::endl;
        return -1;
    }
    return 0;
}

int MetricClient::GetValueFromAttachment(const std::string& attachment,
                                       std::string* value) {
    auto pos = attachment.find(":");
    if (pos == std::string::npos) {
        std::cout << "parse response attachment fail!"
                  << std::endl;
        return -1;
    }
    *value = attachment.substr(pos + 1);
    TrimMetricString(value);
    return 0;
}

}  // namespace tool
}  // namespace curve
