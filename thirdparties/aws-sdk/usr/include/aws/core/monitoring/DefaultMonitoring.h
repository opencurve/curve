/*
* Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

#pragma once
#include <aws/core/Core_EXPORTS.h>
#include <aws/core/client/AWSClient.h>
#include <aws/core/monitoring/MonitoringInterface.h>
#include <aws/core/monitoring/MonitoringFactory.h>
#include <aws/core/net/SimpleUDP.h>
namespace Aws
{
    namespace Monitoring
    {
        /**
         * Default monitoring implementation definition
         */
        class AWS_CORE_API DefaultMonitoring: public MonitoringInterface
        {
        public:
            const static int DEFAULT_MONITORING_VERSION;
            const static char DEFAULT_CSM_CONFIG_ENABLED[];
            const static char DEFAULT_CSM_CONFIG_CLIENT_ID[];
            const static char DEFAULT_CSM_CONFIG_PORT[];
            const static char DEFAULT_CSM_ENVIRONMENT_VAR_ENABLED[];
            const static char DEFAULT_CSM_ENVIRONMENT_VAR_CLIENT_ID[];
            const static char DEFAULT_CSM_ENVIRONMENT_VAR_PORT[];

            /**
             * @brief Construct a default monitoring instance
             * @param clientId, used to identify the application
             * @param port, used to send collected metric to a local agent listen on this port.
             */
            DefaultMonitoring(const Aws::String& clientId, unsigned short port);

            void* OnRequestStarted(const Aws::String& serviceName, const Aws::String& requestName, const std::shared_ptr<const Aws::Http::HttpRequest>& request) const override;

            void OnRequestSucceeded(const Aws::String& serviceName, const Aws::String& requestName, const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                const Aws::Client::HttpResponseOutcome& outcome, const CoreMetricsCollection& metricsFromCore, void* context) const override;


            void OnRequestFailed(const Aws::String& serviceName, const Aws::String& requestName, const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                const Aws::Client::HttpResponseOutcome& outcome, const CoreMetricsCollection& metricsFromCore, void* context) const override;

 
            void OnRequestRetry(const Aws::String& serviceName, const Aws::String& requestName, 
                const std::shared_ptr<const Aws::Http::HttpRequest>& request, void* context) const override;


            void OnFinish(const Aws::String& serviceName, const Aws::String& requestName, 
                const std::shared_ptr<const Aws::Http::HttpRequest>& request, void* context) const override;

            static inline int GetVersion() { return DEFAULT_MONITORING_VERSION; }
        private:
            void CollectAndSendAttemptData(const Aws::String& serviceName, const Aws::String& requestName, 
                const std::shared_ptr<const Aws::Http::HttpRequest>& request, const Aws::Client::HttpResponseOutcome& outcome, 
                const CoreMetricsCollection& metricsFromCore, void* context) const;

            Aws::Net::SimpleUDP m_udp;
            Aws::String m_clientId;
            unsigned short m_port;
        };

        class AWS_CORE_API DefaultMonitoringFactory : public MonitoringFactory
        {
        public:
            Aws::UniquePtr<MonitoringInterface> CreateMonitoringInstance() const override;
        };
    } // namespace Monitoring
} // namepsace Aws
