/*
* Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/TopicConfiguration.h>
#include <aws/s3/model/QueueConfiguration.h>
#include <aws/s3/model/LambdaFunctionConfiguration.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  /**
   * <p>A container for specifying the notification configuration of the bucket. If
   * this element is empty, notifications are turned off for the
   * bucket.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/NotificationConfiguration">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API NotificationConfiguration
  {
  public:
    NotificationConfiguration();
    NotificationConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    NotificationConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p/>
     */
    inline const Aws::Vector<TopicConfiguration>& GetTopicConfigurations() const{ return m_topicConfigurations; }

    /**
     * <p/>
     */
    inline bool TopicConfigurationsHasBeenSet() const { return m_topicConfigurationsHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetTopicConfigurations(const Aws::Vector<TopicConfiguration>& value) { m_topicConfigurationsHasBeenSet = true; m_topicConfigurations = value; }

    /**
     * <p/>
     */
    inline void SetTopicConfigurations(Aws::Vector<TopicConfiguration>&& value) { m_topicConfigurationsHasBeenSet = true; m_topicConfigurations = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithTopicConfigurations(const Aws::Vector<TopicConfiguration>& value) { SetTopicConfigurations(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithTopicConfigurations(Aws::Vector<TopicConfiguration>&& value) { SetTopicConfigurations(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddTopicConfigurations(const TopicConfiguration& value) { m_topicConfigurationsHasBeenSet = true; m_topicConfigurations.push_back(value); return *this; }

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddTopicConfigurations(TopicConfiguration&& value) { m_topicConfigurationsHasBeenSet = true; m_topicConfigurations.push_back(std::move(value)); return *this; }


    /**
     * <p/>
     */
    inline const Aws::Vector<QueueConfiguration>& GetQueueConfigurations() const{ return m_queueConfigurations; }

    /**
     * <p/>
     */
    inline bool QueueConfigurationsHasBeenSet() const { return m_queueConfigurationsHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetQueueConfigurations(const Aws::Vector<QueueConfiguration>& value) { m_queueConfigurationsHasBeenSet = true; m_queueConfigurations = value; }

    /**
     * <p/>
     */
    inline void SetQueueConfigurations(Aws::Vector<QueueConfiguration>&& value) { m_queueConfigurationsHasBeenSet = true; m_queueConfigurations = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithQueueConfigurations(const Aws::Vector<QueueConfiguration>& value) { SetQueueConfigurations(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithQueueConfigurations(Aws::Vector<QueueConfiguration>&& value) { SetQueueConfigurations(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddQueueConfigurations(const QueueConfiguration& value) { m_queueConfigurationsHasBeenSet = true; m_queueConfigurations.push_back(value); return *this; }

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddQueueConfigurations(QueueConfiguration&& value) { m_queueConfigurationsHasBeenSet = true; m_queueConfigurations.push_back(std::move(value)); return *this; }


    /**
     * <p/>
     */
    inline const Aws::Vector<LambdaFunctionConfiguration>& GetLambdaFunctionConfigurations() const{ return m_lambdaFunctionConfigurations; }

    /**
     * <p/>
     */
    inline bool LambdaFunctionConfigurationsHasBeenSet() const { return m_lambdaFunctionConfigurationsHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetLambdaFunctionConfigurations(const Aws::Vector<LambdaFunctionConfiguration>& value) { m_lambdaFunctionConfigurationsHasBeenSet = true; m_lambdaFunctionConfigurations = value; }

    /**
     * <p/>
     */
    inline void SetLambdaFunctionConfigurations(Aws::Vector<LambdaFunctionConfiguration>&& value) { m_lambdaFunctionConfigurationsHasBeenSet = true; m_lambdaFunctionConfigurations = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithLambdaFunctionConfigurations(const Aws::Vector<LambdaFunctionConfiguration>& value) { SetLambdaFunctionConfigurations(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& WithLambdaFunctionConfigurations(Aws::Vector<LambdaFunctionConfiguration>&& value) { SetLambdaFunctionConfigurations(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddLambdaFunctionConfigurations(const LambdaFunctionConfiguration& value) { m_lambdaFunctionConfigurationsHasBeenSet = true; m_lambdaFunctionConfigurations.push_back(value); return *this; }

    /**
     * <p/>
     */
    inline NotificationConfiguration& AddLambdaFunctionConfigurations(LambdaFunctionConfiguration&& value) { m_lambdaFunctionConfigurationsHasBeenSet = true; m_lambdaFunctionConfigurations.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<TopicConfiguration> m_topicConfigurations;
    bool m_topicConfigurationsHasBeenSet;

    Aws::Vector<QueueConfiguration> m_queueConfigurations;
    bool m_queueConfigurationsHasBeenSet;

    Aws::Vector<LambdaFunctionConfiguration> m_lambdaFunctionConfigurations;
    bool m_lambdaFunctionConfigurationsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
