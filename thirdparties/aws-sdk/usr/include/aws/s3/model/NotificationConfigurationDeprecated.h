﻿/*
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
#include <aws/s3/model/TopicConfigurationDeprecated.h>
#include <aws/s3/model/QueueConfigurationDeprecated.h>
#include <aws/s3/model/CloudFunctionConfiguration.h>
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

  class AWS_S3_API NotificationConfigurationDeprecated
  {
  public:
    NotificationConfigurationDeprecated();
    NotificationConfigurationDeprecated(const Aws::Utils::Xml::XmlNode& xmlNode);
    NotificationConfigurationDeprecated& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p/>
     */
    inline const TopicConfigurationDeprecated& GetTopicConfiguration() const{ return m_topicConfiguration; }

    /**
     * <p/>
     */
    inline bool TopicConfigurationHasBeenSet() const { return m_topicConfigurationHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetTopicConfiguration(const TopicConfigurationDeprecated& value) { m_topicConfigurationHasBeenSet = true; m_topicConfiguration = value; }

    /**
     * <p/>
     */
    inline void SetTopicConfiguration(TopicConfigurationDeprecated&& value) { m_topicConfigurationHasBeenSet = true; m_topicConfiguration = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithTopicConfiguration(const TopicConfigurationDeprecated& value) { SetTopicConfiguration(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithTopicConfiguration(TopicConfigurationDeprecated&& value) { SetTopicConfiguration(std::move(value)); return *this;}


    /**
     * <p/>
     */
    inline const QueueConfigurationDeprecated& GetQueueConfiguration() const{ return m_queueConfiguration; }

    /**
     * <p/>
     */
    inline bool QueueConfigurationHasBeenSet() const { return m_queueConfigurationHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetQueueConfiguration(const QueueConfigurationDeprecated& value) { m_queueConfigurationHasBeenSet = true; m_queueConfiguration = value; }

    /**
     * <p/>
     */
    inline void SetQueueConfiguration(QueueConfigurationDeprecated&& value) { m_queueConfigurationHasBeenSet = true; m_queueConfiguration = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithQueueConfiguration(const QueueConfigurationDeprecated& value) { SetQueueConfiguration(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithQueueConfiguration(QueueConfigurationDeprecated&& value) { SetQueueConfiguration(std::move(value)); return *this;}


    /**
     * <p/>
     */
    inline const CloudFunctionConfiguration& GetCloudFunctionConfiguration() const{ return m_cloudFunctionConfiguration; }

    /**
     * <p/>
     */
    inline bool CloudFunctionConfigurationHasBeenSet() const { return m_cloudFunctionConfigurationHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetCloudFunctionConfiguration(const CloudFunctionConfiguration& value) { m_cloudFunctionConfigurationHasBeenSet = true; m_cloudFunctionConfiguration = value; }

    /**
     * <p/>
     */
    inline void SetCloudFunctionConfiguration(CloudFunctionConfiguration&& value) { m_cloudFunctionConfigurationHasBeenSet = true; m_cloudFunctionConfiguration = std::move(value); }

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithCloudFunctionConfiguration(const CloudFunctionConfiguration& value) { SetCloudFunctionConfiguration(value); return *this;}

    /**
     * <p/>
     */
    inline NotificationConfigurationDeprecated& WithCloudFunctionConfiguration(CloudFunctionConfiguration&& value) { SetCloudFunctionConfiguration(std::move(value)); return *this;}

  private:

    TopicConfigurationDeprecated m_topicConfiguration;
    bool m_topicConfigurationHasBeenSet;

    QueueConfigurationDeprecated m_queueConfiguration;
    bool m_queueConfigurationHasBeenSet;

    CloudFunctionConfiguration m_cloudFunctionConfiguration;
    bool m_cloudFunctionConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
