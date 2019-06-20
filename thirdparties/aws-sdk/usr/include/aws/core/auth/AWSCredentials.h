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
#include <aws/core/utils/memory/stl/AWSString.h>
namespace Aws
{
    namespace Auth
    {
        /**
         * Simple data object around aws credentials
         */
        class AWS_CORE_API AWSCredentials
        {
        public:
            AWSCredentials() {}

            /**
             * Initializes object with accessKeyId, secretKey, and sessionToken. Session token defaults to empty.
             */
            AWSCredentials(const Aws::String& accessKeyId, const Aws::String& secretKey, const Aws::String& sessionToken = "") :
                m_accessKeyId(accessKeyId), m_secretKey(secretKey), m_sessionToken(sessionToken)
            {
            }

            bool operator == (const AWSCredentials& other) const { return m_accessKeyId == other.m_accessKeyId && m_secretKey == other.m_secretKey && m_sessionToken == other.m_sessionToken; };
            bool operator != (const AWSCredentials& other) const { return m_accessKeyId != other.m_accessKeyId || m_secretKey != other.m_secretKey || m_sessionToken != other.m_sessionToken; };

            /**
             * If credentials haven't been initialized or been initialized to emtpy values.
             */
            inline bool IsEmpty() const { return m_accessKeyId.empty() && m_secretKey.empty(); }

            /**
             * Gets the underlying access key credential
             */
            inline const Aws::String& GetAWSAccessKeyId() const
            {
                return m_accessKeyId;
            }

            /**
             * Gets the underlying secret key credential
             */
            inline const Aws::String& GetAWSSecretKey() const
            {
                return m_secretKey;
            }

            /**
             * Gets the underlying session token
             */
            inline const Aws::String& GetSessionToken() const
            {
                return m_sessionToken;
            }

            /**
             * Sets the underlying access key credential. Copies from parameter accessKeyId.
             */
            inline void SetAWSAccessKeyId(const Aws::String& accessKeyId)
            {
                m_accessKeyId = accessKeyId;
            }

            /**
             * Sets the underlying secret key credential. Copies from parameter secretKey
             */
            inline void SetAWSSecretKey(const Aws::String& secretKey)
            {
                m_secretKey = secretKey;
            }

            /**
             * Sets the underlyint session token. Copies from parameter sessionToken
             */
            inline void SetSessionToken(const Aws::String& sessionToken)
            {
                m_sessionToken = sessionToken;
            }

            /**
            * Sets the underlying access key credential. Copies from parameter accessKeyId.
            */
            inline void SetAWSAccessKeyId(const char* accessKeyId)
            {
                m_accessKeyId = accessKeyId;
            }

            /**
            * Sets the underlying secret key credential. Copies from parameter secretKey
            */
            inline void SetAWSSecretKey(const char* secretKey)
            {
                m_secretKey = secretKey;
            }

            /**
            * Sets the underlying secret key credential. Copies from parameter secretKey
            */
            inline void SetSessionToken(const char* sessionToken)
            {
                m_sessionToken = sessionToken;
            }

        private:
            Aws::String m_accessKeyId;
            Aws::String m_secretKey;
            Aws::String m_sessionToken;
        };
    }
}
