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
#include <aws/s3/model/PolicyStatus.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class AWS_S3_API GetBucketPolicyStatusResult
  {
  public:
    GetBucketPolicyStatusResult();
    GetBucketPolicyStatusResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketPolicyStatusResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline const PolicyStatus& GetPolicyStatus() const{ return m_policyStatus; }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline void SetPolicyStatus(const PolicyStatus& value) { m_policyStatus = value; }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline void SetPolicyStatus(PolicyStatus&& value) { m_policyStatus = std::move(value); }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline GetBucketPolicyStatusResult& WithPolicyStatus(const PolicyStatus& value) { SetPolicyStatus(value); return *this;}

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline GetBucketPolicyStatusResult& WithPolicyStatus(PolicyStatus&& value) { SetPolicyStatus(std::move(value)); return *this;}

  private:

    PolicyStatus m_policyStatus;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
