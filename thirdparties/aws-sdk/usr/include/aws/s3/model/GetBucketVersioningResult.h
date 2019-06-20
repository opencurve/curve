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
#include <aws/s3/model/BucketVersioningStatus.h>
#include <aws/s3/model/MFADeleteStatus.h>
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
  class AWS_S3_API GetBucketVersioningResult
  {
  public:
    GetBucketVersioningResult();
    GetBucketVersioningResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketVersioningResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The versioning state of the bucket.</p>
     */
    inline const BucketVersioningStatus& GetStatus() const{ return m_status; }

    /**
     * <p>The versioning state of the bucket.</p>
     */
    inline void SetStatus(const BucketVersioningStatus& value) { m_status = value; }

    /**
     * <p>The versioning state of the bucket.</p>
     */
    inline void SetStatus(BucketVersioningStatus&& value) { m_status = std::move(value); }

    /**
     * <p>The versioning state of the bucket.</p>
     */
    inline GetBucketVersioningResult& WithStatus(const BucketVersioningStatus& value) { SetStatus(value); return *this;}

    /**
     * <p>The versioning state of the bucket.</p>
     */
    inline GetBucketVersioningResult& WithStatus(BucketVersioningStatus&& value) { SetStatus(std::move(value)); return *this;}


    /**
     * <p>Specifies whether MFA delete is enabled in the bucket versioning
     * configuration. This element is only returned if the bucket has been configured
     * with MFA delete. If the bucket has never been so configured, this element is not
     * returned.</p>
     */
    inline const MFADeleteStatus& GetMFADelete() const{ return m_mFADelete; }

    /**
     * <p>Specifies whether MFA delete is enabled in the bucket versioning
     * configuration. This element is only returned if the bucket has been configured
     * with MFA delete. If the bucket has never been so configured, this element is not
     * returned.</p>
     */
    inline void SetMFADelete(const MFADeleteStatus& value) { m_mFADelete = value; }

    /**
     * <p>Specifies whether MFA delete is enabled in the bucket versioning
     * configuration. This element is only returned if the bucket has been configured
     * with MFA delete. If the bucket has never been so configured, this element is not
     * returned.</p>
     */
    inline void SetMFADelete(MFADeleteStatus&& value) { m_mFADelete = std::move(value); }

    /**
     * <p>Specifies whether MFA delete is enabled in the bucket versioning
     * configuration. This element is only returned if the bucket has been configured
     * with MFA delete. If the bucket has never been so configured, this element is not
     * returned.</p>
     */
    inline GetBucketVersioningResult& WithMFADelete(const MFADeleteStatus& value) { SetMFADelete(value); return *this;}

    /**
     * <p>Specifies whether MFA delete is enabled in the bucket versioning
     * configuration. This element is only returned if the bucket has been configured
     * with MFA delete. If the bucket has never been so configured, this element is not
     * returned.</p>
     */
    inline GetBucketVersioningResult& WithMFADelete(MFADeleteStatus&& value) { SetMFADelete(std::move(value)); return *this;}

  private:

    BucketVersioningStatus m_status;

    MFADeleteStatus m_mFADelete;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
