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
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/RequestCharged.h>
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
  class AWS_S3_API UploadPartResult
  {
  public:
    UploadPartResult();
    UploadPartResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    UploadPartResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The Server-side encryption algorithm used when storing this object in S3
     * (e.g., AES256, aws:kms).</p>
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * <p>The Server-side encryption algorithm used when storing this object in S3
     * (e.g., AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryption = value; }

    /**
     * <p>The Server-side encryption algorithm used when storing this object in S3
     * (e.g., AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryption = std::move(value); }

    /**
     * <p>The Server-side encryption algorithm used when storing this object in S3
     * (e.g., AES256, aws:kms).</p>
     */
    inline UploadPartResult& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * <p>The Server-side encryption algorithm used when storing this object in S3
     * (e.g., AES256, aws:kms).</p>
     */
    inline UploadPartResult& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline void SetETag(const Aws::String& value) { m_eTag = value; }

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline void SetETag(Aws::String&& value) { m_eTag = std::move(value); }

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline void SetETag(const char* value) { m_eTag.assign(value); }

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline UploadPartResult& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline UploadPartResult& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * <p>Entity tag for the uploaded object.</p>
     */
    inline UploadPartResult& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithm = value; }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithm = std::move(value); }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithm.assign(value); }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline UploadPartResult& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline UploadPartResult& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header confirming the encryption
     * algorithm used.</p>
     */
    inline UploadPartResult& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline const Aws::String& GetSSECustomerKeyMD5() const{ return m_sSECustomerKeyMD5; }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline void SetSSECustomerKeyMD5(const Aws::String& value) { m_sSECustomerKeyMD5 = value; }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline void SetSSECustomerKeyMD5(Aws::String&& value) { m_sSECustomerKeyMD5 = std::move(value); }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline void SetSSECustomerKeyMD5(const char* value) { m_sSECustomerKeyMD5.assign(value); }

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline UploadPartResult& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline UploadPartResult& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * <p>If server-side encryption with a customer-provided encryption key was
     * requested, the response will include this header to provide round trip message
     * integrity verification of the customer-provided encryption key.</p>
     */
    inline UploadPartResult& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyId = value; }

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyId = std::move(value); }

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyId.assign(value); }

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline UploadPartResult& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline UploadPartResult& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * <p>If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.</p>
     */
    inline UploadPartResult& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline UploadPartResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline UploadPartResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    ServerSideEncryption m_serverSideEncryption;

    Aws::String m_eTag;

    Aws::String m_sSECustomerAlgorithm;

    Aws::String m_sSECustomerKeyMD5;

    Aws::String m_sSEKMSKeyId;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
