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
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Initiator.h>
#include <aws/s3/model/Owner.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/RequestCharged.h>
#include <aws/s3/model/Part.h>
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
  class AWS_S3_API ListPartsResult
  {
  public:
    ListPartsResult();
    ListPartsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    ListPartsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Date when multipart upload will become eligible for abort operation by
     * lifecycle.</p>
     */
    inline const Aws::Utils::DateTime& GetAbortDate() const{ return m_abortDate; }

    /**
     * <p>Date when multipart upload will become eligible for abort operation by
     * lifecycle.</p>
     */
    inline void SetAbortDate(const Aws::Utils::DateTime& value) { m_abortDate = value; }

    /**
     * <p>Date when multipart upload will become eligible for abort operation by
     * lifecycle.</p>
     */
    inline void SetAbortDate(Aws::Utils::DateTime&& value) { m_abortDate = std::move(value); }

    /**
     * <p>Date when multipart upload will become eligible for abort operation by
     * lifecycle.</p>
     */
    inline ListPartsResult& WithAbortDate(const Aws::Utils::DateTime& value) { SetAbortDate(value); return *this;}

    /**
     * <p>Date when multipart upload will become eligible for abort operation by
     * lifecycle.</p>
     */
    inline ListPartsResult& WithAbortDate(Aws::Utils::DateTime&& value) { SetAbortDate(std::move(value)); return *this;}


    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline const Aws::String& GetAbortRuleId() const{ return m_abortRuleId; }

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline void SetAbortRuleId(const Aws::String& value) { m_abortRuleId = value; }

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline void SetAbortRuleId(Aws::String&& value) { m_abortRuleId = std::move(value); }

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline void SetAbortRuleId(const char* value) { m_abortRuleId.assign(value); }

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline ListPartsResult& WithAbortRuleId(const Aws::String& value) { SetAbortRuleId(value); return *this;}

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline ListPartsResult& WithAbortRuleId(Aws::String&& value) { SetAbortRuleId(std::move(value)); return *this;}

    /**
     * <p>Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.</p>
     */
    inline ListPartsResult& WithAbortRuleId(const char* value) { SetAbortRuleId(value); return *this;}


    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucket = value; }

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucket = std::move(value); }

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline void SetBucket(const char* value) { m_bucket.assign(value); }

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>Name of the bucket to which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline void SetKey(const Aws::String& value) { m_key = value; }

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline void SetKey(Aws::String&& value) { m_key = std::move(value); }

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline void SetKey(const char* value) { m_key.assign(value); }

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline ListPartsResult& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadId = value; }

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadId = std::move(value); }

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline void SetUploadId(const char* value) { m_uploadId.assign(value); }

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline ListPartsResult& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline ListPartsResult& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * <p>Upload ID identifying the multipart upload whose parts are being listed.</p>
     */
    inline ListPartsResult& WithUploadId(const char* value) { SetUploadId(value); return *this;}


    /**
     * <p>Part number after which listing begins.</p>
     */
    inline int GetPartNumberMarker() const{ return m_partNumberMarker; }

    /**
     * <p>Part number after which listing begins.</p>
     */
    inline void SetPartNumberMarker(int value) { m_partNumberMarker = value; }

    /**
     * <p>Part number after which listing begins.</p>
     */
    inline ListPartsResult& WithPartNumberMarker(int value) { SetPartNumberMarker(value); return *this;}


    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the part-number-marker request parameter in a
     * subsequent request.</p>
     */
    inline int GetNextPartNumberMarker() const{ return m_nextPartNumberMarker; }

    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the part-number-marker request parameter in a
     * subsequent request.</p>
     */
    inline void SetNextPartNumberMarker(int value) { m_nextPartNumberMarker = value; }

    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the part-number-marker request parameter in a
     * subsequent request.</p>
     */
    inline ListPartsResult& WithNextPartNumberMarker(int value) { SetNextPartNumberMarker(value); return *this;}


    /**
     * <p>Maximum number of parts that were allowed in the response.</p>
     */
    inline int GetMaxParts() const{ return m_maxParts; }

    /**
     * <p>Maximum number of parts that were allowed in the response.</p>
     */
    inline void SetMaxParts(int value) { m_maxParts = value; }

    /**
     * <p>Maximum number of parts that were allowed in the response.</p>
     */
    inline ListPartsResult& WithMaxParts(int value) { SetMaxParts(value); return *this;}


    /**
     * <p>Indicates whether the returned list of parts is truncated.</p>
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * <p>Indicates whether the returned list of parts is truncated.</p>
     */
    inline void SetIsTruncated(bool value) { m_isTruncated = value; }

    /**
     * <p>Indicates whether the returned list of parts is truncated.</p>
     */
    inline ListPartsResult& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * <p/>
     */
    inline const Aws::Vector<Part>& GetParts() const{ return m_parts; }

    /**
     * <p/>
     */
    inline void SetParts(const Aws::Vector<Part>& value) { m_parts = value; }

    /**
     * <p/>
     */
    inline void SetParts(Aws::Vector<Part>&& value) { m_parts = std::move(value); }

    /**
     * <p/>
     */
    inline ListPartsResult& WithParts(const Aws::Vector<Part>& value) { SetParts(value); return *this;}

    /**
     * <p/>
     */
    inline ListPartsResult& WithParts(Aws::Vector<Part>&& value) { SetParts(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline ListPartsResult& AddParts(const Part& value) { m_parts.push_back(value); return *this; }

    /**
     * <p/>
     */
    inline ListPartsResult& AddParts(Part&& value) { m_parts.push_back(std::move(value)); return *this; }


    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline const Initiator& GetInitiator() const{ return m_initiator; }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline void SetInitiator(const Initiator& value) { m_initiator = value; }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline void SetInitiator(Initiator&& value) { m_initiator = std::move(value); }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline ListPartsResult& WithInitiator(const Initiator& value) { SetInitiator(value); return *this;}

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline ListPartsResult& WithInitiator(Initiator&& value) { SetInitiator(std::move(value)); return *this;}


    /**
     * <p/>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p/>
     */
    inline void SetOwner(const Owner& value) { m_owner = value; }

    /**
     * <p/>
     */
    inline void SetOwner(Owner&& value) { m_owner = std::move(value); }

    /**
     * <p/>
     */
    inline ListPartsResult& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p/>
     */
    inline ListPartsResult& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClass = value; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClass = std::move(value); }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline ListPartsResult& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline ListPartsResult& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline ListPartsResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline ListPartsResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    Aws::Utils::DateTime m_abortDate;

    Aws::String m_abortRuleId;

    Aws::String m_bucket;

    Aws::String m_key;

    Aws::String m_uploadId;

    int m_partNumberMarker;

    int m_nextPartNumberMarker;

    int m_maxParts;

    bool m_isTruncated;

    Aws::Vector<Part> m_parts;

    Initiator m_initiator;

    Owner m_owner;

    StorageClass m_storageClass;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
