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
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/ObjectLockRetention.h>
#include <aws/s3/model/RequestPayer.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <utility>

namespace Aws
{
namespace Http
{
    class URI;
} //namespace Http
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutObjectRetentionRequest : public S3Request
  {
  public:
    PutObjectRetentionRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutObjectRetention"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    inline bool ShouldComputeContentMd5() const override { return true; }


    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The bucket that contains the object you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>The key name for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline const ObjectLockRetention& GetRetention() const{ return m_retention; }

    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline bool RetentionHasBeenSet() const { return m_retentionHasBeenSet; }

    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline void SetRetention(const ObjectLockRetention& value) { m_retentionHasBeenSet = true; m_retention = value; }

    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline void SetRetention(ObjectLockRetention&& value) { m_retentionHasBeenSet = true; m_retention = std::move(value); }

    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline PutObjectRetentionRequest& WithRetention(const ObjectLockRetention& value) { SetRetention(value); return *this;}

    /**
     * <p>The container element for the Object Retention configuration.</p>
     */
    inline PutObjectRetentionRequest& WithRetention(ObjectLockRetention&& value) { SetRetention(std::move(value)); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline bool RequestPayerHasBeenSet() const { return m_requestPayerHasBeenSet; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline PutObjectRetentionRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline PutObjectRetentionRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>The version ID for the object that you want to apply this Object Retention
     * configuration to.</p>
     */
    inline PutObjectRetentionRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p>Indicates whether this operation should bypass Governance-mode
     * restrictions.j</p>
     */
    inline bool GetBypassGovernanceRetention() const{ return m_bypassGovernanceRetention; }

    /**
     * <p>Indicates whether this operation should bypass Governance-mode
     * restrictions.j</p>
     */
    inline bool BypassGovernanceRetentionHasBeenSet() const { return m_bypassGovernanceRetentionHasBeenSet; }

    /**
     * <p>Indicates whether this operation should bypass Governance-mode
     * restrictions.j</p>
     */
    inline void SetBypassGovernanceRetention(bool value) { m_bypassGovernanceRetentionHasBeenSet = true; m_bypassGovernanceRetention = value; }

    /**
     * <p>Indicates whether this operation should bypass Governance-mode
     * restrictions.j</p>
     */
    inline PutObjectRetentionRequest& WithBypassGovernanceRetention(bool value) { SetBypassGovernanceRetention(value); return *this;}


    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline bool ContentMD5HasBeenSet() const { return m_contentMD5HasBeenSet; }

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline PutObjectRetentionRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline PutObjectRetentionRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    /**
     * <p>The MD5 hash for the request body.</p>
     */
    inline PutObjectRetentionRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline PutObjectRetentionRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline PutObjectRetentionRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutObjectRetentionRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    ObjectLockRetention m_retention;
    bool m_retentionHasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    bool m_bypassGovernanceRetention;
    bool m_bypassGovernanceRetentionHasBeenSet;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
