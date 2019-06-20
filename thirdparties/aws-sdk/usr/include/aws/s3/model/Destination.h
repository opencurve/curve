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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/AccessControlTranslation.h>
#include <aws/s3/model/EncryptionConfiguration.h>
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
   * <p>A container for information about the replication destination.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Destination">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API Destination
  {
  public:
    Destination();
    Destination(const Aws::Utils::Xml::XmlNode& xmlNode);
    Destination& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline Destination& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline Destination& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p> The Amazon Resource Name (ARN) of the bucket where you want Amazon S3 to
     * store replicas of the object identified by the rule. </p> <p> If there are
     * multiple rules in your replication configuration, all rules must specify the
     * same bucket as the destination. A replication configuration can replicate
     * objects to only one destination bucket. </p>
     */
    inline Destination& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline const Aws::String& GetAccount() const{ return m_account; }

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline bool AccountHasBeenSet() const { return m_accountHasBeenSet; }

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline void SetAccount(const Aws::String& value) { m_accountHasBeenSet = true; m_account = value; }

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline void SetAccount(Aws::String&& value) { m_accountHasBeenSet = true; m_account = std::move(value); }

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline void SetAccount(const char* value) { m_accountHasBeenSet = true; m_account.assign(value); }

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline Destination& WithAccount(const Aws::String& value) { SetAccount(value); return *this;}

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline Destination& WithAccount(Aws::String&& value) { SetAccount(std::move(value)); return *this;}

    /**
     * <p>The account ID of the destination bucket. Currently, Amazon S3 verifies this
     * value only if Access Control Translation is enabled. </p> <p>In a cross-account
     * scenario, if you change replica ownership to the AWS account that owns the
     * destination bucket by adding the <code>AccessControlTranslation</code> element,
     * this is the account ID of the owner of the destination bucket. </p>
     */
    inline Destination& WithAccount(const char* value) { SetAccount(value); return *this;}


    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline Destination& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p> The class of storage used to store the object. By default Amazon S3 uses
     * storage class of the source object when creating a replica. </p>
     */
    inline Destination& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline const AccessControlTranslation& GetAccessControlTranslation() const{ return m_accessControlTranslation; }

    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline bool AccessControlTranslationHasBeenSet() const { return m_accessControlTranslationHasBeenSet; }

    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline void SetAccessControlTranslation(const AccessControlTranslation& value) { m_accessControlTranslationHasBeenSet = true; m_accessControlTranslation = value; }

    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline void SetAccessControlTranslation(AccessControlTranslation&& value) { m_accessControlTranslationHasBeenSet = true; m_accessControlTranslation = std::move(value); }

    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline Destination& WithAccessControlTranslation(const AccessControlTranslation& value) { SetAccessControlTranslation(value); return *this;}

    /**
     * <p>A container for information about access control for replicas. </p> <p>Use
     * this element only in a cross-account scenario where source and destination
     * bucket owners are not the same to change replica ownership to the AWS account
     * that owns the destination bucket. If you don't add this element to the
     * replication configuration, the replicas are owned by same AWS account that owns
     * the source object. </p>
     */
    inline Destination& WithAccessControlTranslation(AccessControlTranslation&& value) { SetAccessControlTranslation(std::move(value)); return *this;}


    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline const EncryptionConfiguration& GetEncryptionConfiguration() const{ return m_encryptionConfiguration; }

    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline bool EncryptionConfigurationHasBeenSet() const { return m_encryptionConfigurationHasBeenSet; }

    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline void SetEncryptionConfiguration(const EncryptionConfiguration& value) { m_encryptionConfigurationHasBeenSet = true; m_encryptionConfiguration = value; }

    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline void SetEncryptionConfiguration(EncryptionConfiguration&& value) { m_encryptionConfigurationHasBeenSet = true; m_encryptionConfiguration = std::move(value); }

    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline Destination& WithEncryptionConfiguration(const EncryptionConfiguration& value) { SetEncryptionConfiguration(value); return *this;}

    /**
     * <p>A container that provides information about encryption. If
     * <code>SourceSelectionCriteria</code> is specified, you must specify this
     * element. </p>
     */
    inline Destination& WithEncryptionConfiguration(EncryptionConfiguration&& value) { SetEncryptionConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_account;
    bool m_accountHasBeenSet;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    AccessControlTranslation m_accessControlTranslation;
    bool m_accessControlTranslationHasBeenSet;

    EncryptionConfiguration m_encryptionConfiguration;
    bool m_encryptionConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
