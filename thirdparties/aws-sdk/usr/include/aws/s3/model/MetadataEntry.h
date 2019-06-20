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
#include <aws/core/utils/memory/stl/AWSString.h>
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
   * <p>A metadata key-value pair to store with an object.</p><p><h3>See Also:</h3>  
   * <a href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/MetadataEntry">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API MetadataEntry
  {
  public:
    MetadataEntry();
    MetadataEntry(const Aws::Utils::Xml::XmlNode& xmlNode);
    MetadataEntry& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p/>
     */
    inline const Aws::String& GetName() const{ return m_name; }

    /**
     * <p/>
     */
    inline bool NameHasBeenSet() const { return m_nameHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetName(const Aws::String& value) { m_nameHasBeenSet = true; m_name = value; }

    /**
     * <p/>
     */
    inline void SetName(Aws::String&& value) { m_nameHasBeenSet = true; m_name = std::move(value); }

    /**
     * <p/>
     */
    inline void SetName(const char* value) { m_nameHasBeenSet = true; m_name.assign(value); }

    /**
     * <p/>
     */
    inline MetadataEntry& WithName(const Aws::String& value) { SetName(value); return *this;}

    /**
     * <p/>
     */
    inline MetadataEntry& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline MetadataEntry& WithName(const char* value) { SetName(value); return *this;}


    /**
     * <p/>
     */
    inline const Aws::String& GetValue() const{ return m_value; }

    /**
     * <p/>
     */
    inline bool ValueHasBeenSet() const { return m_valueHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetValue(const Aws::String& value) { m_valueHasBeenSet = true; m_value = value; }

    /**
     * <p/>
     */
    inline void SetValue(Aws::String&& value) { m_valueHasBeenSet = true; m_value = std::move(value); }

    /**
     * <p/>
     */
    inline void SetValue(const char* value) { m_valueHasBeenSet = true; m_value.assign(value); }

    /**
     * <p/>
     */
    inline MetadataEntry& WithValue(const Aws::String& value) { SetValue(value); return *this;}

    /**
     * <p/>
     */
    inline MetadataEntry& WithValue(Aws::String&& value) { SetValue(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline MetadataEntry& WithValue(const char* value) { SetValue(value); return *this;}

  private:

    Aws::String m_name;
    bool m_nameHasBeenSet;

    Aws::String m_value;
    bool m_valueHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
