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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tag.h>
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
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Tagging">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API Tagging
  {
  public:
    Tagging();
    Tagging(const Aws::Utils::Xml::XmlNode& xmlNode);
    Tagging& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p/>
     */
    inline const Aws::Vector<Tag>& GetTagSet() const{ return m_tagSet; }

    /**
     * <p/>
     */
    inline bool TagSetHasBeenSet() const { return m_tagSetHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetTagSet(const Aws::Vector<Tag>& value) { m_tagSetHasBeenSet = true; m_tagSet = value; }

    /**
     * <p/>
     */
    inline void SetTagSet(Aws::Vector<Tag>&& value) { m_tagSetHasBeenSet = true; m_tagSet = std::move(value); }

    /**
     * <p/>
     */
    inline Tagging& WithTagSet(const Aws::Vector<Tag>& value) { SetTagSet(value); return *this;}

    /**
     * <p/>
     */
    inline Tagging& WithTagSet(Aws::Vector<Tag>&& value) { SetTagSet(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline Tagging& AddTagSet(const Tag& value) { m_tagSetHasBeenSet = true; m_tagSet.push_back(value); return *this; }

    /**
     * <p/>
     */
    inline Tagging& AddTagSet(Tag&& value) { m_tagSetHasBeenSet = true; m_tagSet.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<Tag> m_tagSet;
    bool m_tagSetHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
