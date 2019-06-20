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
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Error">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API Error
  {
  public:
    Error();
    Error(const Aws::Utils::Xml::XmlNode& xmlNode);
    Error& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p/>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p/>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p/>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p/>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p/>
     */
    inline Error& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p/>
     */
    inline Error& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline Error& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p/>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p/>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p/>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p/>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p/>
     */
    inline Error& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p/>
     */
    inline Error& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline Error& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p/>
     */
    inline const Aws::String& GetCode() const{ return m_code; }

    /**
     * <p/>
     */
    inline bool CodeHasBeenSet() const { return m_codeHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetCode(const Aws::String& value) { m_codeHasBeenSet = true; m_code = value; }

    /**
     * <p/>
     */
    inline void SetCode(Aws::String&& value) { m_codeHasBeenSet = true; m_code = std::move(value); }

    /**
     * <p/>
     */
    inline void SetCode(const char* value) { m_codeHasBeenSet = true; m_code.assign(value); }

    /**
     * <p/>
     */
    inline Error& WithCode(const Aws::String& value) { SetCode(value); return *this;}

    /**
     * <p/>
     */
    inline Error& WithCode(Aws::String&& value) { SetCode(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline Error& WithCode(const char* value) { SetCode(value); return *this;}


    /**
     * <p/>
     */
    inline const Aws::String& GetMessage() const{ return m_message; }

    /**
     * <p/>
     */
    inline bool MessageHasBeenSet() const { return m_messageHasBeenSet; }

    /**
     * <p/>
     */
    inline void SetMessage(const Aws::String& value) { m_messageHasBeenSet = true; m_message = value; }

    /**
     * <p/>
     */
    inline void SetMessage(Aws::String&& value) { m_messageHasBeenSet = true; m_message = std::move(value); }

    /**
     * <p/>
     */
    inline void SetMessage(const char* value) { m_messageHasBeenSet = true; m_message.assign(value); }

    /**
     * <p/>
     */
    inline Error& WithMessage(const Aws::String& value) { SetMessage(value); return *this;}

    /**
     * <p/>
     */
    inline Error& WithMessage(Aws::String&& value) { SetMessage(std::move(value)); return *this;}

    /**
     * <p/>
     */
    inline Error& WithMessage(const char* value) { SetMessage(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    Aws::String m_code;
    bool m_codeHasBeenSet;

    Aws::String m_message;
    bool m_messageHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
