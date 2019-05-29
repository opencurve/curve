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
#include <aws/core/utils/memory/stl/AWSVector.h>
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
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/CORSRule">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API CORSRule
  {
  public:
    CORSRule();
    CORSRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    CORSRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedHeaders() const{ return m_allowedHeaders; }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline bool AllowedHeadersHasBeenSet() const { return m_allowedHeadersHasBeenSet; }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline void SetAllowedHeaders(const Aws::Vector<Aws::String>& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = value; }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline void SetAllowedHeaders(Aws::Vector<Aws::String>&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = std::move(value); }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline CORSRule& WithAllowedHeaders(const Aws::Vector<Aws::String>& value) { SetAllowedHeaders(value); return *this;}

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline CORSRule& WithAllowedHeaders(Aws::Vector<Aws::String>&& value) { SetAllowedHeaders(std::move(value)); return *this;}

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline CORSRule& AddAllowedHeaders(const Aws::String& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline CORSRule& AddAllowedHeaders(Aws::String&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(std::move(value)); return *this; }

    /**
     * <p>Specifies which headers are allowed in a pre-flight OPTIONS request.</p>
     */
    inline CORSRule& AddAllowedHeaders(const char* value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }


    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedMethods() const{ return m_allowedMethods; }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline bool AllowedMethodsHasBeenSet() const { return m_allowedMethodsHasBeenSet; }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline void SetAllowedMethods(const Aws::Vector<Aws::String>& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = value; }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline void SetAllowedMethods(Aws::Vector<Aws::String>&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = std::move(value); }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline CORSRule& WithAllowedMethods(const Aws::Vector<Aws::String>& value) { SetAllowedMethods(value); return *this;}

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline CORSRule& WithAllowedMethods(Aws::Vector<Aws::String>&& value) { SetAllowedMethods(std::move(value)); return *this;}

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline CORSRule& AddAllowedMethods(const Aws::String& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline CORSRule& AddAllowedMethods(Aws::String&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(std::move(value)); return *this; }

    /**
     * <p>Identifies HTTP methods that the domain/origin specified in the rule is
     * allowed to execute.</p>
     */
    inline CORSRule& AddAllowedMethods(const char* value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }


    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedOrigins() const{ return m_allowedOrigins; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline bool AllowedOriginsHasBeenSet() const { return m_allowedOriginsHasBeenSet; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline void SetAllowedOrigins(const Aws::Vector<Aws::String>& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = value; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline void SetAllowedOrigins(Aws::Vector<Aws::String>&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = std::move(value); }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& WithAllowedOrigins(const Aws::Vector<Aws::String>& value) { SetAllowedOrigins(value); return *this;}

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& WithAllowedOrigins(Aws::Vector<Aws::String>&& value) { SetAllowedOrigins(std::move(value)); return *this;}

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(const Aws::String& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(Aws::String&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(std::move(value)); return *this; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(const char* value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }


    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline const Aws::Vector<Aws::String>& GetExposeHeaders() const{ return m_exposeHeaders; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline bool ExposeHeadersHasBeenSet() const { return m_exposeHeadersHasBeenSet; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline void SetExposeHeaders(const Aws::Vector<Aws::String>& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = value; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline void SetExposeHeaders(Aws::Vector<Aws::String>&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = std::move(value); }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline CORSRule& WithExposeHeaders(const Aws::Vector<Aws::String>& value) { SetExposeHeaders(value); return *this;}

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline CORSRule& WithExposeHeaders(Aws::Vector<Aws::String>&& value) { SetExposeHeaders(std::move(value)); return *this;}

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline CORSRule& AddExposeHeaders(const Aws::String& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline CORSRule& AddExposeHeaders(Aws::String&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(std::move(value)); return *this; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript XMLHttpRequest
     * object).</p>
     */
    inline CORSRule& AddExposeHeaders(const char* value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }


    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline int GetMaxAgeSeconds() const{ return m_maxAgeSeconds; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline bool MaxAgeSecondsHasBeenSet() const { return m_maxAgeSecondsHasBeenSet; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline void SetMaxAgeSeconds(int value) { m_maxAgeSecondsHasBeenSet = true; m_maxAgeSeconds = value; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline CORSRule& WithMaxAgeSeconds(int value) { SetMaxAgeSeconds(value); return *this;}

  private:

    Aws::Vector<Aws::String> m_allowedHeaders;
    bool m_allowedHeadersHasBeenSet;

    Aws::Vector<Aws::String> m_allowedMethods;
    bool m_allowedMethodsHasBeenSet;

    Aws::Vector<Aws::String> m_allowedOrigins;
    bool m_allowedOriginsHasBeenSet;

    Aws::Vector<Aws::String> m_exposeHeaders;
    bool m_exposeHeadersHasBeenSet;

    int m_maxAgeSeconds;
    bool m_maxAgeSecondsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
