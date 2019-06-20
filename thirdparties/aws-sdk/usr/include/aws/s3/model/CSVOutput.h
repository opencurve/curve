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
#include <aws/s3/model/QuoteFields.h>
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
   * <p>Describes how CSV-formatted results are formatted.</p><p><h3>See Also:</h3>  
   * <a href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/CSVOutput">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API CSVOutput
  {
  public:
    CSVOutput();
    CSVOutput(const Aws::Utils::Xml::XmlNode& xmlNode);
    CSVOutput& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline const QuoteFields& GetQuoteFields() const{ return m_quoteFields; }

    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline bool QuoteFieldsHasBeenSet() const { return m_quoteFieldsHasBeenSet; }

    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline void SetQuoteFields(const QuoteFields& value) { m_quoteFieldsHasBeenSet = true; m_quoteFields = value; }

    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline void SetQuoteFields(QuoteFields&& value) { m_quoteFieldsHasBeenSet = true; m_quoteFields = std::move(value); }

    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline CSVOutput& WithQuoteFields(const QuoteFields& value) { SetQuoteFields(value); return *this;}

    /**
     * <p>Indicates whether or not all output fields should be quoted.</p>
     */
    inline CSVOutput& WithQuoteFields(QuoteFields&& value) { SetQuoteFields(std::move(value)); return *this;}


    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline const Aws::String& GetQuoteEscapeCharacter() const{ return m_quoteEscapeCharacter; }

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline bool QuoteEscapeCharacterHasBeenSet() const { return m_quoteEscapeCharacterHasBeenSet; }

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline void SetQuoteEscapeCharacter(const Aws::String& value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter = value; }

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline void SetQuoteEscapeCharacter(Aws::String&& value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter = std::move(value); }

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline void SetQuoteEscapeCharacter(const char* value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter.assign(value); }

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline CSVOutput& WithQuoteEscapeCharacter(const Aws::String& value) { SetQuoteEscapeCharacter(value); return *this;}

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline CSVOutput& WithQuoteEscapeCharacter(Aws::String&& value) { SetQuoteEscapeCharacter(std::move(value)); return *this;}

    /**
     * <p>Th single character used for escaping the quote character inside an already
     * escaped value.</p>
     */
    inline CSVOutput& WithQuoteEscapeCharacter(const char* value) { SetQuoteEscapeCharacter(value); return *this;}


    /**
     * <p>The value used to separate individual records.</p>
     */
    inline const Aws::String& GetRecordDelimiter() const{ return m_recordDelimiter; }

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline bool RecordDelimiterHasBeenSet() const { return m_recordDelimiterHasBeenSet; }

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline void SetRecordDelimiter(const Aws::String& value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter = value; }

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline void SetRecordDelimiter(Aws::String&& value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter = std::move(value); }

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline void SetRecordDelimiter(const char* value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter.assign(value); }

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline CSVOutput& WithRecordDelimiter(const Aws::String& value) { SetRecordDelimiter(value); return *this;}

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline CSVOutput& WithRecordDelimiter(Aws::String&& value) { SetRecordDelimiter(std::move(value)); return *this;}

    /**
     * <p>The value used to separate individual records.</p>
     */
    inline CSVOutput& WithRecordDelimiter(const char* value) { SetRecordDelimiter(value); return *this;}


    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline const Aws::String& GetFieldDelimiter() const{ return m_fieldDelimiter; }

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline bool FieldDelimiterHasBeenSet() const { return m_fieldDelimiterHasBeenSet; }

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline void SetFieldDelimiter(const Aws::String& value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter = value; }

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline void SetFieldDelimiter(Aws::String&& value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter = std::move(value); }

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline void SetFieldDelimiter(const char* value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter.assign(value); }

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline CSVOutput& WithFieldDelimiter(const Aws::String& value) { SetFieldDelimiter(value); return *this;}

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline CSVOutput& WithFieldDelimiter(Aws::String&& value) { SetFieldDelimiter(std::move(value)); return *this;}

    /**
     * <p>The value used to separate individual fields in a record.</p>
     */
    inline CSVOutput& WithFieldDelimiter(const char* value) { SetFieldDelimiter(value); return *this;}


    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline const Aws::String& GetQuoteCharacter() const{ return m_quoteCharacter; }

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline bool QuoteCharacterHasBeenSet() const { return m_quoteCharacterHasBeenSet; }

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline void SetQuoteCharacter(const Aws::String& value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter = value; }

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline void SetQuoteCharacter(Aws::String&& value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter = std::move(value); }

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline void SetQuoteCharacter(const char* value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter.assign(value); }

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline CSVOutput& WithQuoteCharacter(const Aws::String& value) { SetQuoteCharacter(value); return *this;}

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline CSVOutput& WithQuoteCharacter(Aws::String&& value) { SetQuoteCharacter(std::move(value)); return *this;}

    /**
     * <p>The value used for escaping where the field delimiter is part of the
     * value.</p>
     */
    inline CSVOutput& WithQuoteCharacter(const char* value) { SetQuoteCharacter(value); return *this;}

  private:

    QuoteFields m_quoteFields;
    bool m_quoteFieldsHasBeenSet;

    Aws::String m_quoteEscapeCharacter;
    bool m_quoteEscapeCharacterHasBeenSet;

    Aws::String m_recordDelimiter;
    bool m_recordDelimiterHasBeenSet;

    Aws::String m_fieldDelimiter;
    bool m_fieldDelimiterHasBeenSet;

    Aws::String m_quoteCharacter;
    bool m_quoteCharacterHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
