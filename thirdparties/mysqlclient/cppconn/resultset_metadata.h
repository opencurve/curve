/*
 * Copyright (c) 2008, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */



#ifndef _SQL_RESULTSET_METADATA_H_
#define _SQL_RESULTSET_METADATA_H_

#include "sqlstring.h"
#include "datatype.h"

namespace sql
{

class ResultSetMetaData
{
public:
  enum
  {
    columnNoNulls,
    columnNullable,
    columnNullableUnknown
  };

  virtual SQLString getCatalogName(unsigned int column) = 0;

  virtual unsigned int getColumnCount() = 0;

  virtual unsigned int getColumnDisplaySize(unsigned int column) = 0;

  virtual SQLString getColumnLabel(unsigned int column) = 0;

  virtual SQLString getColumnName(unsigned int column) = 0;

  virtual int getColumnType(unsigned int column) = 0;

  virtual SQLString getColumnTypeName(unsigned int column) = 0;

  virtual SQLString getColumnCharset(unsigned int columnIndex) = 0;

  virtual SQLString getColumnCollation(unsigned int columnIndex) = 0;

  virtual unsigned int getPrecision(unsigned int column) = 0;

  virtual unsigned int getScale(unsigned int column) = 0;

  virtual SQLString getSchemaName(unsigned int column) = 0;

  virtual SQLString getTableName(unsigned int column) = 0;

  virtual bool isAutoIncrement(unsigned int column) = 0;

  virtual bool isCaseSensitive(unsigned int column) = 0;

  virtual bool isCurrency(unsigned int column) = 0;

  virtual bool isDefinitelyWritable(unsigned int column) = 0;

  virtual int isNullable(unsigned int column) = 0;

  virtual bool isNumeric(unsigned int column) = 0;

  virtual bool isReadOnly(unsigned int column) = 0;

  virtual bool isSearchable(unsigned int column) = 0;

  virtual bool isSigned(unsigned int column) = 0;

  virtual bool isWritable(unsigned int column) = 0;

  virtual bool isZerofill(unsigned int column) = 0;

protected:
  virtual ~ResultSetMetaData() {}
};


} /* namespace sql */

#endif /* _SQL_RESULTSET_METADATA_H_ */
