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



#ifndef _SQL_STATEMENT_H_
#define _SQL_STATEMENT_H_

#include "config.h"
#include "resultset.h"

#include <string>

namespace sql
{

class ResultSet;
class Connection;
class SQLWarning;


class Statement
{
public:
  virtual ~Statement() {};

  virtual Connection * getConnection() = 0;

  virtual void cancel() = 0;

  virtual void clearWarnings() = 0;

  virtual void close() = 0;

  virtual bool execute(const sql::SQLString& sql) = 0;

  virtual ResultSet * executeQuery(const sql::SQLString& sql) = 0;

  virtual int executeUpdate(const sql::SQLString& sql) = 0;

  virtual size_t getFetchSize() = 0;

  virtual unsigned int getMaxFieldSize() = 0;

  virtual uint64_t getMaxRows() = 0;

  virtual bool getMoreResults() = 0;

  virtual unsigned int getQueryTimeout() = 0;

  virtual ResultSet * getResultSet() = 0;

  virtual sql::ResultSet::enum_type getResultSetType() = 0;

  virtual uint64_t getUpdateCount() = 0;

  virtual const SQLWarning * getWarnings() = 0;

  virtual void setCursorName(const sql::SQLString & name) = 0;

  virtual void setEscapeProcessing(bool enable) = 0;

  virtual void setFetchSize(size_t rows) = 0;

  virtual void setMaxFieldSize(unsigned int max) = 0;

  virtual void setMaxRows(unsigned int max) = 0;

  virtual void setQueryTimeout(unsigned int seconds) = 0;

  virtual Statement * setResultSetType(sql::ResultSet::enum_type type) = 0;

  virtual int setQueryAttrBigInt(const sql::SQLString &name, const sql::SQLString& value) = 0;
  virtual int setQueryAttrBoolean(const sql::SQLString &name, bool value) = 0;
  virtual int setQueryAttrDateTime(const sql::SQLString &name, const sql::SQLString& value) = 0;
  virtual int setQueryAttrDouble(const sql::SQLString &name, double value) = 0;
  virtual int setQueryAttrInt(const sql::SQLString &name, int32_t value) = 0;
  virtual int setQueryAttrUInt(const sql::SQLString &name, uint32_t value) = 0;
  virtual int setQueryAttrInt64(const sql::SQLString &name, int64_t value) = 0;
  virtual int setQueryAttrUInt64(const sql::SQLString &name, uint64_t value) = 0;
  virtual int setQueryAttrNull(const sql::SQLString &name) = 0;
  virtual int setQueryAttrString(const sql::SQLString &name, const sql::SQLString& value) = 0;

  virtual void clearAttributes() = 0;

};

} /* namespace sql */

#endif /* _SQL_STATEMENT_H_ */
