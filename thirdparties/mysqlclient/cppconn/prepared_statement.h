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




#ifndef _SQL_PREPARED_STATEMENT_H_
#define _SQL_PREPARED_STATEMENT_H_

#include <iostream>
#include "statement.h"


namespace sql
{

class Connection;
class ResultSet;
class ResultSetMetaData;
class ParameterMetaData;

class PreparedStatement : public Statement
{
public:
  virtual ~PreparedStatement() {}

  virtual void clearParameters() = 0;

  virtual bool execute(const sql::SQLString& sql) = 0;
  virtual bool execute() = 0;

  virtual ResultSet *executeQuery(const sql::SQLString& sql) = 0;
  virtual ResultSet *executeQuery() = 0;

  virtual int executeUpdate(const sql::SQLString& sql) = 0;
  virtual int executeUpdate() = 0;

  virtual ResultSetMetaData * getMetaData() = 0;

  virtual ParameterMetaData * getParameterMetaData() = 0;

  virtual bool getMoreResults() = 0;

  virtual void setBigInt(unsigned int parameterIndex, const sql::SQLString& value) = 0;

  virtual void setBlob(unsigned int parameterIndex, std::istream * blob) = 0;

  virtual void setBoolean(unsigned int parameterIndex, bool value) = 0;

  virtual void setDateTime(unsigned int parameterIndex, const sql::SQLString& value) = 0;

  virtual void setDouble(unsigned int parameterIndex, double value) = 0;

  virtual void setInt(unsigned int parameterIndex, int32_t value) = 0;

  virtual void setUInt(unsigned int parameterIndex, uint32_t value) = 0;

  virtual void setInt64(unsigned int parameterIndex, int64_t value) = 0;

  virtual void setUInt64(unsigned int parameterIndex, uint64_t value) = 0;

  virtual void setNull(unsigned int parameterIndex, int sqlType) = 0;

  virtual void setString(unsigned int parameterIndex, const sql::SQLString& value) = 0;

  virtual PreparedStatement * setResultSetType(sql::ResultSet::enum_type type) = 0;
};


} /* namespace sql */

#endif /* _SQL_PREPARED_STATEMENT_H_ */
