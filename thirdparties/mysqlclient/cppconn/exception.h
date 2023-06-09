/*
 * Copyright (c) 2008, 2020, Oracle and/or its affiliates.
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



#ifndef _SQL_EXCEPTION_H_
#define _SQL_EXCEPTION_H_

#include "build_config.h"
#include <stdexcept>
#include <string>
#include <memory>

namespace sql
{

#define MEMORY_ALLOC_OPERATORS(Class) \
  void* operator new(size_t size){ return ::operator new(size); }  \
  void* operator new(size_t, void*) noexcept; \
  void* operator new(size_t, const std::nothrow_t&) noexcept; \
  void* operator new[](size_t); \
  void* operator new[](size_t, void*) noexcept; \
  void* operator new[](size_t, const std::nothrow_t&) noexcept; \
  void* operator new(size_t N, std::allocator<Class>&);


class SQLException : public std::runtime_error
{
protected:
  const std::string sql_state;
  const int errNo;

public:
  SQLException(const SQLException& e) : std::runtime_error(e.what()), sql_state(e.sql_state), errNo(e.errNo) {}

  SQLException(const std::string& reason, const std::string& SQLState, int vendorCode) :
    std::runtime_error	(reason		),
    sql_state			(SQLState	),
    errNo				(vendorCode)
  {}

  SQLException(const std::string& reason, const std::string& SQLState) : std::runtime_error(reason), sql_state(SQLState), errNo(0) {}

  SQLException(const std::string& reason) : std::runtime_error(reason), sql_state("HY000"), errNo(0) {}

  SQLException() : std::runtime_error(""), sql_state("HY000"), errNo(0) {}

  const std::string & getSQLState() const
  {
    return sql_state;
  }

  const char * getSQLStateCStr() const
  {
    return sql_state.c_str();
  }


  int getErrorCode() const
  {
    return errNo;
  }

  virtual ~SQLException() noexcept {};

protected:
  MEMORY_ALLOC_OPERATORS(SQLException)
};

struct MethodNotImplementedException : public SQLException
{
  MethodNotImplementedException(const MethodNotImplementedException& e) : SQLException(e.what(), e.sql_state, e.errNo) { }
  MethodNotImplementedException(const std::string& reason) : SQLException(reason, "", 0) {}
};

struct InvalidArgumentException : public SQLException
{
  InvalidArgumentException(const InvalidArgumentException& e) : SQLException(e.what(), e.sql_state, e.errNo) { }
  InvalidArgumentException(const std::string& reason) : SQLException(reason, "", 0) {}
};

struct InvalidInstanceException : public SQLException
{
  InvalidInstanceException(const InvalidInstanceException& e) : SQLException(e.what(), e.sql_state, e.errNo) { }
  InvalidInstanceException(const std::string& reason) : SQLException(reason, "", 0) {}
};


struct NonScrollableException : public SQLException
{
  NonScrollableException(const NonScrollableException& e) : SQLException(e.what(), e.sql_state, e.errNo) { }
  NonScrollableException(const std::string& reason) : SQLException(reason, "", 0) {}
};

struct SQLUnsupportedOptionException : public SQLException
{
  SQLUnsupportedOptionException(const SQLUnsupportedOptionException& e, const std::string conn_option) :
    SQLException(e.what(), e.sql_state, e.errNo),
    option(conn_option )
  {}

  SQLUnsupportedOptionException(const std::string& reason, const std::string conn_option) :
    SQLException(reason, "", 0),
    option(conn_option )
  {}

  const char *getConnectionOption() const
  {
    return option.c_str();
  }

  ~SQLUnsupportedOptionException() noexcept {};
protected:
  const std::string option;
};


} /* namespace sql */

#endif /* _SQL_EXCEPTION_H_ */
