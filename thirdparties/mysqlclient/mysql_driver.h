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



#ifndef _MYSQL_DRIVER_H_
#define _MYSQL_DRIVER_H_


#include "cppconn/driver.h"

#include <memory>

extern "C"
{
CPPCONN_PUBLIC_FUNC void  * sql_mysql_get_driver_instance();
}

namespace sql
{
namespace mysql
{
namespace NativeAPI
{
  class NativeDriverWrapper;
}

//class sql::mysql::NativeAPI::NativeDriverWrapper;

class CPPCONN_PUBLIC_FUNC MySQL_Driver : public sql::Driver
{
#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable: 4251)
#endif
  std::unique_ptr< ::sql::mysql::NativeAPI::NativeDriverWrapper > proxy;
#ifdef _WIN32
#pragma warning(pop)
#endif

  ::sql::Fido_Callback* fido_callback = nullptr;
  ::sql::Fido_Callback fido_callback_store;

public:
  MySQL_Driver();
  MySQL_Driver(const ::sql::SQLString & clientLib);

  virtual ~MySQL_Driver();

  sql::Connection * connect(const sql::SQLString& hostName, const sql::SQLString& userName, const sql::SQLString& password);

  sql::Connection * connect(sql::ConnectOptionsMap & options);

  int getMajorVersion();

  int getMinorVersion();

  int getPatchVersion();

  const sql::SQLString & getName();

  void setCallBack(sql::Fido_Callback &cb);

  void setCallBack(sql::Fido_Callback &&cb);

  void threadInit();

  void threadEnd();

private:
  /* Prevent use of these */
  MySQL_Driver(const MySQL_Driver &);
  void operator=(MySQL_Driver &);

  friend MySQL_Connection;

};

/** We do not hide the function if MYSQLCLIENT_STATIC_BINDING(or anything else) not defined
    because the counterpart C function is declared in the cppconn and is always visible.
    If dynamic loading is not enabled then its result is just like of get_driver_instance()
*/

CPPCONN_PUBLIC_FUNC MySQL_Driver * _get_driver_instance_by_name(const char * const clientlib);

inline static MySQL_Driver * get_driver_instance_by_name(const char * const clientlib)
{
  check_lib();
  return sql::mysql::_get_driver_instance_by_name(clientlib);
}

inline static MySQL_Driver * get_driver_instance()
{
  return sql::mysql::get_driver_instance_by_name("");
}

inline static MySQL_Driver *get_mysql_driver_instance() { return get_driver_instance(); }

} /* namespace mysql */
} /* namespace sql */

#endif // _MYSQL_DRIVER_H_

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
