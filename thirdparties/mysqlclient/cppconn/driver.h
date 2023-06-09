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



#ifndef _SQL_DRIVER_H_
#define _SQL_DRIVER_H_

#include "connection.h"
#include "build_config.h"
#include "callback.h"

namespace sql
{

class CPPCONN_PUBLIC_FUNC Driver
{
protected:
  virtual ~Driver() {}
public:
  // Attempts to make a database connection to the given URL.

  virtual Connection * connect(const sql::SQLString& hostName, const sql::SQLString& userName, const sql::SQLString& password) = 0;

  virtual Connection * connect(ConnectOptionsMap & options) = 0;

  virtual int getMajorVersion() = 0;

  virtual int getMinorVersion() = 0;

  virtual int getPatchVersion() = 0;

  virtual const sql::SQLString & getName() = 0;

  virtual void setCallBack(sql::Fido_Callback &cb) = 0;

  virtual void setCallBack(sql::Fido_Callback &&cb) = 0;

  virtual void threadInit() = 0;

  virtual void threadEnd() = 0;
};

} /* namespace sql */


CPPCONN_PUBLIC_FUNC void check(const std::string &);
CPPCONN_PUBLIC_FUNC void check(const std::map<std::string,std::string> &);

/*
  Checks if user standard lib is compatible with connector one
*/
inline static void check_lib()
{
  check(std::string{});
  check(std::map<std::string,std::string>{});
}

extern "C"
{

  CPPCONN_PUBLIC_FUNC sql::Driver * _get_driver_instance_by_name(const char * const clientlib);

  /* If dynamic loading is disabled in a driver then this function works just like get_driver_instance() */
  inline static sql::Driver * get_driver_instance_by_name(const char * const clientlib)
  {
    check_lib();
    return _get_driver_instance_by_name(clientlib);
  }

  inline static sql::Driver * get_driver_instance()
  {
    return get_driver_instance_by_name("");
  }
}

#endif /* _SQL_DRIVER_H_ */
