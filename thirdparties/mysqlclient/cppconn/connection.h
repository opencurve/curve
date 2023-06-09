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



#ifndef _SQL_CONNECTION_H_
#define _SQL_CONNECTION_H_

#include <map>

#include "build_config.h"
#include "warning.h"
#include "sqlstring.h"
#include "variant.h"

/*
 Options used on ConnectOptionsMap
*/

/*
 Connect related
*/
#define OPT_HOSTNAME   "hostName"
#define OPT_USERNAME   "userName"
#define OPT_PASSWORD   "password"
#define OPT_PASSWORD1  "password1"
#define OPT_PASSWORD2  "password2"
#define OPT_PASSWORD3  "password3"
#define OPT_PORT       "port"
#define OPT_SOCKET     "socket"
#define OPT_PIPE       "pipe"
#define OPT_SCHEMA     "schema"
#define OPT_MULTI_HOST "OPT_MULTI_HOST"
#define OPT_DNS_SRV    "OPT_DNS_SRV"
#define OPT_NAMED_PIPE "OPT_NAMED_PIPE"
#define OPT_INIT_COMMAND "preInit"
#define OPT_POST_INIT_COMMAND "postInit"
#define OPT_LOCAL_INFILE "OPT_LOCAL_INFILE"
#define OPT_LOAD_DATA_LOCAL_DIR "OPT_LOAD_DATA_LOCAL_DIR"

/*
 SSL related
*/
#define OPT_SSL_MODE          "ssl-mode"
#define OPT_SSL_KEY           "ssl-key"
#define OPT_SSL_CERT          "ssl-cert"
#define OPT_SSL_CA            "ssl-ca"
#define OPT_SSL_CAPATH        "ssl-capath"
#define OPT_SSL_CIPHER        "ssl-cipher"
#define OPT_SSL_CRL           "ssl-crl"
#define OPT_SSL_CRLPATH       "ssl-crlpath"
#define OPT_SERVER_PUBLIC_KEY "rsaKey"
#define OPT_TLS_VERSION       "tls-version"

/*
 Connection related
*/
#define OPT_RECONNECT          "OPT_RECONNECT"
#define OPT_RETRY_COUNT        "OPT_RETRY_COUNT"
#define OPT_CONNECT_TIMEOUT    "OPT_CONNECT_TIMEOUT"
#define OPT_READ_TIMEOUT       "OPT_READ_TIMEOUT"
#define OPT_WRITE_TIMEOUT      "OPT_WRITE_TIMEOUT"
#define OPT_MAX_ALLOWED_PACKET "OPT_MAX_ALLOWED_PACKET"
#define OPT_NET_BUFFER_LENGTH  "OPT_NET_BUFFER_LENGTH"

/*
 Connection Attributes
*/
#define OPT_CONNECT_ATTR_ADD    "OPT_CONNECT_ATTR_ADD"
#define OPT_CONNECT_ATTR_DELETE "OPT_CONNECT_ATTR_DELETE"
#define OPT_CONNECT_ATTR_RESET  "OPT_CONNECT_ATTR_RESET"

/*
 Authentication
*/
#define OPT_ENABLE_CLEARTEXT_PLUGIN      "OPT_ENABLE_CLEARTEXT_PLUGIN"
#define OPT_CAN_HANDLE_EXPIRED_PASSWORDS "OPT_CAN_HANDLE_EXPIRED_PASSWORDS"
#define OPT_GET_SERVER_PUBLIC_KEY        "OPT_GET_SERVER_PUBLIC_KEY"
#define OPT_LEGACY_AUTH                  "useLegacyAuth"
#define OPT_DEFAULT_AUTH                 "defaultAuth"

/*
 Charracter set results and Metadata
*/
#define OPT_CHARACTER_SET_RESULTS       "characterSetResults"
#define OPT_OPTIONAL_RESULTSET_METADATA "OPT_OPTIONAL_RESULTSET_METADATA"
#define OPT_REPORT_DATA_TRUNCATION      "OPT_REPORT_DATA_TRUNCATION"
#define OPT_CHARSET_NAME                "OPT_CHARSET_NAME"
#define OPT_DEFAULT_STMT_RESULT_TYPE    "defaultStatementResultType"

/*
  Client side options
 */
#define OPT_CLIENT_COMPRESS         "CLIENT_COMPRESS"
#define OPT_CLIENT_FOUND_ROWS       "CLIENT_FOUND_ROWS"
#define OPT_CLIENT_IGNORE_SIGPIPE   "CLIENT_IGNORE_SIGPIPE"
#define OPT_CLIENT_IGNORE_SPACE     "CLIENT_IGNORE_SPACE"
#define OPT_CLIENT_INTERACTIVE      "CLIENT_INTERACTIVE"
#define OPT_CLIENT_LOCAL_FILES      "CLIENT_LOCAL_FILES"
#define OPT_CLIENT_MULTI_STATEMENTS "CLIENT_MULTI_STATEMENTS"
#define OPT_CLIENT_NO_SCHEMA        "CLIENT_NO_SCHEMA"
#define OPT_SET_CHARSET_DIR         "charsetDir"
#define OPT_PLUGIN_DIR              "pluginDir"
#define OPT_READ_DEFAULT_GROUP      "readDefaultGroup"
#define OPT_READ_DEFAULT_FILE       "readDefaultFile"

/*
  Auth plugin options
*/
#define OPT_OCI_CONFIG_FILE "OPT_OCI_CONFIG_FILE"
#define OPT_AUTHENTICATION_KERBEROS_CLIENT_MODE \
  "OPT_AUTHENTICATION_KERBEROS_CLIENT_MODE"

namespace sql
{

typedef sql::Variant ConnectPropertyVal;

typedef std::map< sql::SQLString, ConnectPropertyVal > ConnectOptionsMap;

class DatabaseMetaData;
class PreparedStatement;
class Statement;
class Driver;

typedef enum transaction_isolation
{
  TRANSACTION_NONE= 0,
  TRANSACTION_READ_COMMITTED,
  TRANSACTION_READ_UNCOMMITTED,
  TRANSACTION_REPEATABLE_READ,
  TRANSACTION_SERIALIZABLE
} enum_transaction_isolation;

enum ssl_mode
{
  SSL_MODE_DISABLED= 1, SSL_MODE_PREFERRED, SSL_MODE_REQUIRED,
  SSL_MODE_VERIFY_CA, SSL_MODE_VERIFY_IDENTITY
};

class CPPCONN_PUBLIC_FUNC Savepoint
{
  /* Prevent use of these */
  Savepoint(const Savepoint &);
  void operator=(Savepoint &);
public:
  Savepoint() {};
  virtual ~Savepoint() {};
  virtual int getSavepointId() = 0;

  virtual sql::SQLString getSavepointName() = 0;
};


class CPPCONN_PUBLIC_FUNC Connection
{
  /* Prevent use of these */
  Connection(const Connection &);
  void operator=(Connection &);
public:

  Connection() {};

  virtual ~Connection() {};

  virtual void clearWarnings() = 0;

  virtual Statement *createStatement() = 0;

  virtual void close()  = 0;

  virtual void commit() = 0;

  virtual bool getAutoCommit() = 0;

  virtual sql::SQLString getCatalog() = 0;

  virtual Driver *getDriver() = 0;

  virtual sql::SQLString getSchema() = 0;

  virtual sql::SQLString getClientInfo() = 0;

  virtual void getClientOption(const sql::SQLString & optionName, void * optionValue) = 0;

  virtual sql::SQLString getClientOption(const sql::SQLString & optionName) = 0;

  virtual DatabaseMetaData * getMetaData() = 0;

  virtual enum_transaction_isolation getTransactionIsolation() = 0;

  virtual const SQLWarning * getWarnings() = 0;

  virtual bool isClosed() = 0;

  virtual bool isReadOnly() = 0;

  virtual bool isValid() = 0;

  virtual bool reconnect() = 0;

  virtual sql::SQLString nativeSQL(const sql::SQLString& sql) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql, int autoGeneratedKeys) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql, int* columnIndexes) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql, int resultSetType, int resultSetConcurrency) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) = 0;

  virtual PreparedStatement * prepareStatement(const sql::SQLString& sql, sql::SQLString columnNames[]) = 0;

  virtual void releaseSavepoint(Savepoint * savepoint) = 0;

  virtual void rollback() = 0;

  virtual void rollback(Savepoint * savepoint) = 0;

  virtual void setAutoCommit(bool autoCommit) = 0;

  virtual void setCatalog(const sql::SQLString& catalog) = 0;

  virtual void setSchema(const sql::SQLString& catalog) = 0;

  virtual sql::Connection * setClientOption(const sql::SQLString & optionName, const void * optionValue) = 0;

  virtual sql::Connection * setClientOption(const sql::SQLString & optionName, const sql::SQLString & optionValue) = 0;

  virtual void setHoldability(int holdability) = 0;

  virtual void setReadOnly(bool readOnly) = 0;

  virtual Savepoint * setSavepoint() = 0;

  virtual Savepoint * setSavepoint(const sql::SQLString& name) = 0;

  virtual void setTransactionIsolation(enum_transaction_isolation level) = 0;

  /* virtual void setTypeMap(Map map) = 0; */
};

} /* namespace sql */

#endif // _SQL_CONNECTION_H_
