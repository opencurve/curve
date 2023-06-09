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



#ifndef _SQL_BUILD_CONFIG_H_
#define _SQL_BUILD_CONFIG_H_

#ifdef STATIC_CONCPP
  #define CPPCONN_PUBLIC_FUNC
#endif


#if defined _MSC_VER

 #define DLL_EXPORT __declspec(dllexport)
 #define DLL_IMPORT __declspec(dllimport)
 #define DLL_LOCAL

#elif __GNUC__ >= 4

 #define DLL_EXPORT __attribute__ ((visibility ("default")))
 #define DLL_IMPORT
 #define DLL_LOCAL  __attribute__ ((visibility ("hidden")))

#elif defined __SUNPRO_CC || defined __SUNPRO_C

 #define DLL_EXPORT __global
 #define DLL_IMPORT __global
 #define DLL_LOCAL  __hidden

#else

 #define DLL_EXPORT
 #define DLL_IMPORT
 #define DLL_LOCAL

#endif


#ifndef CPPCONN_PUBLIC_FUNC

  #ifdef connector_jdbc_EXPORTS
    #define CPPCONN_PUBLIC_FUNC DLL_EXPORT
  #else
    // this is for static build
    #ifdef CPPCONN_LIB_BUILD
      #define CPPCONN_PUBLIC_FUNC
    #else
      // this is for clients using dynamic lib
      #define CPPCONN_PUBLIC_FUNC DLL_IMPORT
    #endif
  #endif

#endif


#ifdef _MSC_VER

  /*
    Warning 4251 is about non dll-interface classes being used by ones exported
    from our DLL (for example std lib classes or Boost ones). Following
    the crowd, we ignore this issue for now.
  */

  __pragma(warning (disable:4251))

#elif defined __SUNPRO_CC || defined __SUNPRO_C
#else

  /*
    These are triggered by, e.g., std::auto_ptr<> which is used by Boost.
  */

  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#endif

#endif    //#ifndef _SQL_BUILD_CONFIG_H_
