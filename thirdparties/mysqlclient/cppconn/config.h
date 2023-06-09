/*
 * Copyright (c) 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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

// libmysql defines HAVE_STRTOUL (on win), so we have to follow different pattern in definitions names
// to avoid annoying warnings.

#define HAVE_FUNCTION_STRTOLD 1
#define HAVE_FUNCTION_STRTOLL 1
#define HAVE_FUNCTION_STRTOL 1
#define HAVE_FUNCTION_STRTOULL 1

#define HAVE_FUNCTION_STRTOUL 1

#define HAVE_FUNCTION_STRTOIMAX 1
#define HAVE_FUNCTION_STRTOUMAX 1

#define HAVE_STDINT_H 1
#define HAVE_INTTYPES_H 1

#define HAVE_INT8_T   1
#define HAVE_UINT8_T  1
#define HAVE_INT16_T  1
#define HAVE_UINT16_T 1
#define HAVE_INT32_T  1
#define HAVE_UINT32_T 1
#define HAVE_INT32_T  1
#define HAVE_UINT32_T 1
#define HAVE_INT64_T  1
#define HAVE_UINT64_T 1
/* #undef HAVE_MS_INT8 */
/* #undef HAVE_MS_UINT8 */
/* #undef HAVE_MS_INT16 */
/* #undef HAVE_MS_UINT16 */
/* #undef HAVE_MS_INT32 */
/* #undef HAVE_MS_UINT32 */
/* #undef HAVE_MS_INT64 */
/* #undef HAVE_MS_UINT64 */


#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif


#if defined(HAVE_INTTYPES_H) && !defined(_WIN32)
#include <inttypes.h>
#endif

#if defined(_WIN32)
#ifndef CPPCONN_DONT_TYPEDEF_MS_TYPES_TO_C99_TYPES

#if _MSC_VER >= 1600

#include <stdint.h>

#else

#if !defined(HAVE_INT8_T) && defined(HAVE_MS_INT8)
typedef __int8			int8_t;
#endif

#ifdef HAVE_MS_UINT8
typedef unsigned __int8	uint8_t;
#endif
#ifdef HAVE_MS_INT16
typedef __int16			int16_t;
#endif

#ifdef HAVE_MS_UINT16
typedef unsigned __int16	uint16_t;
#endif

#ifdef HAVE_MS_INT32
typedef __int32			int32_t;
#endif

#ifdef HAVE_MS_UINT32
typedef unsigned __int32	uint32_t;
#endif

#ifdef HAVE_MS_INT64
typedef __int64			int64_t;
#endif
#ifdef HAVE_MS_UINT64
typedef unsigned __int64	uint64_t;
#endif

#endif  // _MSC_VER >= 1600
#endif	// CPPCONN_DONT_TYPEDEF_MS_TYPES_TO_C99_TYPES
#endif	//	_WIN32
