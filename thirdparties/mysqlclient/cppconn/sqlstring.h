/*
 * Copyright (c) 2008, 2019, Oracle and/or its affiliates. All rights reserved.
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



#ifndef _SQL_STRING_H_
#define _SQL_STRING_H_

#include <string>
#include <algorithm>
#include "build_config.h"
#include <iostream>

namespace sql
{
  class SQLString
  {
#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable: 4251)
#endif
    std::string realStr;
#ifdef _WIN32
#pragma warning(pop)
#endif

  public:
#ifdef _WIN32
        //TODO something less dirty-hackish.
        static const size_t npos = static_cast<std::string::size_type>(-1);
#else
    static const size_t npos = std::string::npos;
#endif

    ~SQLString() {}

    SQLString() {}

    SQLString(const SQLString & other) : realStr(other.realStr) {}

    SQLString(const std::string & other) : realStr(other) {}

    SQLString(const char other[]) : realStr(other) {}

    SQLString(const char * s, size_t n) : realStr(s, n) {}

    // Needed for stuff like SQLString str= "char * string constant"
    const SQLString & operator=(const char * s)
    {
      realStr = s;
      return *this;
    }

    const SQLString & operator=(const std::string & rhs)
    {
      realStr = rhs;
      return *this;
    }

    const SQLString & operator=(const SQLString & rhs)
    {
      realStr = rhs.realStr;
      return *this;
    }

    // Conversion to st::string. Comes in play for stuff like std::string str= SQLString_var;
    operator const std::string &() const
    {
      return realStr;
    }

    /** For access std::string methods. Not sure we need it. Makes it look like some smart ptr.
      possibly operator* - will look even more like smart ptr */
    std::string * operator ->()
    {
      return & realStr;
    }

    int compare(const SQLString& str) const
    {
      return realStr.compare(str.realStr);
    }

    int compare(const char * s) const
    {
      return realStr.compare(s);
    }

    int compare(size_t pos1, size_t n1, const char * s) const
    {
      return realStr.compare(pos1, n1, s);
    }

    int caseCompare(const SQLString &s) const
    {
      std::string tmp(realStr), str(s);
      std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
      std::transform(str.begin(), str.end(), str.begin(), ::tolower);
      return tmp.compare(str);
    }

    int caseCompare(const char * s) const
    {
      std::string tmp(realStr), str(s);
      std::transform(str.begin(), str.end(), str.begin(), ::tolower);
      std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
      return tmp.compare(str);
    }

    int caseCompare(size_t pos1, size_t n1, const char * s) const
    {
      std::string tmp(realStr.c_str() + pos1, n1), str(s);
      std::transform(str.begin(), str.end(), str.begin(), ::tolower);
      std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
      return tmp.compare(str);
    }

    const std::string & asStdString() const
    {
      return realStr;
    }

    const char * c_str() const
    {
      return realStr.c_str();
    }

    size_t length() const
    {
      return realStr.length();
    }

    SQLString & append(const std::string & str)
    {
      realStr.append(str);
      return *this;
    }

    SQLString & append(const char * s)
    {
      realStr.append(s);
      return *this;
    }

    const char& operator[](size_t pos) const
    {
      return realStr[pos];
    }

    size_t find(char c, size_t pos = 0) const
    {
      return realStr.find(c, pos);
    }

    size_t find(const SQLString & s, size_t pos = 0) const
    {
      return realStr.find(s.realStr, pos);
    }

    SQLString substr(size_t pos = 0, size_t n = npos) const
    {
      return realStr.substr(pos, n);
    }

    const SQLString& replace(size_t pos1, size_t n1, const SQLString & s)
    {
      realStr.replace(pos1, n1, s.realStr);
      return *this;
    }

    size_t find_first_of(char c, size_t pos = 0) const
    {
      return realStr.find_first_of(c, pos);
    }

    size_t find_last_of(char c, size_t pos = npos) const
    {
      return realStr.find_last_of(c, pos);
    }

    const SQLString & operator+=(const SQLString & op2)
    {
      realStr += op2.realStr;
      return *this;
    }
};


/*
  Operators that can and have to be not a member.
*/
inline const SQLString operator+(const SQLString & op1, const SQLString & op2)
{
  return sql::SQLString(op1.asStdString() + op2.asStdString());
}

inline bool operator ==(const SQLString & op1, const SQLString & op2)
{
  return (op1.asStdString() == op2.asStdString());
}

inline bool operator !=(const SQLString & op1, const SQLString & op2)
{
  return (op1.asStdString() != op2.asStdString());
}

inline bool operator <(const SQLString & op1, const SQLString & op2)
{
  return op1.asStdString() < op2.asStdString();
}


}// namespace sql


namespace std
{
  // operator << for SQLString output
  inline ostream & operator << (ostream & os, const sql::SQLString & str )
  {
    return os << str.asStdString();
  }
}
#endif
