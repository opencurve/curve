/*
 * Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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



#ifndef _SQL_VARIANT_H_
#define _SQL_VARIANT_H_

#include <string>
#include <map>
#include <list>
#include <algorithm>
#include <typeinfo>

#include "build_config.h"
#include "sqlstring.h"
#include "exception.h"

namespace sql
{

class BaseVariantImpl
{
public:
  BaseVariantImpl (void *ptr, sql::SQLString vtype) :
    cvptr(ptr),
    vTypeName(vtype)
  {}

  virtual ~BaseVariantImpl() {
    cvptr=NULL;
  }

  virtual BaseVariantImpl* Clone()=0;

  template<class T>
  T* get() const {
    if (typeid(T).name() == typeid(void).name()) {
      return static_cast< T * > (cvptr);
    }

    if ((vTypeName == typeid(std::string).name() &&
      typeid(T).name() == typeid(sql::SQLString).name()) ||
      (vTypeName == typeid(sql::SQLString).name() &&
      typeid(T).name() == typeid(std::string).name())    ||
      (vTypeName == typeid(std::map< std::string, std::string >).name() &&
      typeid(T).name() ==
        typeid(std::map< sql::SQLString, sql::SQLString >).name()) ||
      (vTypeName ==
        typeid(std::map< sql::SQLString, sql::SQLString >).name() &&
      typeid(T).name() ==
        typeid(std::map< std::string, std::string >).name()) ||
      (vTypeName == typeid(std::list< std::string >).name() &&
      typeid(T).name() ==
        typeid(std::list< sql::SQLString >).name()) ||
      (vTypeName ==
        typeid(std::list< sql::SQLString >).name() &&
      typeid(T).name() ==
        typeid(std::list< std::string >).name()))
    {
      return static_cast< T * > (cvptr);
    }

    if (typeid(T).name() != vTypeName) {
      throw sql::InvalidArgumentException("Variant type doesn't match.");
    }

    return static_cast< T * > (cvptr);
    }

protected:
  void *cvptr;
  sql::SQLString vTypeName;
};


template<class T>
class  VariantImpl : public BaseVariantImpl
{
public:
  VariantImpl(T i) : BaseVariantImpl(new T(i), typeid(i).name()) {}

  ~VariantImpl() {
    destroy_content();
  }

  VariantImpl(VariantImpl& that) : BaseVariantImpl(that) {
    copy_content(that);
  }

  VariantImpl& operator=(VariantImpl& that) {
    if (this != &that) {
      destroy_content();
      if (cvptr == NULL) {
        copy_content(that);
      }
    }
    return *this;
  }

  virtual VariantImpl* Clone() {
    return new VariantImpl(*this);
  }

private:

  void destroy_content() {
    T *tmp=static_cast< T * >(cvptr);
    if (tmp) {
      delete tmp;
      cvptr=NULL;
    }
  }

  void copy_content(BaseVariantImpl& that) {
    cvptr=new T (*(static_cast< T * > (that.get< void >())));
  }
};


template<class T>
class  VariantMap : public BaseVariantImpl
{
public:
  VariantMap(T i) : BaseVariantImpl(new T(i), typeid(i).name()) {}

  ~VariantMap() {
    destroy_content();
  }

  VariantMap(VariantMap& that) : BaseVariantImpl(that) {
    if (this != &that) {
      copy_content(that);
    }
  }

  VariantMap& operator=(VariantMap& that) {
    if (this != &that) {
      destroy_content();
      copy_content(that);
    }
    return *this;
  }

  virtual VariantMap* Clone() {
    return new VariantMap(*this);
  }


private:
  void destroy_content() {
    T *tmp=static_cast< T *> (cvptr);
    if (tmp) {
      tmp->clear();
      delete tmp;
      cvptr=NULL;
    }
  }

  void copy_content(VariantMap& var) {
    T *tmp=static_cast< T *> (var.cvptr);
    if (tmp) {
      cvptr=new T();
      typename T::const_iterator cit=tmp->begin();
      while(cit != tmp->end()) {
        (static_cast< T * >(cvptr))->insert(
            std::make_pair(sql::SQLString(cit->first),
                    sql::SQLString(cit->second)));
        ++cit;
      }
    }
  }
};


template<class T>
class  VariantList : public BaseVariantImpl
{
public:
  VariantList(T i) : BaseVariantImpl(new T(i), typeid(i).name()) {}

  ~VariantList() {
    destroy_content();
  }

  VariantList(VariantList& that) : BaseVariantImpl(that) {
    if (this != &that) {
      copy_content(that);
    }
  }

  VariantList& operator=(VariantList& that) {
    if (this != &that) {
      destroy_content();
      copy_content(that);
    }
    return *this;
  }

  virtual VariantList* Clone() {
    return new VariantList(*this);
  }


private:
  void destroy_content()
  {
    T *tmp=static_cast< T *> (cvptr);
    if (tmp) {
      tmp->clear();
      delete tmp;
      cvptr=NULL;
    }
  }

  void copy_content(VariantList& var)
  {
    T *tmp=static_cast< T *> (var.cvptr);
    if (tmp) {
      cvptr=new T();
      typename T::const_iterator cit=tmp->begin();
      while(cit != tmp->end()) {
        (static_cast< T * >(cvptr))->push_back(sql::SQLString(*cit));
        ++cit;
      }
    }
  }
};


class CPPCONN_PUBLIC_FUNC Variant
{
public:
  Variant(const int &i=0) :
    variant(new VariantImpl< int >(i)) {}

  Variant(const double &i) :
    variant(new VariantImpl< double >(i)) {}

  Variant(const bool &i) :
    variant(new VariantImpl< bool >(i)) {}

  Variant(const char* i) :
    variant(new VariantImpl< sql::SQLString >(i)) {}

  Variant(const std::string &i) :
    variant(new VariantImpl< sql::SQLString >(i)) {}

  Variant(const sql::SQLString &i) :
    variant(new VariantImpl< sql::SQLString >(i)) {}

  Variant(const std::list< std::string > &i) :
    variant(new VariantList< std::list < std::string > >(i)) {}

  Variant(const std::list< sql::SQLString > &i) :
    variant(new VariantList< std::list < sql::SQLString > >(i)) {}

  Variant(const std::map< std::string, std::string > &i) :
    variant(new VariantMap< std::map< std::string, std::string > >(i)) {}

  Variant(const std::map< sql::SQLString, sql::SQLString > &i) :
    variant(new VariantMap< std::map< sql::SQLString, sql::SQLString > >(i)) {}

  ~Variant() {
    if (variant) {
      delete variant;
      variant=0;
    }
  }

  Variant(const Variant& that) {
    if (this != &that) {
      variant=that.variant->Clone();
    }
  }

  Variant& operator=(const Variant& that) {
    if (this != &that) {
      delete variant;
      variant=that.variant->Clone();
    }
    return *this;
  }

  template<class T>
  T* get() const {
    return variant->get<T>();
  }

private:
  BaseVariantImpl *variant;
};


} /* namespace sql */

#endif /* _SQL_VARIANT_H_ */
