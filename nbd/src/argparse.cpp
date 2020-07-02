/*
 * Project: curve
 * Created Date: Thursday April 23rd 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <experimental/string_view>
#include <limits.h>
#include <string.h>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>

#include "nbd/src/argparse.h"

namespace curve {
namespace nbd {

using std::experimental::string_view;
using std::ostringstream;

float strict_strtof(string_view str, std::string *err) {
    char *endptr;
    errno = 0; /* To distinguish success/failure after call (see man page) */
    float ret = strtof(str.data(), &endptr);
    if (errno == ERANGE) {
        ostringstream oss;
        oss << "strict_strtof: floating point overflow or underflow parsing '"
            << str << "'";
        *err = oss.str();
        return 0.0;
    }
    if (endptr == str) {
        ostringstream oss;
        oss << "strict_strtof: expected float, got: '" << str << "'";
        *err = oss.str();
        return 0;
    }
    if (*endptr != '\0') {
        ostringstream oss;
        oss << "strict_strtof: garbage at end of string. got: '" << str << "'";
        *err = oss.str();
        return 0;
    }
    *err = "";
    return ret;
}

float strict_strtof(const char *str, std::string *err) {
    return strict_strtof(string_view(str), err);
}

int64_t strict_strtoll(string_view str, int base, std::string *err) {
    char *endptr;
    errno = 0; /* To distinguish success/failure after call (see man page) */
    int64_t ret = strtoll(str.data(), &endptr, base);
    if (endptr == str.data() || endptr != str.data() + str.size()) {
        *err = (std::string {"Expected option value to be integer, got '"} +
                std::string {str} + "'");
        return 0;
    }
    if (errno) {
        *err = (std::string {"The option value '"} + std::string {str} +
                "' seems to be invalid");
        return 0;
    }
    *err = "";
    return ret;
}

int64_t strict_strtoll(const char *str, int base, std::string *err) {
    return strict_strtoll(string_view(str), base, err);
}

int strict_strtol(string_view str, int base, std::string *err) {
    int64_t ret = strict_strtoll(str, base, err);
    if (!err->empty())
        return 0;
    if ((ret < INT_MIN) || (ret > INT_MAX)) {
        ostringstream errStr;
        errStr << "The option value '" << str << "' seems to be invalid";
        *err = errStr.str();
        return 0;
    }
    return static_cast<int>(ret);
}

int strict_strtol(const char *str, int base, std::string *err) {
    return strict_strtol(string_view(str), base, err);
}

struct strict_str_convert {
    const char *str;
    std::string *err;
    strict_str_convert(const char *str,  std::string *err)
        : str(str), err(err) {}

    inline operator float() const {
        return strict_strtof(str, err);
    }
    inline operator int() const {
        return strict_strtol(str, 10, err);
    }
    inline operator int64_t() const {
        return  strict_strtoll(str, 10, err);
    }
};

static void dashes_to_underscores(const char *input, char *output) {
    char c = 0;
    char *o = output;
    const char *i = input;
    // first two characters are copied as-is
    *o = *i++;
    if (*o++ == '\0')
        return;
    *o = *i++;
    if (*o++ == '\0')
        return;
    for (; ((c = *i)); ++i) {
        if (c == '=') {
            strcpy(o, i);  // NOLINT
            return;
        }
        if (c == '-')
            *o++ = '_';
        else
            *o++ = c;
    }
    *o++ = '\0';
}

void arg_value_type(const char * nextargstr, bool *bool_option,
                    bool *bool_numeric) {
    bool is_numeric = true;
    bool is_float = false;
    bool is_option;

    if (nextargstr == NULL) {
        return;
    }

    if (strlen(nextargstr) < 2) {
        is_option = false;
    } else {
        is_option = (nextargstr[0] == '-') && (nextargstr[1] == '-');
    }

    for (unsigned int i = 0; i < strlen(nextargstr); i++) {
        if (!(nextargstr[i] >= '0' && nextargstr[i] <= '9')) {
            // May be negative numeral value
            if ((i == 0) && (strlen(nextargstr) >= 2))  {
                if (nextargstr[0] == '-') continue;
            }
            if ( (nextargstr[i] == '.') && (is_float == false) ) {
                is_float = true;
                continue;
            }
            is_numeric = false;
            break;
        }
    }

    // -<option>
    if (nextargstr[0] == '-' && is_numeric == false) {
        is_option = true;
    }

    *bool_option = is_option;
    *bool_numeric = is_numeric;

    return;
}

void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args) {  // NOLINT
    args.insert(args.end(), argv + 1, argv + argc);
}

bool argparse_flag(std::vector<const char*> &args,  // NOLINT
	               std::vector<const char*>::iterator &i, ...) {    // NOLINT
    const char *first = *i;
    const int kFirstLen = strlen(first);
    char tmp[kFirstLen+1];
    dashes_to_underscores(first, tmp);
    first = tmp;
    va_list ap;

    va_start(ap, i);
    while (1) {
        const char *a = va_arg(ap, char*);
        if (a == NULL) {
            va_end(ap);
            return false;
        }
        const int kStrLen = strlen(a);
        char a2[kStrLen+1];
        dashes_to_underscores(a, a2);
        if (strcmp(a2, first) == 0) {
            i = args.erase(i);
            va_end(ap);
            return true;
        }
    }
}

static int va_argparse_witharg(std::vector<const char*> &args,          // NOLINT
                               std::vector<const char*>::iterator &i,   // NOLINT
                               std::string *ret,
                               std::ostream &oss,
                               va_list ap) {
    const char *first = *i;
    const int kFirstLen = strlen(first);
    char tmp[kFirstLen+1];
    dashes_to_underscores(first, tmp);
    first = tmp;

    // does this argument match any of the possibilities?
    while (true) {
        const char *a = va_arg(ap, char*);
        if (a == NULL)
            return 0;
        const int kStrLen = strlen(a);
        char a2[kStrLen+1];
        dashes_to_underscores(a, a2);
        if (strncmp(a2, first, strlen(a2)) == 0) {
            if (first[kStrLen] == '=') {
                *ret = first + kStrLen + 1;
                i = args.erase(i);
                return 1;
            } else if (first[kStrLen] == '\0') {
                // find second part (or not)
                if (i+1 == args.end()) {
                    oss << "Option " << *i << " requires an argument."
                        << std::endl;
                    i = args.erase(i);
                    return -EINVAL;
                }
                i = args.erase(i);
                *ret = *i;
                i = args.erase(i);
                return 1;
            }
        }
    }
}

template<class T>
bool argparse_witharg(std::vector<const char*> &args,           // NOLINT
                      std::vector<const char*>::iterator &i,    // NOLINT
                      T *ret,
                      std::ostream &oss, ...) {
    int r;
    va_list ap;
    bool is_option = false;
    bool is_numeric = true;
    std::string str;
    va_start(ap, oss);
    r = va_argparse_witharg(args, i, &str, oss, ap);
    va_end(ap);
    if (r == 0) {
        return false;
    } else if (r < 0) {
        return true;
    }

    arg_value_type(str.c_str(), &is_option, &is_numeric);
    if ((is_option == true) || (is_numeric == false)) {
        *ret = EXIT_FAILURE;
        if (is_option == true) {
            oss << "Missing option value";
        } else {
            oss << "The option value '" << str << "' is invalid";
        }
        return true;
    }

    std::string err;
    T myret = strict_str_convert(str.c_str(), &err);
    *ret = myret;
    if (!err.empty()) {
        oss << err;
    }
    return true;
}

template bool argparse_witharg<int>(std::vector<const char*> &args,     // NOLINT
	std::vector<const char*>::iterator &i, int *ret,                    // NOLINT
	std::ostream &oss, ...);                                            // NOLINT

template bool argparse_witharg<int64_t>(std::vector<const char*> &args, // NOLINT
	std::vector<const char*>::iterator &i, int64_t *ret,                // NOLINT
	std::ostream &oss, ...);                                            // NOLINT

template bool argparse_witharg<float>(std::vector<const char*> &args,   // NOLINT
	std::vector<const char*>::iterator &i, float *ret,                  // NOLINT
	std::ostream &oss, ...);                                            // NOLINT

bool argparse_witharg(std::vector<const char*> &args,           // NOLINT
                      std::vector<const char*>::iterator &i,    // NOLINT
                      std::string *ret,
                      std::ostream &oss, ...) {
    int r;
    va_list ap;
    va_start(ap, oss);
    r = va_argparse_witharg(args, i, ret, oss, ap);
    va_end(ap);
    return r != 0;
}

}  // namespace nbd
}  // namespace curve
