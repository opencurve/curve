/*
 * Project: curve
 * File Created: Thursday, 21st March 2019 3:15:15 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <sys/types.h>

#include "src/client/libcurve_define.h"

const char* ErrorNum2ErrorName(LIBCURVE_ERROR err) {
    switch (err) {
        case LIBCURVE_ERROR::OK:
            return "operation ok!";
        case LIBCURVE_ERROR::EXISTS:
            return "file exists!";
        case LIBCURVE_ERROR::FAILED:
            return "operation failed!";
        case LIBCURVE_ERROR::DISABLEIO:
            return "now io is disabled!";
        case LIBCURVE_ERROR::AUTHFAIL:
            return "auth failed!";
        case LIBCURVE_ERROR::DELETING:
            return "file is deleting!";
        case LIBCURVE_ERROR::NOTEXIST:
            return "file not exists!";
        case LIBCURVE_ERROR::UNDER_SNAPSHOT:
            return "file under snapshot";
        case LIBCURVE_ERROR::NOT_UNDERSNAPSHOT:
            return "file not under snapshot";
        case LIBCURVE_ERROR::DELETE_ERROR:
            return "file delete error!";
        default:
            return "unknown ret!";
    }
}
