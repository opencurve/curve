#ifndef AWS_COMMON_ERROR_H
#define AWS_COMMON_ERROR_H

/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/common/common.h>
#include <aws/common/exports.h>

struct aws_error_info {
    int error_code;
    const char *literal_name;
    const char *error_str;
    const char *lib_name;
    const char *formatted_name;
};

struct aws_error_info_list {
    const struct aws_error_info *error_list;
    uint16_t count;
};

#define AWS_DEFINE_ERROR_INFO(C, ES, LN)                                                                               \
    {                                                                                                                  \
        .literal_name = #C, .error_code = (C), .error_str = (ES), .lib_name = (LN),                                    \
        .formatted_name = LN ": " #C ", " ES,                                                                          \
    }

typedef void(aws_error_handler_fn)(int err, void *ctx);

AWS_EXTERN_C_BEGIN

/*
 * Returns the latest error code on the current thread, or 0 if none have
 * occurred.
 */
AWS_COMMON_API
int aws_last_error(void);

/*
 * Returns the error str corresponding to `err`.
 */
AWS_COMMON_API
const char *aws_error_str(int err);

/*
 * Returns the error lib name corresponding to `err`.
 */
AWS_COMMON_API
const char *aws_error_lib_name(int err);

/*
 * Returns libname concatenated with error string.
 */
AWS_COMMON_API
const char *aws_error_debug_str(int err);

/*
 * Internal implementation detail.
 */
AWS_COMMON_API
void aws_raise_error_private(int err);

/*
 * Raises `err` to the installed callbacks, and sets the thread's error.
 */
AWS_STATIC_IMPL
int aws_raise_error(int err) {
    /*
     * Certain static analyzers can't see through the out-of-line call to aws_raise_error,
     * and assume that this might return AWS_OP_SUCCESS. We'll put the return inline just
     * to help with their assumptions.
     */

    aws_raise_error_private(err);

    return AWS_OP_ERR;
}
/*
 * Resets the `err` back to defaults
 */
AWS_COMMON_API
void aws_reset_error(void);
/*
 * Sets `err` to the latest error. Does not invoke callbacks.
 */
AWS_COMMON_API
void aws_restore_error(int err);

/*
 * Sets an application wide error handler function. This will be overridden by
 * the thread local handler. The previous handler is returned, this can be used
 * for restoring an error handler if it needs to be overridden temporarily.
 * Setting this to NULL will turn off this error callback after it has been
 * enabled.
 */
AWS_COMMON_API
aws_error_handler_fn *aws_set_global_error_handler_fn(aws_error_handler_fn *handler, void *ctx);

/*
 * Sets a thread-local error handler function. This will override the global
 * handler. The previous handler is returned, this can be used for restoring an
 * error handler if it needs to be overridden temporarily. Setting this to NULL
 * will turn off this error callback after it has been enabled.
 */
AWS_COMMON_API
aws_error_handler_fn *aws_set_thread_local_error_handler_fn(aws_error_handler_fn *handler, void *ctx);

/** TODO: this needs to be a private function (wait till we have the cmake story
 * better before moving it though). It should be external for the purpose of
 * other libs we own, but customers should not be able to hit it without going
 * out of their way to do so.
 */
AWS_COMMON_API
void aws_register_error_info(const struct aws_error_info_list *error_info);

AWS_EXTERN_C_END

#endif /* AWS_COMMON_ERROR_H */
