#ifndef AWS_TESTING_AWS_TEST_HARNESS_H
#define AWS_TESTING_AWS_TEST_HARNESS_H
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
#include <aws/common/error.h>
#include <aws/common/mutex.h>

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef AWS_UNSTABLE_TESTING_API
#    error The AWS Test Fixture is designed only for use by AWS owned libraries for the AWS C99 SDK. You are welcome to use it,   \
but you should be aware we make no promises on the stability of this API.  To enable use of the aws test fixtures, set \
the AWS_UNSTABLE_TESTING_API compiler flag
#endif

#ifndef AWS_TESTING_REPORT_FD
#    define AWS_TESTING_REPORT_FD stderr
#endif

#if _MSC_VER
#    pragma warning(disable : 4221) /* aggregate initializer using local variable addresses */
#    pragma warning(disable : 4204) /* non-constant aggregate initializer */
#endif

struct memory_test_allocator {
    size_t allocated;
    size_t freed;
    struct aws_mutex mutex;
};

struct memory_test_tracker {
    size_t size;
    void *blob;
};

static inline void *s_mem_acquire_malloc(struct aws_allocator *allocator, size_t size) {
    struct memory_test_allocator *test_allocator = (struct memory_test_allocator *)allocator->impl;

    aws_mutex_lock(&test_allocator->mutex);
    test_allocator->allocated += size;
    struct memory_test_tracker *memory =
        (struct memory_test_tracker *)malloc(size + sizeof(struct memory_test_tracker));

    if (!memory) {
        return NULL;
    }

    memory->size = size;
    memory->blob = (uint8_t *)memory + sizeof(struct memory_test_tracker);
    aws_mutex_unlock(&test_allocator->mutex);
    return memory->blob;
}

static inline void s_mem_release_free(struct aws_allocator *allocator, void *ptr) {
    struct memory_test_allocator *test_allocator = (struct memory_test_allocator *)allocator->impl;

    struct memory_test_tracker *memory =
        (struct memory_test_tracker *)((uint8_t *)ptr - sizeof(struct memory_test_tracker));
    aws_mutex_lock(&test_allocator->mutex);
    test_allocator->freed += memory->size;
    aws_mutex_unlock(&test_allocator->mutex);
    free(memory);
}

/** Prints a message to stdout using printf format that appends the function, file and line number.
 * If format is null, returns 0 without printing anything; otherwise returns 1.
 */
static int s_cunit_failure_message0(
    const char *prefix,
    const char *function,
    const char *file,
    int line,
    const char *format,
    ...) {
    if (!format)
        return 0;

    fprintf(AWS_TESTING_REPORT_FD, "%s", prefix);

    va_list ap;
    va_start(ap, format);
    vfprintf(AWS_TESTING_REPORT_FD, format, ap);
    va_end(ap);

    fprintf(AWS_TESTING_REPORT_FD, " [%s():%s@#%d]\n", function, file, line);

    return 1;
}

#define FAIL_PREFIX "***FAILURE*** "
#define CUNIT_FAILURE_MESSAGE(func, file, line, format, ...)                                                           \
    s_cunit_failure_message0(FAIL_PREFIX, func, file, line, format, #__VA_ARGS__)

static int total_failures;

#define SUCCESS (0)
#define FAILURE (-1)

#define RETURN_SUCCESS(format, ...)                                                                                    \
    do {                                                                                                               \
        printf(format, ##__VA_ARGS__);                                                                                 \
        printf("\n");                                                                                                  \
        return SUCCESS;                                                                                                \
    } while (0)
#define PRINT_FAIL_INTERNAL(...)                                                                                       \
    CUNIT_FAILURE_MESSAGE(__FUNCTION__, __FILE__, __LINE__, ##__VA_ARGS__, (const char *)NULL)

#define PRINT_FAIL_INTERNAL0(...)                                                                                      \
    s_cunit_failure_message0(FAIL_PREFIX, __FUNCTION__, __FILE__, __LINE__, ##__VA_ARGS__, (const char *)NULL)

#define POSTFAIL_INTERNAL()                                                                                            \
    do {                                                                                                               \
        total_failures++;                                                                                              \
        return FAILURE;                                                                                                \
    } while (0)

#define FAIL(format, ...)                                                                                              \
    do {                                                                                                               \
        PRINT_FAIL_INTERNAL0(format, ##__VA_ARGS__);                                                                   \
        POSTFAIL_INTERNAL();                                                                                           \
    } while (0)

#define ASSERT_TRUE(condition, ...)                                                                                    \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("Expected condition to be true: " #condition);                                    \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_FALSE(condition, ...)                                                                                   \
    do {                                                                                                               \
        if ((condition)) {                                                                                             \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("Expected condition to be false: " #condition);                                   \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_SUCCESS(condition, ...)                                                                                 \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        if (assert_rv != AWS_OP_SUCCESS) {                                                                             \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "Expected success at %s; got return value %d with last error 0x%04x\n",                            \
                    #condition,                                                                                        \
                    assert_rv,                                                                                         \
                    aws_last_error());                                                                                 \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_FAILS(condition, ...)                                                                                   \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        if (assert_rv != AWS_OP_ERR) {                                                                                 \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "Expected failure at %s; got return value %d with last error 0x%04x\n",                            \
                    #condition,                                                                                        \
                    assert_rv,                                                                                         \
                    aws_last_error());                                                                                 \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_ERROR(error, condition, ...)                                                                            \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        int assert_err = aws_last_error();                                                                             \
        int assert_err_expect = (error);                                                                               \
        if (assert_rv != AWS_OP_ERR) {                                                                                 \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sExpected error but no error occurred; rv=%d, aws_last_error=%04x (expected %04x): ",                \
                FAIL_PREFIX,                                                                                           \
                assert_rv,                                                                                             \
                assert_err,                                                                                            \
                assert_err_expect);                                                                                    \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #condition);                                                                \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
        if (assert_err != assert_err_expect) {                                                                         \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sIncorrect error code; aws_last_error=%04x (expected %04x): ",                                       \
                FAIL_PREFIX,                                                                                           \
                assert_err,                                                                                            \
                assert_err_expect);                                                                                    \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #condition);                                                                \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_NULL(ptr, ...)                                                                                          \
    do {                                                                                                               \
        /* XXX: Some tests use ASSERT_NULL on ints... */                                                               \
        void *assert_p = (void *)(uintptr_t)(ptr);                                                                     \
        if (assert_p) {                                                                                                \
            fprintf(AWS_TESTING_REPORT_FD, "%sExpected null but got %p: ", FAIL_PREFIX, assert_p);                     \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #ptr);                                                                      \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_NOT_NULL(ptr, ...)                                                                                      \
    do {                                                                                                               \
        /* XXX: Some tests use ASSERT_NULL on ints... */                                                               \
        void *assert_p = (void *)(uintptr_t)(ptr);                                                                     \
        if (!assert_p) {                                                                                               \
            fprintf(AWS_TESTING_REPORT_FD, "%sExpected non-null but got null: ", FAIL_PREFIX);                         \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #ptr);                                                                      \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_TYP_EQUALS(type, formatarg, expected, got, ...)                                                         \
    do {                                                                                                               \
        type assert_expected = (expected);                                                                             \
        type assert_actual = (got);                                                                                    \
        if (assert_expected != assert_actual) {                                                                        \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%s" formatarg " != " formatarg ": ",                                                                  \
                FAIL_PREFIX,                                                                                           \
                assert_expected,                                                                                       \
                assert_actual);                                                                                        \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s != %s", #expected, #got);                                                     \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#ifdef _MSC_VER
#    define ASSERT_INT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(intmax_t, "%lld", expected, got, __VA_ARGS__)
#    define ASSERT_UINT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uintmax_t, "%llu", expected, got, __VA_ARGS__)
#else
/* For comparing any signed integer types */
#    define ASSERT_INT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(intmax_t, "%jd", expected, got, __VA_ARGS__)
/* For comparing any unsigned integer types */
#    define ASSERT_UINT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uintmax_t, "%ju", expected, got, __VA_ARGS__)
#endif

#define ASSERT_PTR_EQUALS(expected, got, ...)                                                                          \
    do {                                                                                                               \
        void *assert_expected = (void *)(uintptr_t)(expected);                                                         \
        void *assert_actual = (void *)(uintptr_t)(got);                                                                \
        if (assert_expected != assert_actual) {                                                                        \
            fprintf(AWS_TESTING_REPORT_FD, "%s%p != %p: ", FAIL_PREFIX, assert_expected, assert_actual);               \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s != %s", #expected, #got);                                                     \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

/* note that uint8_t is promoted to unsigned int in varargs, so %02x is an acceptable format string */
#define ASSERT_BYTE_HEX_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uint8_t, "%02X", expected, got, __VA_ARGS__)
#define ASSERT_HEX_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(unsigned long long, "%llX", expected, got, __VA_ARGS__)

#define ASSERT_STR_EQUALS(expected, got, ...)                                                                          \
    do {                                                                                                               \
        const char *assert_expected = (expected);                                                                      \
        const char *assert_got = (got);                                                                                \
        if (strcmp(assert_expected, assert_got) != 0) {                                                                \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD, "%sExpected: \"%s\"; got: \"%s\": ", FAIL_PREFIX, assert_expected, assert_got); \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("ASSERT_STR_EQUALS(%s, %s)", #expected, #got);                                    \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_BIN_ARRAYS_EQUALS(expected, expected_size, got, got_size, ...)                                          \
    do {                                                                                                               \
        const uint8_t *assert_ex_p = (const uint8_t *)(expected);                                                      \
        size_t assert_ex_s = (expected_size);                                                                          \
        const uint8_t *assert_got_p = (const uint8_t *)(got);                                                          \
        size_t assert_got_s = (got_size);                                                                              \
        if (assert_ex_s != assert_got_s) {                                                                             \
            fprintf(AWS_TESTING_REPORT_FD, "%sSize mismatch: %zu != %zu: ", FAIL_PREFIX, assert_ex_s, assert_got_s);   \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "ASSERT_BIN_ARRAYS_EQUALS(%s, %s, %s, %s)", #expected, #expected_size, #got, #got_size);           \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
        if (memcmp(assert_ex_p, assert_got_p, assert_got_s) != 0) {                                                    \
            fprintf(AWS_TESTING_REPORT_FD, "%sData mismatch: ", FAIL_PREFIX);                                          \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "ASSERT_BIN_ARRAYS_EQUALS(%s, %s, %s, %s)", #expected, #expected_size, #got, #got_size);           \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

typedef void(aws_test_before_fn)(struct aws_allocator *allocator, void *ctx);
typedef int(aws_test_run_fn)(struct aws_allocator *allocator, void *ctx);
typedef void(aws_test_after_fn)(struct aws_allocator *allocator, void *ctx);

struct aws_test_harness {
    aws_test_before_fn *on_before;
    aws_test_run_fn *run;
    aws_test_after_fn *on_after;
    struct aws_allocator *allocator;
    void *ctx;
    const char *test_name;
    int suppress_memcheck;
};

#ifdef _WIN32
/* If I meet the engineer that wrote the dbghelp.h file for the windows 8.1 SDK we're gonna have words! */
#    pragma warning(disable : 4091)
#    include <Windows.h>
#    include <dbghelp.h>

struct win_symbol_data {
    struct _SYMBOL_INFO sym_info;
    char symbol_name[1024];
};

typedef BOOL __stdcall SymInitialize_fn(_In_ HANDLE hProcess, _In_opt_ PCSTR UserSearchPath, _In_ BOOL fInvadeProcess);

typedef BOOL __stdcall SymFromAddr_fn(
    _In_ HANDLE hProcess,
    _In_ DWORD64 Address,
    _Out_opt_ PDWORD64 Displacement,
    _Inout_ PSYMBOL_INFO Symbol);

#    if defined(_WIN64)
typedef BOOL __stdcall SymGetLineFromAddr_fn(
    _In_ HANDLE hProcess,
    _In_ DWORD64 qwAddr,
    _Out_ PDWORD pdwDisplacement,
    _Out_ PIMAGEHLP_LINE64 Line64);
#        define SymGetLineFromAddrName "SymGetLineFromAddr64"
#    else
typedef BOOL __stdcall SymGetLineFromAddr_fn(
    _In_ HANDLE hProcess,
    _In_ DWORD dwAddr,
    _Out_ PDWORD pdwDisplacement,
    _Out_ PIMAGEHLP_LINE Line);
#        define SymGetLineFromAddrName "SymGetLineFromAddr"
#    endif

static LONG WINAPI s_test_print_stack_trace(struct _EXCEPTION_POINTERS *exception_pointers) {
    (void)exception_pointers;
    fprintf(stderr, "** Exception 0x%x occured **\n", exception_pointers->ExceptionRecord->ExceptionCode);
    void *stack[1024];

    HMODULE dbghelp = LoadLibraryA("DbgHelp.dll");
    if (!dbghelp) {
        fprintf(stderr, "Failed to load DbgHelp.dll.\n");
        goto done;
    }

    SymInitialize_fn *p_SymInitialize = (SymInitialize_fn *)GetProcAddress(dbghelp, "SymInitialize");
    if (!p_SymInitialize) {
        fprintf(stderr, "Failed to load SymInitialize from DbgHelp.dll.\n");
        goto done;
    }

    SymFromAddr_fn *p_SymFromAddr = (SymFromAddr_fn *)GetProcAddress(dbghelp, "SymFromAddr");
    if (!p_SymFromAddr) {
        fprintf(stderr, "Failed to load SymFromAddr from DbgHelp.dll.\n");
        goto done;
    }

    SymGetLineFromAddr_fn *p_SymGetLineFromAddr =
        (SymGetLineFromAddr_fn *)GetProcAddress(dbghelp, SymGetLineFromAddrName);
    if (!p_SymGetLineFromAddr) {
        fprintf(stderr, "Failed to load " SymGetLineFromAddrName " from DbgHelp.dll.\n");
        goto done;
    }

    HANDLE process = GetCurrentProcess();
    p_SymInitialize(process, NULL, TRUE);
    WORD num_frames = CaptureStackBackTrace(0, 1024, stack, NULL);
    DWORD64 displacement = 0;
    DWORD disp = 0;

    fprintf(stderr, "Stack Trace:\n");
    for (size_t i = 0; i < num_frames; ++i) {
        uintptr_t address = (uintptr_t)stack[i];
        struct win_symbol_data sym_info;
        AWS_ZERO_STRUCT(sym_info);
        sym_info.sym_info.MaxNameLen = sizeof(sym_info.symbol_name);
        sym_info.sym_info.SizeOfStruct = sizeof(struct _SYMBOL_INFO);
        p_SymFromAddr(process, address, &displacement, &sym_info.sym_info);

        IMAGEHLP_LINE line;
        line.SizeOfStruct = sizeof(IMAGEHLP_LINE);
        if (p_SymGetLineFromAddr(process, address, &disp, &line)) {
            if (i != 0) {
                fprintf(
                    stderr,
                    "at %s(%s:%lu): address: 0x%llX\n",
                    sym_info.sym_info.Name,
                    line.FileName,
                    line.LineNumber,
                    sym_info.sym_info.Address);
            }
        }
    }

done:
    if (dbghelp) {
        FreeLibrary(dbghelp);
    }
    return EXCEPTION_EXECUTE_HANDLER;
}
#else
#    include <execinfo.h>
#    include <signal.h>

static void s_print_stack_trace(int sig, siginfo_t *sig_info, void *user_data) {
    (void)sig_info;
    (void)user_data;

    void *array[100];
    char **strings;
    size_t i;

    fprintf(stderr, "** Signal Thrown %d**\n", sig);
    int size = backtrace(array, 10);
    strings = backtrace_symbols(array, size);
    fprintf(stderr, "Stack Trace:\n");

    for (i = 0; i < size; i++)
        fprintf(stderr, "%s\n", strings[i]);

    free(strings);
    exit(-1);
}
#endif

static inline int s_aws_run_test_case(struct aws_test_harness *harness) {
    assert(harness->run);

#ifdef _WIN32
    SetUnhandledExceptionFilter(s_test_print_stack_trace);
#else
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);

    sa.sa_flags = SA_NODEFER;
    sa.sa_sigaction = s_print_stack_trace;

    sigaction(SIGSEGV, &sa, NULL);
#endif

    if (harness->on_before) {
        harness->on_before(harness->allocator, harness->ctx);
    }

    int ret_val = harness->run(harness->allocator, harness->ctx);

    if (harness->on_after) {
        harness->on_after(harness->allocator, harness->ctx);
    }

    if (!ret_val) {
        if (!harness->suppress_memcheck) {
            struct memory_test_allocator *alloc_impl = (struct memory_test_allocator *)harness->allocator->impl;
            ASSERT_UINT_EQUALS(
                alloc_impl->allocated,
                alloc_impl->freed,
                "%s [ \033[31mFAILED\033[0m ]"
                "Memory Leak Detected %d bytes were allocated, "
                "but only %d were freed.",
                harness->test_name,
                alloc_impl->allocated,
                alloc_impl->freed);
        }

        RETURN_SUCCESS("%s [ \033[32mOK\033[0m ]", harness->test_name);
    }

    FAIL("%s [ \033[31mFAILED\033[0m ]", harness->test_name);
}

/* Enables terminal escape sequences for text coloring on Windows. */
/* https://docs.microsoft.com/en-us/windows/console/console-virtual-terminal-sequences */
#ifdef _WIN32

#    include <Windows.h>

#    ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#        define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#    endif

static inline int enable_vt_mode(void) {
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    if (hOut == INVALID_HANDLE_VALUE) {
        return AWS_OP_ERR;
    }

    DWORD dwMode = 0;
    if (!GetConsoleMode(hOut, &dwMode)) {
        return AWS_OP_ERR;
    }

    dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
    if (!SetConsoleMode(hOut, dwMode)) {
        return AWS_OP_ERR;
    }
    return AWS_OP_SUCCESS;
}

#else

static inline int enable_vt_mode(void) {
    return AWS_OP_ERR;
}

#endif

#define AWS_TEST_ALLOCATOR_INIT(name)                                                                                  \
    static struct memory_test_allocator name##_alloc_impl = {                                                          \
        0,                                                                                                             \
        0,                                                                                                             \
        AWS_MUTEX_INIT,                                                                                                \
    };                                                                                                                 \
    static struct aws_allocator name##_allocator = {                                                                   \
        s_mem_acquire_malloc,                                                                                          \
        s_mem_release_free,                                                                                            \
        NULL,                                                                                                          \
        &name##_alloc_impl,                                                                                            \
    };

#define AWS_TEST_CASE_SUPRESSION(name, fn, s)                                                                          \
    static int fn(struct aws_allocator *allocator, void *ctx);                                                         \
    AWS_TEST_ALLOCATOR_INIT(name)                                                                                      \
    static struct aws_test_harness name##_test = {                                                                     \
        NULL,                                                                                                          \
        fn,                                                                                                            \
        NULL,                                                                                                          \
        &name##_allocator,                                                                                             \
        NULL,                                                                                                          \
        #name,                                                                                                         \
        s,                                                                                                             \
    };                                                                                                                 \
    int name(int argc, char *argv[]) {                                                                                 \
        (void)argc, (void)argv;                                                                                        \
        return s_aws_run_test_case(&name##_test);                                                                      \
    }

#define AWS_TEST_CASE_FIXTURE_SUPPRESSION(name, b, fn, af, c, s)                                                       \
    static void b(struct aws_allocator *allocator, void *ctx);                                                         \
    static int fn(struct aws_allocator *allocator, void *ctx);                                                         \
    static void af(struct aws_allocator *allocator, void *ctx);                                                        \
    AWS_TEST_ALLOCATOR_INIT(name)                                                                                      \
    static struct aws_test_harness name##_test = {                                                                     \
        b,                                                                                                             \
        fn,                                                                                                            \
        af,                                                                                                            \
        &name##_allocator,                                                                                             \
        c,                                                                                                             \
        #name,                                                                                                         \
        s,                                                                                                             \
    };                                                                                                                 \
    int name(int argc, char *argv[]) {                                                                                 \
        (void)argc;                                                                                                    \
        (void)argv;                                                                                                    \
        return s_aws_run_test_case(&name##_test);                                                                      \
    }

#define AWS_TEST_CASE(name, fn) AWS_TEST_CASE_SUPRESSION(name, fn, 0)
#define AWS_TEST_CASE_FIXTURE(name, b, fn, af, c) AWS_TEST_CASE_FIXTURE_SUPPRESSION(name, b, fn, af, c, 0)

#endif /* AWS_TESTING_AWS_TEST_HARNESS_H */
