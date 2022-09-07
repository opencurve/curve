#include "curvefs_test.h"
#include "reqparse.h"

extern "C" {
#include "iceoryx_binding_c/listener.h"
#include "iceoryx_binding_c/client.h"
#include "iceoryx_binding_c/request_header.h"
#include "iceoryx_binding_c/response_header.h"
#include "iceoryx_binding_c/runtime.h"
#include "iceoryx_binding_c/wait_set.h"
};

extern iox_server_t _g_server;

int getReqSize(char** req) {
    char* pos = *req;
    int* pobjsize = (int* )(pos);
    pos += OBJ_HEADER_SIZE;
    *req = pos;

    return *pobjsize;
}

enum func_no getFuncNo(char ** req) {
    char* pos = *req;
    enum func_no no = (enum func_no) (*pos);
    pos += OBJ_FUNC_SIZE;
    *req = pos;

    return no;
}

char* getArgs_str(char** req) {
    char* pos = *req;

    uint8_t slen = (uint8_t) (*pos);
    pos += 1;
    char* spos = pos;
    pos += slen + 1;
    *req = pos;

    return spos;
}

int getArgs_int(char** req) {
    char* pos = *req;
    int* pint = (int* )pos;
    pos += OBJ_INT_SIZE;
    *req = pos;

    return *pint;
}

off_t getArgs_off(char** req) {
    char* pos = *req;
    off_t* plen = (off_t*) pos;
    pos += OBJ_OFF_SIZE;
    *req = pos;

    return *plen;
}

size_t getArgs_size (char** req) {
    char* pos = *req;
    size_t* psize = (size_t*) pos;
    pos += OBJ_SIZE_T_SIZE;
    *req = pos;

    return *psize;
}

int getFuncArgs(enum func_no fno, char** req) {
    int rv = 0;

    switch (fno) {
    case FUNC_NO_ACCESS:
    {
        char* __name = getArgs_str(req);
        int __type = getArgs_int(req);
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "the args is __name %s __type %d\n", __name, __type);

        break;
    }
    default:
        break;
    }

    return rv;
}

void hexdump (void* psrc, int len) {
    unsigned char* line;
    int i, thisline, offset;

    line = (unsigned char*) psrc;
    offset = 0;

    while (offset < len) {
        printf ("%04x ", offset);
        thisline = len - offset;

        if (thisline > 16) {
            thisline = 16;
        }

        for (i=0; i < thisline; i++) {
            printf ("%02x ", line[i]);
        }

        for (; i < 16; i++) {
            printf (" ");
        }

        for (i=0; i < thisline; i++) {
            printf ("%c", (line[i] >= 0x20 && line[i] < 0x7f)? line[i]: '.');
        }

        printf ("\n");
        offset += thisline;
        line += thisline;
    }

    return;
}

int convert_to_libsysio_access(char* req) {
    int rv = 0;
    const char* __name = getArgs_str(&req);
    int __type = getArgs_int(&req);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "convert_to_libsysio_access %s %d\n", __name, __type);
    rv = SYSIO_INTERFACE_NAME(access) (__name, __type);
    return rv;
}

int convert_to_libsysio_open(char* req) {
    int rv = 0;
    const char* __name = getArgs_str(&req);
    int __oflag = getArgs_int(&req);
    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_open name=%s flag=%x\n", __name, __oflag);
    mode_t mode = 0;
    if (__oflag & O_CREAT) {
        mode = getArgs_int(&req);
        CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_open name=%s flag=%x mode=%x\n", __name, __oflag, mode);
        rv = SYSIO_INTERFACE_NAME(open) (__name, __oflag, mode);
    } else {
        rv = SYSIO_INTERFACE_NAME(open) (__name, __oflag);
    }

    return rv;
}

int convert_to_libsysio_ftruncate(char* req) {
    int rv = 0;
    int __fd = getArgs_int(&req);
    __off_t __length = getArgs_off(&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_ftruncate fd=%d length=%ld\n", __fd, __length);
    rv = SYSIO_INTERFACE_NAME(ftruncate) (__fd, __length);

    return rv;
}

int convert_to_libsysio_close (char* req) {
    int rv = 0;
    int __fd = getArgs_int(&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_close fd=%d\n", __fd);
    rv = SYSIO_INTERFACE_NAME(close) (__fd);

    return rv;
}

ssize_t convert_to_libsysio_pwrite (char* req) {
    ssize_t rv = 0;
    int __fd = getArgs_int (&req);
    size_t __n = getArgs_size (&req);
    off_t off = getArgs_off (&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_pwrite fd=%d size=%ld off=%ld\n", __fd, __n, off);
    rv = SYSIO_INTERFACE_NAME(pwrite) (__fd, req, __n, off);
    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_pwrite rv=%ld\n", rv);

    return rv;
}

int convert_to_libsysio_fsync (char* req) {
    int rv = 0;
    int __fd = getArgs_int (&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_fsync fd=%d\n", __fd);
    rv = SYSIO_INTERFACE_NAME(fsync) (__fd);

    return rv;
}

int convert_to_libsysio_stat (char* req) {
    int rv = 0;
    const char* __name = getArgs_str(&req);
    struct stat* __stat = (struct stat*) req;

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_stat name=%s\n", __name);
    rv = SYSIO_INTERFACE_NAME(stat) (__name, __stat);

    return rv;
}

ssize_t convert_to_libsysio_pread (char* req) {
    ssize_t rv = 0;

    int __fd = getArgs_int (&req);
    size_t __n = getArgs_size (&req);
    off_t off = getArgs_off (&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_pread fd=%d size=%ld off=%ld\n", __fd, __n, off);
    rv = SYSIO_INTERFACE_NAME(pread) (__fd, req, __n, off);

    return rv;
}

int convert_to_libsysio_unlink (char* req) {
    int rv = 0;
    const char* __name = getArgs_str(&req);

    CURVEFS_FUSE_PRINTF (FUSE_LOG_ERR, "convert_to_libsysio_unlink name=%s\n", __name);
    rv = SYSIO_INTERFACE_NAME(unlink) (__name);

    return rv;
}

int reply_libsysio_ret(char* req, int ret) {
    int rv = 0;
    int* response = NULL;
    enum iox_AllocationResult loanResult = iox_server_loan_response(_g_server, req, (void**)&response, sizeof(int));
    if (loanResult == AllocationResult_SUCCESS) {
        *response = ret;
        enum iox_ServerSendResult sendResult = iox_server_send(_g_server, response);
        if (sendResult != ServerSendResult_SUCCESS) {
            rv = -1;
            CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "reply_libsysio_ret  iox_server_send failed res=%d\n", sendResult);
        }
    } else {
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "reply_libsysio_ret  iox_server_loan_response failed res=%d\n", loanResult);
        rv = -1;
    }

    iox_server_release_request(_g_server, req);

    return rv;
}

int reply_libsysio_ssize (char* req, ssize_t res) {
    int rv = 0;
    ssize_t* response = NULL;

    enum iox_AllocationResult loanResult = iox_server_loan_response(_g_server, req, (void**)&response, sizeof(ssize_t));
    if (loanResult == AllocationResult_SUCCESS) {
        *response = res;
        enum iox_ServerSendResult sendResult = iox_server_send(_g_server, response);
        if (sendResult != ServerSendResult_SUCCESS) {
            rv = -1;
        }
    } else {
        rv = -1;
    }

    iox_server_release_request(_g_server, req);

    return rv;
}