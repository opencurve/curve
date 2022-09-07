#ifndef REQPARSE_H_INCLUDE

#define REQPARSE_H_INCLUDE

#define OBJ_HEADER_SIZE                 4
#define OBJ_FUNC_SIZE                   1
#define OBJ_INT_SIZE                    4
#define OBJ_STR_HEADER_SIZE             (1 + 1)
#define OBJ_OFF_SIZE                    sizeof(off_t)
#define OBJ_SIZE_T_SIZE                 sizeof(size_t)
#define OBJ_STAT_SIZE                   sizeof(struct stat)


enum func_no {
    FUNC_NO_NONE,
    FUNC_NO_ACCESS,
    FUNC_NO_OPEN,
    FUNC_NO_FTRUNCATE,
    FUNC_NO_CLOSE,
    FUNC_NO_PWRITE,
    FUNC_NO_FSYNC,
    FUNC_NO_STAT,
    FUNC_NO_PREAD,
    FUNC_NO_UNLINK,
};

extern int getReqSize(char** req);
extern enum func_no getFuncNo(char ** req);
extern char* getArgs_str(char** req);
extern int getArgs_int(char** req);
extern off_t getArgs_off(char** req);
extern int getFuncArgs(enum func_no fno, char** req);
extern void hexdump (void* psrc, int len);

extern int convert_to_libsysio_access (char* req);
extern int convert_to_libsysio_open (char* req);
extern int convert_to_libsysio_ftruncate (char* req);
extern int convert_to_libsysio_close (char* req);
extern ssize_t convert_to_libsysio_pwrite (char* req);
extern int convert_to_libsysio_fsync (char* req);
extern int convert_to_libsysio_stat (char* req);
extern ssize_t convert_to_libsysio_pread (char* req);
extern int convert_to_libsysio_unlink (char* req);

extern int reply_libsysio_ret (char* req, int ret);
extern int reply_libsysio_ssize (char* req, ssize_t res);

#endif