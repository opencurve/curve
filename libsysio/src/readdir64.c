#ifdef _LARGEFILE64_SOURCE
#define _SCANDIR SYSIO_INTERFACE_NAME(scandir64)
#define _READDIR SYSIO_INTERFACE_NAME(readdir64)
#define _GETDIRENTRIES SYSIO_INTERFACE_NAME(getdirentries64)
#define _DIRENT_T struct dirent64
#define _OFF_T _SYSIO_OFF_T

#include "readdir.c"

#endif
