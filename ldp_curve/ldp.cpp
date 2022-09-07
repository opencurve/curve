#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>
#include <sys/time.h>
#include <fcntl.h>

#include <gflags/gflags.h>

extern "C" {
#include "iceoryx_binding_c/client.h"
#include "iceoryx_binding_c/request_header.h"
#include "iceoryx_binding_c/response_header.h"
#include "iceoryx_binding_c/runtime.h"
#include "iceoryx_binding_c/wait_set.h"
};

#include "ldp.h"
#include "ldp_curvefs_wrapper.h"

#define NUMBER_OF_NOTIFICATIONS 1

DEFINE_int32(max_client_works, 16, "the max client works supported");
DEFINE_string(app_name, "curvefs_client", "the appname");
DEFINE_int32(debug, LDP_DEBUG_INIT, "LDP_ENV_DEBUG");
DEFINE_int32(sh_sid_bit, 10, "session handle LDP_ENV_SID_BIT");

typedef struct work_client {
    iox_client_storage_t cli_storage;
    iox_client_t cli;
    iox_ws_storage_t cli_waitsetStorage;
    iox_ws_t cli_waitset;
    pthread_mutex_t worker_rpc_lock;
    int64_t send_requestid;
    int64_t recv_requestid;
    int64_t want_requestid;
} work_client_t;

typedef struct {
    int init;
    int iox_init;
    char app_name[LDP_APP_NAME_MAX];
    uint32_t vlsh_bit_val;
    uint32_t vlsh_bit_mask;
    uint32_t debug;
    work_client_t**  cli_pools;
    pthread_mutex_t cli_pools_lock;
    uint32_t cli_pools_allocs;
} ldp_main_t;

#if 0 

#define LDP_DEBUG ldp->debug

    //if (ldp->debug > _lvl) { 

#define LDBG(_lvl, _fmt, _args...) 					                    \
    if (ldp->debug > _lvl) {                                                                \
        int errno_saved = errno;						                \
        fprintf (stderr, "ldp<%d>: " _fmt "\n", getpid(), ##_args);	    \
        errno = errno_saved;						                    \
    }

#define LDBG1(_lvl, _fmt, _args...) 					                    \
    if (ldp->debug > _lvl) {                                                                \
        int errno_saved = errno;						                \
        fprintf (stderr, "" _fmt "", ##_args);	    \
        errno = errno_saved;						                    \
    }

#endif

static ldp_main_t ldp_main = {
    .init = 0,
    .iox_init = 0,
    .vlsh_bit_val = (1 << LDP_SID_BIT_MIN),
    .vlsh_bit_mask = (1 << LDP_SID_BIT_MIN) - 1,
    .debug = LDP_DEBUG_INIT,
    .cli_pools = NULL,
    .cli_pools_lock = PTHREAD_MUTEX_INITIALIZER,
    .cli_pools_allocs = 0,
};

static ldp_main_t *ldp = &ldp_main;
static ldp_main_t pre_ldp_main;
static ldp_main_t *pre_ldp = &pre_ldp_main;

static __thread uint64_t __vcl_worker_index = ~0;
static __thread uint64_t __prev_vcl_worker_index = ~0;

static inline void vcl_set_worker_index (uint64_t wrk_index) {
  __vcl_worker_index = wrk_index;
}

static inline uint64_t vcl_get_worker_index (void) {
  return __vcl_worker_index;
}

/*
 * RETURN:  0 on success or -1 on error.
 * */
static inline void
ldp_set_app_name (const char *app_name)
{
  snprintf (ldp->app_name, LDP_APP_NAME_MAX,
	    "ldp-%d-%s", getpid (), app_name);
}

static inline char *
ldp_get_app_name ()
{
  if (ldp->app_name[0] == '\0')
    ldp_set_app_name ("app");

  return ldp->app_name;
}

static int get_work_from_pool() {

    int i = 0;

    pthread_mutex_lock(&ldp->cli_pools_lock);
    if ((NULL == ldp->cli_pools) || (ldp->cli_pools_allocs >= FLAGS_max_client_works)){
        pthread_mutex_unlock(&ldp->cli_pools_lock);
        return -1;
    }

    for(i=0; i < FLAGS_max_client_works; i++) {
        if (NULL == ldp->cli_pools[i]) {
            ldp->cli_pools[i] = (work_client_t*)malloc(sizeof(work_client_t));
            if (NULL == ldp->cli_pools[i]) {
                pthread_mutex_unlock(&ldp->cli_pools_lock);
                errno = ENOMEM;
                return -1;
            }
            ldp->cli_pools_allocs++;
            break;
        }
    }
    pthread_mutex_unlock(&ldp->cli_pools_lock);

    return i;
}

static int free_work_to_pool(int index) {

    pthread_mutex_lock(&ldp->cli_pools_lock);
    if ((NULL == ldp->cli_pools) || (ldp->cli_pools_allocs < 1)) {
        pthread_mutex_unlock(&ldp->cli_pools_lock);
        return -1;
    }

    if (NULL == ldp->cli_pools[index]) {
        pthread_mutex_unlock(&ldp->cli_pools_lock);
        return -1;
    }

    free(ldp->cli_pools[index]);
    ldp->cli_pools[index] = NULL;
    ldp->cli_pools_allocs--;

    //no client_work left just free the pool
    if (0 == ldp->cli_pools_allocs) {
        free(ldp->cli_pools);
        ldp->cli_pools = NULL;
    }

    pthread_mutex_unlock(&ldp->cli_pools_lock);

    return 0;
}

int alloc_client_works() {

    pthread_mutex_lock(&ldp->cli_pools_lock);
    if (NULL == ldp->cli_pools) {
        ldp->cli_pools = (work_client_t**)malloc(sizeof(work_client_t *) * FLAGS_max_client_works);
        if (NULL == ldp->cli_pools) {
            pthread_mutex_unlock(&ldp->cli_pools_lock);
            return -1;
        }
        for(int i=0; i < FLAGS_max_client_works; i++) {
            ldp->cli_pools[i] = NULL;
        }
    }
    pthread_mutex_unlock(&ldp->cli_pools_lock);

    return 0;
}

void client_work_init(work_client_t* cli_wrk) {
    cli_wrk->cli = iox_client_init(&cli_wrk->cli_storage, "CurveFS", "curvefs_client", "request", NULL);
    LDBG(0, "client_work_init cli=%p\n", cli_wrk->cli);
    cli_wrk->cli_waitset = iox_ws_init(&cli_wrk->cli_waitsetStorage);
    LDBG(0, "client_work_init ws=%p\n", cli_wrk->cli_waitset);
    iox_ws_attach_client_state(cli_wrk->cli_waitset, cli_wrk->cli, ClientState_HAS_RESPONSE, 0U, NULL);
    cli_wrk->send_requestid = 0;
    cli_wrk->recv_requestid = 0;
    cli_wrk->want_requestid = 0;
    return;
}

static int client_worker_alloc_and_init() {
    int index = 0;

    /* This was initialized already */
    if (vcl_get_worker_index () != ~0)
        return 0;

    index = get_work_from_pool();
    
    vcl_set_worker_index(index);
    
    client_work_init(ldp->cli_pools[index]);

    return 0;
}

static void client_worker_free() {
    int index;

    index = vcl_get_worker_index();
    if (~0 == index) {
        return;
    }

    free_work_to_pool(index);
    vcl_set_worker_index(~0);

    return; 
}

static inline work_client_t* get_current_client_work() {
    int index =  vcl_get_worker_index();
    return ldp->cli_pools[index];
}

static std::string g_version;
static std::string g_help;

std::string& get_version() {
    g_version = "0.1";
    return g_version;
}

std::string& get_help() {
    g_help = "help";
    return g_help;
}

static void cld_read_conf(char* filename) {
    char **argv = NULL;
    int argc = 0;
    char strfile[256];

    argv = (char **)malloc(8 * sizeof(char *));
    argv[0] = (char *)"curvefs";
    if (NULL == filename) {
        argc = 1;
        argv[1] = NULL;
    } else {
        snprintf(strfile, 256, "--flagfile=%s", filename);
        argv[1] = strfile;
        argc = 2;
        argv[2] = NULL;
    }

    google::SetVersionString(get_version());
    google::SetUsageMessage(get_help());
    google::ParseCommandLineFlags(&argc, (char ***) &argv, true);

    ldp->debug = FLAGS_debug;

    if (FLAGS_sh_sid_bit < LDP_SID_BIT_MIN) {
        ldp->vlsh_bit_val = 1 << LDP_SID_BIT_MIN;
        ldp->vlsh_bit_mask = 1 << LDP_SID_BIT_MIN - 1;
    } else if (FLAGS_sh_sid_bit > LDP_SID_BIT_MAX) {
        ldp->vlsh_bit_val = 1 << LDP_SID_BIT_MAX;
        ldp->vlsh_bit_mask = 1 << LDP_SID_BIT_MAX - 1;
    } else {
        ldp->vlsh_bit_val = 1 << FLAGS_sh_sid_bit;
        ldp->vlsh_bit_mask = 1 << FLAGS_sh_sid_bit - 1;
    }

    /* Make sure there are enough bits in the fd set for vcl sessions */
    if (ldp->vlsh_bit_val > FD_SETSIZE / 2) {
        /* Only valid for select/pselect, so just WARNING and not exit */
	    LDBG (0, "WARNING: LDP vlsh bit value %d > FD_SETSIZE/2 %d, " "select/pselect not supported now!",
		        ldp->vlsh_bit_val, FD_SETSIZE / 2);

    }

    free(argv);
    return;
}

static void free_ldp_main(ldp_main_t* pre_ldp) {

    if (pre_ldp->init) {
        //just free the resources
        if (pre_ldp->cli_pools) {
            for(int i=0; i < FLAGS_max_client_works; i++) {
                if (NULL != pre_ldp->cli_pools[i]) {
                    work_client_t* cli_wrk = pre_ldp->cli_pools[i];
                    LDBG(0, "free_ldp_main deinit ws=%p\n", cli_wrk->cli_waitset);
                    iox_ws_deinit(cli_wrk->cli_waitset);
                    LDBG(0, "free_ldp_main DEINIT cli=%p\n", cli_wrk->cli);
                    iox_client_deinit(cli_wrk->cli);
                    free(pre_ldp->cli_pools[i]);
                    pre_ldp->cli_pools[i] = 0;
                }
            }
            free(pre_ldp->cli_pools);
            pre_ldp->cli_pools = NULL;
            pre_ldp->cli_pools_allocs = 0;
        }
        pre_ldp->init = 0;
    }

    return;
}

static void print_ldp_main(ldp_main_t* _p_ldp) {

    LDBG (0, "print_ldp_main ldp=%p init=%d\n", _p_ldp, _p_ldp->init);

    if (_p_ldp->init) {
        LDBG (0, "cli_pools=%p\n", _p_ldp->cli_pools);
        for(int i=0; i < FLAGS_max_client_works; i++) {
            if (NULL != _p_ldp->cli_pools[i]) {
                LDBG(0, "client=%d\n", i);
            }
        }
    }

    return;
}

//clean app thread, if the last one clean the who pool
static void curvefs_app_exit() {
    print_ldp_main(pre_ldp);
    print_ldp_main(ldp);

    free_ldp_main(pre_ldp);
    free_ldp_main(ldp);
    
    return;
}

static void curvefs_app_pre_fork (void) {
    LDBG(0, "curvefs_app_pre_fork\n");
    //backup ldp main
    memcpy(&pre_ldp_main, &ldp_main, sizeof(ldp_main));
    
    ldp->init = 0;
    ldp->iox_init = 0;
    ldp->cli_pools = NULL,
    ldp->cli_pools_allocs = 0;

    __prev_vcl_worker_index = vcl_get_worker_index();
    vcl_set_worker_index(~0);

    return;
}

static void curvefs_app_fork_parent_handler(void) {
    LDBG(0, "curvefs_app_fork_parent_handler\n");
    //restore ldp main
    memcpy(&ldp_main, &pre_ldp_main, sizeof(ldp_main));
    vcl_set_worker_index(__prev_vcl_worker_index);

    /* just init the prev ldp main */
    pre_ldp->init = 0;
    pre_ldp->iox_init = 0;
    pre_ldp->cli_pools = NULL;
    pre_ldp->cli_pools_allocs = 0;

    __prev_vcl_worker_index = ~0;
    return;
}

static void curvefs_app_fork_child_handler(void) {
    int rv;

    LDBG(0, "curvefs_app_fork_child_handler\n");
    //free the resource of pre_ldp
    free_ldp_main(pre_ldp);

    //rebuild ldp main
    ldp_set_app_name(FLAGS_app_name.c_str());
    
    rv = alloc_client_works(); 
    if (rv) {
        LDBG (0, "alloc_client_works failed");
        return;
    }
    
    LDBG(0, "curvefs_app_fork_child_handler iox_init=%d init=%d\n", ldp->iox_init, ldp->init);
    //init the shmem module
    if (0 == ldp->iox_init) {
        LDBG (0, "curvefs_app_fork_child_handler iox_runtime_init name=%s\n", ldp->app_name);
        iox_runtime_init(ldp->app_name);
        ldp->iox_init = 1;
    }

    //TODO set current work_client
    rv = client_worker_alloc_and_init();
    LDBG (0, "app_name '%s', my_client_index %ld ", ldp->app_name,
	    vcl_get_worker_index());

    atexit (curvefs_app_exit);

    return;
}

static int ldp_init (void) {
    int rv;

    //ASSERT (!ldp->init);

    ldp->init = 1;

    LDBG (0, " ldp_init ");

    //TODO configuration config
    char *env_var_str = getenv (LDP_ENV_CONFIGFILE);
    if (env_var_str) {
        uint32_t tmp;
        if (sscanf (env_var_str, "%u", &tmp) != 1) { 
	        LDBG (0, "LDP<%d>: WARNING: Invalid LDP debug level specified in the env var %s", 
                	getpid (),
		        env_var_str);
        } else {
            LDBG (0, "configured LDP configfile (%s) from env var "
		        LDP_ENV_CONFIGFILE "!", "null");
	}
    }
    cld_read_conf(env_var_str);
    ldp_set_app_name(FLAGS_app_name.c_str());

    LDBG (0, " cld_read_conf ");
    rv = alloc_client_works(); 
    if (rv) {
        LDBG (0, "alloc_client_works failed");
        return rv;
    }

    LDBG (0, " begin iox_init ");
    //init the shmem module
    if (0 == ldp->iox_init) {
        iox_runtime_init(ldp->app_name);
        ldp->iox_init = 1;
    }
    LDBG (0, " end iox_init ");

    //TODO set current work_client
    rv = client_worker_alloc_and_init();
    LDBG (0, "app_name '%s', my_client_index %ld ", ldp->app_name,
	    vcl_get_worker_index());

    pthread_atfork (curvefs_app_pre_fork, curvefs_app_fork_parent_handler,
		    curvefs_app_fork_child_handler);
    atexit (curvefs_app_exit);

    LDBG (0, "LDP initialization: done!");

    return 0;
}

#define ldp_init_check()                                                        \
    if (PREDICT_FALSE (!ldp->init)) {                                           \
        if ((errno = -ldp_init ()))                                             \
	    return -1;                                                            \
    }

static const char* path_skip_list[] = {
    "/proc/",
    "/dev/",
    "/tmp/",
};

static const int path_skip_list_size = sizeof(path_skip_list) / sizeof(char *);
/* check that the file name arg is not the proc, sys, dev,
   just the curvefs
*/
static inline bool check_if_curvefs_file(const char* __name) {
    bool orgin = true;

    for(int i=0; i < path_skip_list_size; i++) {
        int res = strncmp(__name, path_skip_list[i], strlen(path_skip_list[i]));
        //printf("check_if_curvefs_file name=%s list=%s i=%d res=%d list size=%ld\n", __name, path_skip_list[i], i, res, sizeof(path_skip_list));
        if (0 == res) {
            orgin = false;
            break;
        }
    }

    return orgin;
}

static inline bool check_if_curvefs_fd (int __fd) {
    
    if (__fd >= (FD_SETSIZE/2)) {
        return true;
    }
    
    return false;
}

/* 
  size of memory int, just 4 bytes;
  8bits for class
  class 1. func no SERIALIZE_FUNC;   1 byte
  class 2. byte SERIALIZE_BYTE;      1 byte
  class 3. string SERIALIZE_STR;    1 byte len, string
  class 4. int  SERIALIZE_INT;      4 byte
  
*/

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

#define OBJ_HEADER_SIZE         4
#define OBJ_FUNC_SIZE           1
#define OBJ_INT_SIZE            4
#define OBJ_STR_HEADER_SIZE     (1 + 1)
#define OBJ_OFF_SIZE            sizeof(off_t)
#define OBJ_SIZE_T_SIZE         sizeof(size_t)
#define OBJ_STAT_SIZE           sizeof(struct stat)

static inline void serialize_str(char** ptr, const char* __str, uint8_t len) {
    char* pos = *ptr;
    *pos = len;
    pos += OBJ_STR_HEADER_SIZE - 1;
    memcpy(pos, __str, len);
    pos += len;
    *pos = 0;
    pos += 1;
    *ptr = pos;

    return ;
}

static inline void serialize_func(char** ptr, enum func_no __no) {
    char* pos = *ptr;
    *pos = (char)__no;
    pos += OBJ_FUNC_SIZE;
    *ptr = pos;

    return;
}

static inline void serialize_objsize(char** ptr, int objsize) {
    int* ipos = (int *)*ptr;
    char* pos = *ptr;
    *ipos = objsize;
    pos += OBJ_HEADER_SIZE;
    *ptr = pos;

    return;
}

static inline void serialize_int(char** ptr, int __number) {
    int* ipos = (int*)(*ptr);
    char* pos = *ptr;
    *ipos = __number;
    pos += OBJ_INT_SIZE;
    *ptr = pos;

    return;
}

static inline void serialize_offt (char** ptr, off_t __number) {
    off_t* ipos = (off_t*) (*ptr);
    char* pos = *ptr;
    *ipos = __number;
    pos += OBJ_OFF_SIZE;
    *ptr = pos;

    return;
}

static inline void serialize_sizet (char** ptr, size_t __n) {
    size_t* ipos = (size_t*) (*ptr);
    char* pos = *ptr;
    *ipos = __n;
    pos += OBJ_SIZE_T_SIZE;
    *ptr = pos;

    return;
}

static inline void serialize_mem (char** ptr, char* mem, size_t __n) {
    char* pos = *ptr;
    memcpy(pos, mem, __n);
    pos += __n;
    *ptr = pos;

    return;
}

static void hexdump (void* psrc, int len) {
    unsigned char* line;
    int i, thisline, offset;

    line = (unsigned char*) psrc;
    offset = 0;

    while (offset < len) {
        LDBG (0, "%04x ", offset);
        thisline = len - offset;

        if (thisline > 16) {
            thisline = 16;
        }

        for (i=0; i < thisline; i++) {
            LDBG (0, "%02x ", line[i]);
        }

        for (; i < 16; i++) {
            LDBG (0, " ");
        }

        for (i=0; i < thisline; i++) {
            LDBG (0, "%c", (line[i] >= 0x20 && line[i] < 0x7f)? line[i]: '.');
        }

        LDBG (0, "\n");
        offset += thisline;
        line += thisline;
    }

    return;
}

static inline char* get_request(work_client_t* cli, int obj_size) {

    char* request = NULL;

    enum iox_AllocationResult loanResult = iox_client_loan_request(cli->cli, (void**)&request, obj_size);
    if (loanResult == AllocationResult_SUCCESS) {
        iox_request_header_t requestHeader = iox_request_header_from_payload(request);
        iox_request_header_set_sequence_id(requestHeader, cli->send_requestid);
    }

    return request;
}

static inline int send_request(work_client_t* cli, char* req) {

    enum iox_ClientSendResult sendResult = iox_client_send(cli->cli, req);
    if (sendResult != ClientSendResult_SUCCESS) {
        return -1;
    }

    return 0;
}

static inline ssize_t get_ssize_reply (work_client_t* cli) {
    ssize_t rv = -1;
    bool torecv = true;
    const ssize_t* response = NULL;
    iox_notification_info_t notificationArray[NUMBER_OF_NOTIFICATIONS];
    uint64_t missedNotifications = 0U;

    int64_t expect_id = cli->send_requestid;
    struct timespec timeout;
    
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    cli->send_requestid++;

#ifndef CURVEFS_CLIENT_POLL

    uint64_t numberOfNotifications = iox_ws_timed_wait(cli->cli_waitset, timeout, notificationArray, NUMBER_OF_NOTIFICATIONS, &missedNotifications); 
    for (uint64_t i = 0; i < numberOfNotifications; ++i) {
        if (iox_notification_info_does_originate_from_client(notificationArray[i], cli->cli)) {
            while (iox_client_take_response(cli->cli, (const void**)&response) == ChunkReceiveResult_SUCCESS) {
        	    iox_const_response_header_t responseHeader = iox_response_header_from_payload_const(response);
        	    int64_t receivedSequenceId = iox_response_header_get_sequence_id_const(responseHeader);
        	    LDBG (0, " get receivedSequenceId %ld expect_id %ld respose=%ld", receivedSequenceId, expect_id, *response);
        	    if (receivedSequenceId == expect_id) {
            	        rv = *response;
        	    } else {
            	        LDBG (0, "func failed err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
        	    }

        	    iox_client_release_response(cli->cli, response);
                torecv = false;
        	    break;
            }
        }
    }

#else

    for(; true == torecv ;) {
        while (iox_client_take_response(cli->cli, (const void**)&response) == ChunkReceiveResult_SUCCESS) {
        	iox_const_response_header_t responseHeader = iox_response_header_from_payload_const(response);
        	int64_t receivedSequenceId = iox_response_header_get_sequence_id_const(responseHeader);
        	LDBG (0, " get receivedSequenceId %ld expect_id %ld respose=%ld", receivedSequenceId, expect_id, *response);
        	if (receivedSequenceId == expect_id) {
            	rv = *response;
        	} else {
            	LDBG (0, "func failed err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
        	}

        	iox_client_release_response(cli->cli, response);
            torecv = false;
        	break;
        }
    }

#endif

    return rv;
}

static inline int get_int_reply(work_client_t* cli) {
    int rv = -1;
    bool torecv = true;
    const int* response = NULL;
    iox_notification_info_t notificationArray[NUMBER_OF_NOTIFICATIONS];
    uint64_t missedNotifications = 0U;

    int64_t expect_id = cli->send_requestid;

    LDBG (0, "expect_id %ld send_requestid %ld", expect_id, cli->send_requestid);

    struct timespec timeout;
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    
    cli->send_requestid++;

#ifndef CURVEFS_CLIENT_POLL

    LDBG (0, "get_int_reply errno=%d\n", errno);

    uint64_t numberOfNotifications = iox_ws_timed_wait(cli->cli_waitset, timeout, notificationArray, NUMBER_OF_NOTIFICATIONS, &missedNotifications); 

    LDBG (0, " in get_int_reply ");
    LDBG (0, "get_int_reply numberOfNotifications=%ld errno=%d\n", numberOfNotifications, errno);

    for (uint64_t i = 0; i < numberOfNotifications; ++i) {
        if (iox_notification_info_does_originate_from_client(notificationArray[i], cli->cli)) {
            while (iox_client_take_response(cli->cli, (const void**)&response) == ChunkReceiveResult_SUCCESS) {
                LDBG (0, "get_int_reply iox_client_take_response errno=%d response=%d\n", errno, *response);
        	    iox_const_response_header_t responseHeader = iox_response_header_from_payload_const(response);
        	    int64_t receivedSequenceId = iox_response_header_get_sequence_id_const(responseHeader);
        	    LDBG (0, " get receivedSequenceId %ld expect_id %ld respose=%d", receivedSequenceId, expect_id, *response);
        	    if (receivedSequenceId == expect_id) {
            	        rv = *response;
        	    } else {
            	        LDBG (0, "func failed err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
                        LDBG (0, "func failed err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
        	    }

        	    iox_client_release_response(cli->cli, response);
                torecv = false;
        	    break;
            }
        }
    }

#else

    for(; true == torecv ;) {
        while (iox_client_take_response(cli->cli, (const void**)&response) == ChunkReceiveResult_SUCCESS) {
        	iox_const_response_header_t responseHeader = iox_response_header_from_payload_const(response);
        	int64_t receivedSequenceId = iox_response_header_get_sequence_id_const(responseHeader);
        	LDBG (0, " get receivedSequenceId %ld expect_id %ld respose=%d", receivedSequenceId, expect_id, *response);
        	if (receivedSequenceId == expect_id) {
            	rv = *response;
        	} else {
            	LDBG (0, "func failed err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
        	}

        	iox_client_release_response(cli->cli, response);
            torecv = false;
        	break;
        }
    }

#endif

    return rv;
}

static inline int getAllocSize(int objsize) {

    int tmp = objsize - 1;

    tmp |= tmp >> 1;
    tmp |= tmp >> 2;
    tmp |= tmp >> 4;
    tmp |= tmp >> 8;
    tmp |= tmp >> 16;

    if (PREDICT_FALSE(tmp < 0)) {
        tmp = 1;
    } else {
        tmp = tmp + 1;
    }

    return tmp;
}

/* Test for access to NAME using the real UID and real GID. */
int access (const char * __name, int __type) {
    int rv;
    bool bv;

    struct timeval start, end;
    double time;

    gettimeofday(&start, NULL);

    bv = check_if_curvefs_file(__name);
    if (true == bv) {
        ldp_init_check();

        work_client_t* cli = get_current_client_work();

        LDBG (0, "func access send req app_name '%s', my_client_index %ld cli %p\n", ldp->app_name, vcl_get_worker_index(), cli);
        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_access(__name, __type);
        } else {
            int str_len = strlen(__name);
            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE + OBJ_STR_HEADER_SIZE + str_len;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            LDBG (0, " mem_obj %p p_obj %p alloc_size %d obj_size %d ", mem_obj, p_obj, alloc_size, obj_size);
            p_obj = mem_obj;

            //hexdump(mem_obj, obj_size);
            serialize_objsize(&p_obj, obj_size);
            //hexdump(mem_obj, obj_size);
            //LDBG (0, " mem_obj %p p_obj %p alloc_size %d obj_size %d ", mem_obj, p_obj, alloc_size, obj_size);

            serialize_func(&p_obj, FUNC_NO_ACCESS);
            serialize_str(&p_obj, __name, str_len);
            serialize_int(&p_obj, __type);

            LDBG (0, " mem_obj %p p_obj %p ", mem_obj, p_obj);
            LDBG (0, "func access send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
            //printf("access the request is %p\n", mem_obj);

            //hexdump(mem_obj, obj_size);
            
            if (send_request(cli, mem_obj) < 0) {
                LDBG (0, "func access err send req app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
                return -1;
            }
            
            LDBG (0, "func access get reply app_name '%s', my_client_index %ld ", ldp->app_name, vcl_get_worker_index());
            rv = get_int_reply(cli);

            gettimeofday(&end, NULL);
            time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
            //printf("client access cost %lf seconds\n", time);

        }

    } else {
        rv = libc_access(__name, __type);
    }

    return rv;
}

/* Turn accounting on if NAME is an existing file. The System will then write
   a record for each process as it terminates, to this file. If NAME is NULL, 
   turn accounting off. This call is restricted to the super-user.  */
int acct (const char *__name) {
    int rv;
    rv = libc_acct(__name);
    return rv;
}

/* Change the process's working directory to PATH.  */
int chdir (const char *__path) {
    int rv;
    rv = libc_chdir(__path);
    return rv;
}

/* Set file access perminssions for FILE to MODE.
   If FILE is a symbolic link, this affects its target instead.  */
int chmod (const char *__file, __mode_t __mode) {
    int rv;
    rv = libc_chmod(__file, __mode);
    return rv;
}

/* Change the owner and group of FILE.  */
int chown (const char *__file, __uid_t __owner, __gid_t __group) {
    int rv;
    rv = libc_chown(__file, __owner, __group);
    return rv;
}

/* Make PATH be the root directory (the starting point for absolute paths).
   This call is restricted to the super-user.  */
int chroot (const char *__path) {
    int rv;
    rv = libc_chroot(__path);
    return rv;
}

/* Close the file descriptor FD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int close (int __fd) {
    int rv = 0;
    bool bv = true;

    bv = check_if_curvefs_fd (__fd);
    if (true == bv) {
        ldp_init_check ();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_close(__fd);
        } else {
            __fd -= FD_SETSIZE / 2;

            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE;
            int alloc_size = getAllocSize (obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_CLOSE);
            serialize_int (&p_obj, __fd);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply(cli);
        }
    } else {
        rv = libc_close(__fd);
    }

    return rv;
}

/* Close all file descriptors in the range FD up to MAX_FD.  The flag FLAGS
   are define by the CLOSE_RANGE prefix.  This function behaves like close
   on the range and gaps where the file descriptor is invalid or errors
   encountered while closing file descriptors are ignored.   Returns 0 on
   successor or -1 for failure (and sets errno accordingly).  */
int close_range (unsigned int __fd, unsigned int __max_fd,
			int __flags) {
    int rv;
    rv = libc_close_range(__fd, __max_fd, __flags);
    return rv;
}

/* Copy LENGTH bytes from INFD to OUTFD.  */
ssize_t copy_file_range (int __infd, __off64_t *__pinoff,
			 int __outfd, __off64_t *__poutoff,
			 size_t __length, unsigned int __flags) {
    int rv;
    rv = libc_copy_file_range(__infd, __pinoff,
                            __outfd, __poutoff,
                            __length, __flags);
    return rv;
}

/* Create and open FILE, with mode MODE.  This takes an `int' MODE
   argument because that is what `mode_t' will be widened to.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int creat (const char *__file, mode_t __mode) {
    int rv;
    rv = libc_creat(__file, __mode);
    return rv;
}

int creat64 (const char *__file, mode_t __mode) {
    int rv;
    rv = libc_creat64(__file, __mode);
    return rv;
}

/* Duplicate FD, returning a new file descriptor on the same file.  */
int dup (int __fd) {
    int rv;
    rv = libc_dup(__fd);
    return rv;
}

/* Duplicate FD to FD2, closing FD2 and making it open on the same file.  */
int dup2 (int __fd, int __fd2) {
    int rv;
    rv = libc_dup2(__fd, __fd2);
    return rv;
}

/* Duplicate FD to FD2, closing FD2 and making it open on the same
   file while setting flags according to FLAGS.  */
int dup3 (int __fd, int __fd2, int __flags) {
    int rv;
    rv = libc_dup3(__fd, __fd2, __flags);
    return rv;
}

/* Test for access to FILE relative to the directory FD is open on.
   If AT_EACCESS is set in FLAG, then use effective IDs like `eaccess',
   otherwise use real IDs like `access'.  */
int faccessat (int __fd, const char *__file, int __type, int __flag) {
    int rv;
    rv = libc_faccessat(__fd, __file, __type, __flag);
    return rv;
}

int posix_fadvise64 (int __fd, off64_t __offset, off64_t __len,
			    int __advise) {
    int rv;
    rv = libc_posix_fadvise64(__fd, __offset, __len, __advise);
    return rv;
}

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int posix_fallocate (int __fd, off_t __offset, off_t __len) {
    int rv;
    rv = libc_posix_fallocate(__fd, __offset, __len);
    return rv;
}

int posix_fallocate64 (int __fd, off64_t __offset, off64_t __len) {
    int rv;
    rv = libc_posix_fallocate64(__fd, __offset, __len);
    return rv;
}

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int fallocate (int __fd, int __mode, __off_t __offset, __off_t __len) {
    int rv;
    rv = libc_fallocate(__fd, __mode, __offset, __len);
    return rv;
}

int fallocate64 (int __fd, int __mode, __off64_t __offset,
			__off64_t __len) {
    int rv;
    rv = libc_fallocate64(__fd, __mode, __offset, __len);
    return rv;
}

/* Change the process's working directory to the one FD is open on.  */
int fchdir (int __fd) {
    int rv;
    rv = libc_fchdir(__fd);
    return rv;
}

/* Set file access permissions of the file FD is open on to MODE.  */
int fchmod (int __fd, __mode_t __mode) {
    int rv;
    rv = libc_fchmod(__fd, __mode);
    return rv;
}

/* Set file access permissions of FILE relative to
   the directory FD is open on.  */
int fchmodat (int __fd, const char *__file, __mode_t __mode,
		     int __flag) {
    int rv;
    rv = libc_fchmodat(__fd, __file, __mode, __flag);
    return rv;
}

/* Change the owner and group of the file that FD is open on.  */
int fchown (int __fd, __uid_t __owner, __gid_t __group) {
    int rv;
    rv = libc_fchown(__fd, __owner, __group);
    return rv;
}

/* Change the owner and group of FILE relative to the directory FD is open
   on.  */
int fchownat (int __fd, const char *__file, __uid_t __owner,
		     __gid_t __group, int __flag) {
    int rv;
    rv = libc_fchownat(__fd, __file, __owner, __group, __flag);
    return rv;
}

/* Do the file control operation described by CMD on FD.
   The remaining arguments are interpreted depending on CMD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int fcntl (int __fd, int __cmd, ...) {
    int rv;

    va_list ap;
    va_start (ap, __cmd);
    rv = libc_fcntl(__fd, __cmd, ap);
    va_end (ap);

    return rv;
}

int fcntl64 (int __fd, int __cmd, ...) {
    int rv;

    va_list ap;
    va_start (ap, __cmd);
    rv = libc_fcntl64(__fd, __cmd, ap);
    va_end (ap);

    return rv;
}

/* Synchronize at least the data part of a file with the underlying
   media.  */
int fdatasync (int __fildes) {
    int rv;
    rv = libc_fdatasync(__fildes);
    return rv;
}

/* Get the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
ssize_t fgetxattr (int __fd, const char *__name, void *__value,
			  size_t __size) {
    ssize_t rv;
    rv = libc_fgetxattr(__fd, __name, __value, __size);
    return rv;
}

/* List attributes of the file descriptor FD into the user-supplied buffer
   LIST (which is SIZE bytes big).  Return 0 on success, -1 for errors.  */
ssize_t flistxattr (int __fd, char *__list, size_t __size) {
    ssize_t rv;
    rv = libc_flistxattr(__fd, __list, __size);
    return rv;
}

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
int flock (int __fd, int __operation) {
    int rv;
    rv = libc_flock(__fd, __operation);
    return rv;
}

/* Remove the attribute NAME from the file descriptor FD.  Return 0 on
   success, -1 for errors.  */
int fremovexattr (int __fd, const char *__name) {
    int rv;
    rv = libc_fremovexattr(__fd, __name);
    return rv;
}

/* Set the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
int fsetxattr (int __fd, const char *__name, const void *__value,
		      size_t __size, int __flags) {
    int rv;
    rv = libc_fsetxattr(__fd, __name, __value, __size, __flags);
    return rv;
}

/* Get file attributes for the file, device, pipe, or socket
   that file descriptor FD is open on and put them in BUF.  */
int fstat (int __fd, struct stat *__buf) {
    int rv;
    rv = libc_fstat(__fd, __buf);
    return rv;
}

int fstat64 (int __fd, struct stat64 *__buf) {
    int rv;
    rv = libc_fstat64(__fd, __buf);
    return rv;
}

int fstatat64 (int __fd, const char *__restrict __file,
		      struct stat64 *__restrict __buf, int __flag) {
    int rv;
    rv = libc_fstatat64(__fd, __file, __buf, __flag);
    return rv;
}

/* Return information about the filesystem containing the file FILDES
   refers to.  */
int fstatfs (int __fildes, struct statfs *__buf) {
    int rv;
    rv = libc_fstatfs(__fildes, __buf);
    return rv;
}

int fstatfs64 (int __fildes, struct statfs64 *__buf) {
    int rv;
    rv = libc_fstatfs64(__fildes, __buf);
    return rv;
}

/* Make all changes done to FD actually appear on disk.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int fsync (int __fd) {
    int rv = 0;
    bool bv = true;


    bv = check_if_curvefs_fd (__fd);
    if (true == bv) {
        ldp_init_check ();
    
        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_fsync(__fd);
        } else {
            __fd -= FD_SETSIZE / 2;

            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_FSYNC);
            serialize_int (&p_obj, __fd);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply(cli);
        }
    } else {
        rv = libc_fsync(__fd);
    }

    return rv;
}

/* Fill in TIMEBUF with information about the current time.  */

int ftime (struct timeb *__timebuf) {
    int rv;
    rv = libc_ftime(__timebuf);
    return rv;
}

/* Truncate the file FD is open on to LENGTH bytes.  */
int ftruncate (int __fd, __off_t __length) {
    int rv = 0;
    bool bv = true;

    bv = check_if_curvefs_fd (__fd);
    if (true == bv) {
        ldp_init_check ();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_ftruncate(__fd, __length);
        } else {
            __fd -= FD_SETSIZE / 2;

            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE + OBJ_INT_SIZE;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_FTRUNCATE);
            serialize_int (&p_obj, __fd);
            serialize_offt (&p_obj, __length);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply(cli);
        }
    } else {
        rv = libc_ftruncate(__fd, __length);
    }
    return rv;
}

int ftruncate64 (int __fd, __off64_t __length) {
    int rv;
    rv = libc_ftruncate64(__fd, __length);
    return rv;
}

/* Same as `utimes', but takes an open file descriptor instead of a name.  */
int futimes (int __fd, const struct timeval __tvp[2]) {
    int rv;
    rv = libc_futimes(__fd, __tvp);
    return rv;
}

/* Change the access time of FILE relative to FD to TVP[0] and the
   modification time of FILE to TVP[1].  If TVP is a null pointer, use
   the current time instead.  Returns 0 on success, -1 on errors.  */
int futimesat (int __fd, const char *__file,
		      const struct timeval __tvp[2]) {
    int rv;
    rv = libc_futimesat(__fd, __file, __tvp);
    return rv;
}


/* Get the pathname of the current working directory,
   and put it in SIZE bytes of BUF.  Returns NULL if the
   directory couldn't be determined or SIZE was too small.
   If successful, returns BUF.  In GNU, if BUF is NULL,
   an array is allocated with `malloc'; the array is SIZE
   bytes long, unless SIZE == 0, in which case it is as
   big as necessary.  */
char *getcwd (char *__buf, size_t __size) {
    char* rv;
    rv = libc_getcwd(__buf, __size);
    return rv;
}

/* Read from the directory descriptor FD into LENGTH bytes at BUFFER.
   Return the number of bytes read on success (0 for end of
   directory), and -1 for failure.  */
__ssize_t getdents64 (int __fd, void *__buffer, size_t __length) {
    __ssize_t rv;
    rv = libc_getdents64(__fd, __buffer, __length);
    return rv;
}

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long).  Return 0 on success, -1 for errors.  */
ssize_t getxattr (const char *__path, const char *__name,
			 void *__value, size_t __size) {
    ssize_t rv;
    rv = libc_getxattr(__path, __name, __value, __size);
    return rv;
}

/* Perform the I/O control operation specified by REQUEST on FD.
   One argument may follow; its presence and type depend on REQUEST.
   Return value depends on REQUEST.  Usually -1 indicates error.  */
int ioctl (int __fd, unsigned long int __request, ...) {
    int rv;
    va_list ap;

    va_start (ap, __request);
    rv = libc_ioctl(__fd, __request, ap);
    va_end (ap);

    return rv;
}

/* Change owner and group of FILE, if it is a symbolic
   link the ownership of the symbolic link is changed.  */
int lchown (const char *__file, __uid_t __owner, __gid_t __group) {
    int rv;
    rv = libc_lchown(__file, __owner, __group);
    return rv;
}

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
ssize_t lgetxattr (const char *__path, const char *__name,
			  void *__value, size_t __size) {
    ssize_t rv;
    rv = libc_lgetxattr(__path, __name, __value, __size);
    return rv;
}

/* Make a link to FROM named TO.  */
int link (const char *__from, const char *__to) {
    int rv;
    rv = libc_link(__from, __to);
    return rv;
}

/* Like link but relative paths in TO and FROM are interpreted relative
   to FROMFD and TOFD respectively.  */
int linkat (int __fromfd, const char *__from, int __tofd,
		   const char *__to, int __flags) {
    int rv;
    rv = libc_linkat(__fromfd, __from, __tofd, __to, __flags);
    return rv;
}

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big).  Return 0 on success, -1 for
   errors.  */
ssize_t listxattr (const char *__path, char *__list, size_t __size) {
    ssize_t rv;
    rv = libc_listxattr(__path, __list, __size);
    return rv;
}

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big), not following symlinks for the
   last pathname component.  Return 0 on success, -1 for errors.  */
ssize_t llistxattr (const char *__path, char *__list, size_t __size) {
    ssize_t rv;
    rv = libc_llistxattr(__path, __list, __size);
    return rv;
}

/* Remove the attribute NAME from the file pointed to by PATH, not
   following symlinks for the last pathname component.  Return 0 on
   success, -1 for errors.  */
int lremovexattr (const char *__path, const char *__name) {
    int rv;
    rv = libc_lremovexattr(__path, __name);
    return rv;
}

/* Move FD's file position to OFFSET bytes from the
   beginning of the file (if WHENCE is SEEK_SET),
   the current position (if WHENCE is SEEK_CUR),
   or the end of the file (if WHENCE is SEEK_END).
   Return the new file position.  */
__off_t lseek (int __fd, __off_t __offset, int __whence) {
    __off_t rv;
    rv = libc_lseek(__fd, __offset, __whence);
    return rv;
}

__off64_t lseek64 (int __fd, __off64_t __offset, int __whence) {
    __off64_t rv;
    rv = libc_lseek64(__fd, __offset, __whence);
    return rv;
}

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
int lsetxattr (const char *__path, const char *__name,
		      const void *__value, size_t __size, int __flags) {
    int rv;
    rv = libc_lsetxattr(__path, __name, __value, __size, __flags);
    return rv;
}

/* Get file attributes about FILE and put them in BUF.
   If FILE is a symbolic link, do not follow it.  */
int lstat (const char *__restrict __file,
		  struct stat *__restrict __buf) {
    int rv;
    rv = libc_lstat(__file, __buf);
    return rv;
}

int lstat64 (const char *__restrict __file,
		    struct stat64 *__restrict __buf) {
    int rv;
    rv = libc_lstat64(__file, __buf);
    return rv;
}

/* Advise the system about particular usage patterns the program follows
   for the region starting at ADDR and extending LEN bytes.  */
int madvise (void *__addr, size_t __len, int __advice) {
    int rv;
    rv = libc_madvise(__addr, __len, __advice);
    return rv;
}

/* This is the POSIX name for this function.  */
int posix_madvise (void *__addr, size_t __len, int __advice) {
    int rv;
    rv = libc_posix_madvise(__addr, __len, __advice);
    return rv;
}

/* Create a new directory named PATH, with permission bits MODE.  */
int mkdir (const char *__path, __mode_t __mode) {
    int rv;
    rv = libc_mkdir(__path, __mode);
    return rv;
}

/* Like mkdir, create a new directory with permission bits MODE.  But
   interpret relative PATH names relative to the directory associated
   with FD.  */
int mkdirat (int __fd, const char *__path, __mode_t __mode) {
    int rv;
    rv = libc_mkdirat(__fd, __path, __mode);
    return rv;
}

/* Create a device file named PATH, with permission and special bits MODE
   and device number DEV (which can be constructed from major and minor
   device numbers with the `makedev' macro above).  */
int mknod (const char *__path, __mode_t __mode, __dev_t __dev) {
    int rv;
    rv = libc_mknod(__path, __mode, __dev);
    return rv;
}

/* Like mknod, create a new device file with permission bits MODE and
   device number DEV.  But interpret relative PATH names relative to
   the directory associated with FD.  */
int mknodat (int __fd, const char *__path, __mode_t __mode,
		    __dev_t __dev) {
    int rv;
    rv = libc_mknodat(__fd, __path, __mode, __dev);
    return rv;
}


/* Map addresses starting near ADDR and extending for LEN bytes.  from
   OFFSET into the file FD describes according to PROT and FLAGS.  If ADDR
   is nonzero, it is the desired mapping address.  If the MAP_FIXED bit is
   set in FLAGS, the mapping will be at ADDR exactly (which must be
   page-aligned); otherwise the system chooses a convenient nearby address.
   The return value is the actual mapping address chosen or MAP_FAILED
   for errors (in which case `errno' is set).  A successful `mmap' call
   deallocates any previous mapping for the affected region.  */

void *mmap (void *__addr, size_t __len, int __prot,
		   int __flags, int __fd, __off_t __offset) {
    void* rv;
    rv = libc_mmap(__addr, __len, __prot, __flags, __fd, __offset);
    return rv;
}

void *mmap64 (void *__addr, size_t __len, int __prot,
		     int __flags, int __fd, __off64_t __offset) {
    void* rv;
    rv = libc_mmap64(__addr, __len, __prot, __flags, __fd, __offset);
    return rv;
}


/* Mount a filesystem.  */
int mount (const char *__special_file, const char *__dir,
		  const char *__fstype, unsigned long int __rwflag,
		  const void *__data) {

    return 0;
}

/* Remap pages mapped by the range [ADDR,ADDR+OLD_LEN) to new length
   NEW_LEN.  If MREMAP_MAYMOVE is set in FLAGS the returned address
   may differ from ADDR.  If MREMAP_FIXED is set in FLAGS the function
   takes another parameter which is a fixed address at which the block
   resides after a successful call.  */
void *mremap (void *__addr, size_t __old_len, size_t __new_len,
		     int __flags, ...) {
    void* rv;
    va_list ap;

    va_start (ap, __flags);
    rv = libc_mremap(__addr, __old_len, __new_len, __flags, ap);
    va_end (ap);

    return rv;
}

/* Synchronize the region starting at ADDR and extending LEN bytes with the
   file it maps.  Filesystem operations on a file being mapped are
   unpredictable before this is done.  Flags are from the MS_* set.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int msync (void *__addr, size_t __len, int __flags) {
    int rv;
    rv = libc_msync(__addr, __len, __flags);
    return rv;
}

/* Deallocate any mapping for the region starting at ADDR and extending LEN
   bytes.  Returns 0 if successful, -1 for errors (and sets errno).  */
int munmap (void *__addr, size_t __len) {
    int rv;
    rv = libc_munmap(__addr, __len);
    return rv;
}

/* Open FILE and return a new file descriptor for it, or -1 on error.
   OFLAG determines the type of access used.  If O_CREAT or O_TMPFILE is set
   in OFLAG, the third argument is taken as a `mode_t', the mode of the
   created file.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int open (const char *__file, int __oflag, ...) {
    int rv = 0;
    bool orgin = false;
    va_list ap;

    orgin = check_if_curvefs_file (__file);
    if (true == orgin) {
        ldp_init_check ();

        LDBG (0, "calling curvefs_open name=%s type=%x\n", __file, __oflag);

        work_client_t* cli = get_current_client_work();
        if (PREDICT_FALSE(NULL == cli)) {
            va_start (ap, __oflag);
            rv = libc_open(__file, __oflag, ap);
            va_end (ap);
        } else {
            mode_t mode = 0;
            int str_len = strlen (__file);
            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE + OBJ_INT_SIZE + OBJ_STR_HEADER_SIZE + str_len;
            int alloc_size = getAllocSize (obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;
            mem_obj = get_request (cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;
            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_OPEN);
            serialize_str (&p_obj, __file, str_len);
            serialize_int (&p_obj, __oflag);

            if (__oflag & O_CREAT) {
                va_start (ap, __oflag);
                mode = va_arg(ap, mode_t);
                va_end(ap);
            }
            serialize_int (&p_obj, mode);

            LDBG (0, "open file=%s len=%d flag=%x mode=%x\n", __file, str_len, __oflag, mode);

            if (send_request (cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply (cli);
            if (rv >= 0) {
                rv += FD_SETSIZE / 2;
            }
        }
    } else {
        LDBG (0, "calling libc_open filename %s", __file);

        va_start (ap, __oflag);
        rv = libc_open(__file, __oflag, ap);
        va_end (ap);
    }

    return rv;

}

int open64 (const char *__file, int __oflag, ...) {
    int rv;
    bool orgin = false;
    va_list ap;

    orgin = check_if_curvefs_file(__file);

    if (true == orgin) {
        ldp_init_check ();

        LDBG (0, "calling curvefs_open64\n");
    } else {
        LDBG (0, "calling libc_open64 filename %s\n", __file);

        va_start (ap, __oflag);
        rv = libc_open64(__file, __oflag, ap);
        va_end (ap);
    }

    return rv;
}

/* Similar to `open' but a relative path name is interpreted relative to
   the directory for which FD is a descriptor.

   NOTE: some other `openat' implementation support additional functionality
   through this interface, especially using the O_XATTR flag.  This is not
   yet supported here.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int openat (int __fd, const char *__file, int __oflag, ...) {
    int rv;
    va_list ap;

    va_start (ap, __oflag);
    rv = libc_openat(__fd, __file, __oflag, ap);
    va_end (ap);

    return rv;
}

int openat64 (int __fd, const char *__file, int __oflag, ...) {
    int rv;
    va_list ap;

    va_start (ap, __oflag);
    rv = libc_openat64(__fd, __file, __oflag, ap);
    va_end (ap);

    return rv;
}

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t pread (int __fd, void *__buf, size_t __nbytes,
		      __off_t __offset) {
    ssize_t rv = 0;
    bool bv = true;

    bv = check_if_curvefs_fd (__fd);
    if (true == bv) {
        ldp_init_check ();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_pread(__fd, __buf, __nbytes, __offset);
        } else {
            __fd -= FD_SETSIZE / 2;

            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE + OBJ_OFF_SIZE + OBJ_SIZE_T_SIZE + __nbytes;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_PREAD);
            serialize_int (&p_obj, __fd);
            serialize_sizet (&p_obj, __nbytes);
            serialize_offt (&p_obj, __offset);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_ssize_reply (cli);
            if (rv > 0) {
                memcpy((char*) __buf, p_obj, rv);
            }
        }
    } else {
        rv = libc_pread(__fd, __buf, __nbytes, __offset);
    }

    return rv;
}

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.  */
ssize_t pread64 (int __fd, void *__buf, size_t __nbytes,
			__off64_t __offset) {
    ssize_t rv;
    rv = libc_pread64(__fd, __buf, __nbytes, __offset);
    return rv;
}

/* Read data from file descriptor FD at the given position OFFSET
   without change the file pointer, and put the result in the buffers
   described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.  Operates just like
   'pread' (see <unistd.h>) except that data are put in IOVEC instead
   of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t preadv (int __fd, const struct iovec *__iovec, int __count,
		       __off_t __offset) {
    ssize_t rv;
    rv = libc_preadv(__fd, __iovec, __count, __offset);
    return rv;
}

/* Same as preadv but with an additional flag argumenti defined at uio.h.  */
ssize_t preadv2 (int __fp, const struct iovec *__iovec, int __count,
			__off_t __offset, int ___flags) {
    ssize_t rv;
    rv = libc_preadv2(__fp, __iovec, __count, __offset, ___flags);
    return rv;
}

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t pwrite (int __fd, const void *__buf, size_t __n,
		       __off_t __offset) {
    ssize_t rv = 0;
    bool bv = true;

    bv = check_if_curvefs_fd (__fd);
    if (true == bv) {
        ldp_init_check ();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_pwrite(__fd, __buf, __n, __offset);
        } else {
            __fd -= FD_SETSIZE / 2;

            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_INT_SIZE + OBJ_OFF_SIZE + OBJ_SIZE_T_SIZE + __n;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_PWRITE);
            serialize_int (&p_obj, __fd);
            serialize_sizet (&p_obj, __n);
            serialize_offt (&p_obj, __offset);
            serialize_mem (&p_obj, (char*) __buf, __n);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_ssize_reply(cli);
        }
    } else {
        rv = libc_pwrite(__fd, __buf, __n, __offset);
    }

    return rv;
}

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.  */
ssize_t pwrite64 (int __fd, const void *__buf, size_t __n,
			 __off64_t __offset) {
    ssize_t rv;
    rv = libc_pwrite64(__fd, __buf, __n, __offset);
    return rv;
}
    
/* Write data pointed by the buffers described by IOVEC, which is a
   vector of COUNT 'struct iovec's, to file descriptor FD at the given
   position OFFSET without change the file pointer.  The data is
   written in the order specified.  Operates just like 'pwrite' (see
   <unistd.h>) except that the data are taken from IOVEC instead of a
   contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t pwritev (int __fd, const struct iovec *__iovec, int __count,
			__off_t __offset) {
    ssize_t rv;
    rv = libc_pwritev(__fd, __iovec, __count, __offset);
    return rv;
}

/* Same as preadv but with an additional flag argument defined at uio.h.  */
ssize_t pwritev2 (int __fd, const struct iovec *__iodev, int __count,
			 __off_t __offset, int __flags) {
    ssize_t rv;
    rv = libc_pwritev2(__fd, __iodev, __count, __offset, __flags);
    return rv;
}

/* Read NBYTES into BUF from FD.  Return the
   number read, -1 for errors or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t read (int __fd, void *__buf, size_t __nbytes) {
    ssize_t rv;
    rv = libc_read(__fd, __buf, __nbytes);
    return rv;
}

/* Provide kernel hint to read ahead.  */
__ssize_t readahead (int __fd, __off64_t __offset, size_t __count) {
    __ssize_t rv;
    rv = libc_readahead(__fd, __offset, __count);
    return rv;
}

/* Read a directory entry from DIRP.  Return a pointer to a `struct
   dirent' describing the entry, or NULL for EOF or error.  The
   storage returned may be overwritten by a later readdir call on the
   same DIR stream.

   If the Large File Support API is selected we have to use the
   appropriate interface.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
struct dirent *readdir (DIR *__dirp) {
    struct dirent * rv;
    rv = libc_readdir(__dirp);
    return rv;
}

struct dirent64 *readdir64 (DIR *__dirp) {
    struct dirent64 * rv;
    rv = libc_readdir64(__dirp);
    return rv;

}

/* Read the contents of the symbolic link PATH into no more than
   LEN bytes of BUF.  The contents are not null-terminated.
   Returns the number of characters read, or -1 for errors.  */
ssize_t readlink (const char *__restrict __path,
			 char *__restrict __buf, size_t __len) {
    ssize_t rv;
    rv = libc_readlink(__path, __buf, __len);
    return rv;
}

/* Like readlink but a relative PATH is interpreted relative to FD.  */
ssize_t readlinkat (int __fd, const char *__restrict __path,
			   char *__restrict __buf, size_t __len) {
    ssize_t rv;
    rv = libc_readlinkat(__fd, __path, __buf, __len);
    return rv;
}

/* Read data from file descriptor FD, and put the result in the
   buffers described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.
   Operates just like 'read' (see <unistd.h>) except that data are
   put in IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t readv (int __fd, const struct iovec *__iovec, int __count) {
    ssize_t rv;
    rv = libc_readv(__fd, __iovec, __count);
    return rv;
}

/* Remap arbitrary pages of a shared backing store within an existing
   VMA.  */
int remap_file_pages (void *__start, size_t __size, int __prot,
			     size_t __pgoff, int __flags) {
    int rv;
    rv = libc_remap_file_pages(__start, __size, __prot, __pgoff, __flags);
    return rv;
}

/* Remove the attribute NAME from the file pointed to by PATH.  Return 0
   on success, -1 for errors.  */
int removexattr (const char *__path, const char *__name) {
    int rv;
    rv = libc_removexattr(__path, __name);
    return rv;
}

/* Rename file OLD to NEW.  */
int rename (const char *__old, const char *__new) {
    int rv;
    rv = libc_rename(__old, __new);
    return rv;
}

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD.  */
int renameat (int __oldfd, const char *__old, int __newfd,
		     const char *__new) {
    int rv;
    rv = libc_renameat(__oldfd, __old, __newfd, __new);
    return rv;
}

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD, with
   additional flags.  */
int renameat2 (int __oldfd, const char *__old, int __newfd,
		      const char *__new, unsigned int __flags) {
    int rv;
    rv = libc_renameat2(__oldfd, __old, __newfd, __new, __flags);
    return rv;
}

/* Remove the directory PATH.  */
int rmdir (const char *__path) {
    int rv;
    rv = libc_rmdir(__path);
    return rv;
}

/* Check the first NFDS descriptors each in READFDS (if not NULL) for read
   readiness, in WRITEFDS (if not NULL) for write readiness, and in EXCEPTFDS
   (if not NULL) for exceptional conditions.  If TIMEOUT is not NULL, time out
   after waiting the interval specified therein.  Returns the number of ready
   descriptors, or -1 for errors.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int select (int __nfds, fd_set *__restrict __readfds,
		   fd_set *__restrict __writefds,
		   fd_set *__restrict __exceptfds,
		   struct timeval *__restrict __timeout) {
    int rv;
    rv = libc_select(__nfds, __readfds, __writefds, __exceptfds, __timeout);
    return rv;
}

/* Send up to COUNT bytes from file associated with IN_FD starting at
   *OFFSET to descriptor OUT_FD.  Set *OFFSET to the IN_FD's file position
   following the read bytes.  If OFFSET is a null pointer, use the normal
   file position instead.  Return the number of written bytes, or -1 in
   case of error.  */
ssize_t sendfile (int __out_fd, int __in_fd, off_t *__offset,
			 size_t __count) {
    ssize_t rv;
    rv = libc_sendfile(__out_fd, __in_fd, __offset, __count);
    return rv;
}

ssize_t sendfile64 (int __out_fd, int __in_fd, __off64_t *__offset,
			   size_t __count) {
    ssize_t rv;
    rv = libc_sendfile64(__out_fd, __in_fd, __offset, __count);
    return rv;
}

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which
   is SIZE bytes long).  Return 0 on success, -1 for errors.  */
int setxattr (const char *__path, const char *__name,
		     const void *__value, size_t __size, int __flags) {
    int rv;
    rv = libc_setxattr(__path, __name, __value, __size, __flags);
    return rv;
}

int __xstat (int ver, const char* path, struct stat* stat_buf) {
    int rv = 0;

    printf ("__xstat the ver=%d path=%s\n", ver, path);
    rv = stat (path, stat_buf);

    return rv;
}

/* Get file attributes for FILE and put them in BUF.  */
int stat (const char*__restrict __file,
		 struct stat*__restrict __buf) {
    int rv = 0;
    bool bv = true;
    
    printf ("stat the file=%s\n", __file);

    bv = check_if_curvefs_file (__file);
    if (true == bv) {
        ldp_init_check();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_stat(__file, __buf);
        } else {
            int str_len = strlen(__file);
            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_STR_HEADER_SIZE + str_len + OBJ_STAT_SIZE;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;
            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_STAT);
            serialize_str (&p_obj, __file, str_len);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply (cli);
            if (0 == rv) {
                memcpy(__buf, p_obj, sizeof(struct stat));
            }
        }
    } else {
        rv = libc_stat(__file, __buf);
    }

    return rv;
}

int stat64 (const char *__restrict __file,
		   struct stat64 *__restrict __buf) {
    int rv;
    printf ("stat64 the file=%s\n", __file);
    rv = libc_stat64(__file, __buf);
    return rv;
}

int statfs (const char *__file, struct statfs *__buf) {
    int rv;
    printf ("statfs the file=%s\n", __file);
    rv = libc_statfs(__file, __buf);
    return rv;
}

int statfs64 (const char *__file, struct statfs64 *__buf) {
    int rv;
    printf ("statfs64 the file=%s\n", __file);
    rv = libc_statfs64(__file, __buf);
    return rv;
}

/* Fill *BUF with information about PATH in DIRFD.  */
int statx (int __dirfd, const char *__restrict __path, int __flags,
           unsigned int __mask, struct statx *__restrict __buf) {
    int rv;
    rv = libc_statx(__dirfd, __path, __flags, __mask, __buf);
    return rv;
}

/* Make a symbolic link to FROM named TO.  */
int symlink (const char *__from, const char *__to) {
    int rv;
    rv = libc_symlink(__from, __to);
    return rv;
}

/* Like symlink but a relative path in TO is interpreted relative to TOFD.  */
int symlinkat (const char *__from, int __tofd,
		      const char *__to) {
    int rv;
    rv = libc_symlinkat(__from, __tofd, __to);
    return rv;
}

/* Make all changes done to all files actually appear on disk.  */
void sync (void) {
    
    libc_sync();
    return;
}

/* Selective file content synch'ing.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int sync_file_range (int __fd, __off64_t __offset, __off64_t __count,
			    unsigned int __flags) {
    int rv;
    rv = libc_sync_file_range(__fd, __offset, __count, __flags);
    return rv;
}

/* Make all changes done to all files on the file system associated
   with FD actually appear on disk.  */
int syncfs (int __fd) {
    int rv;
    rv = libc_syncfs(__fd);
    return rv;
}

/* Truncate FILE to LENGTH bytes.  */
int truncate (const char *__file, __off_t __length) {
    int rv = 0;
    bool orgin = false;

    orgin = check_if_curvefs_file(__file);
    if (true == orgin) {
        ldp_init_check ();

        LDBG (0, "calling curvefs_truncate");
    } else {
        LDBG (0, "calling libc_truncate filename %s", __file);
        rv = libc_truncate(__file, __length);
    }

    return rv;
}

int truncate64 (const char *__file, __off64_t __length) {
    int rv;
    rv = libc_truncate64(__file, __length);
    return rv;
}

/* Unmount a filesystem.  */
int umount (const char *__special_file) {
    int rv;
    rv = libc_umount(__special_file);
    return rv;
}

/* Unmount a filesystem.  Force unmounting if FLAGS is set to MNT_FORCE.  */
int umount2 (const char *__special_file, int __flags) {
    int rv;
    rv = libc_umount2(__special_file, __flags);
    return rv;
}

/* Remove the link NAME.  */
int unlink (const char *__name) {
    int rv = 0;
    bool bv = true;

    bv = check_if_curvefs_file(__name);
    if (true == bv) {
        ldp_init_check();

        work_client_t* cli = get_current_client_work();

        if (PREDICT_FALSE(NULL == cli)) {
            rv = libc_unlink(__name);
        } else {
            int str_len = strlen (__name);
            int obj_size = OBJ_HEADER_SIZE + OBJ_FUNC_SIZE + OBJ_STR_HEADER_SIZE + str_len;
            int alloc_size = getAllocSize(obj_size);

            char* mem_obj = NULL;
            char* p_obj = NULL;

            mem_obj = get_request(cli, alloc_size);
            if (NULL == mem_obj) {
                errno = ENOMEM;
                return -1;
            }

            p_obj = mem_obj;

            serialize_objsize (&p_obj, obj_size);
            serialize_func (&p_obj, FUNC_NO_UNLINK);
            serialize_str(&p_obj, __name, str_len);

            if (send_request(cli, mem_obj) < 0) {
                return -1;
            }

            rv = get_int_reply(cli);
        }
    } else {
        rv = libc_unlink(__name);
    }

    return rv;
}

/* Remove the link NAME relative to FD.  */
int unlinkat (int __fd, const char *__name, int __flag) {
    int rv;
    rv = libc_unlinkat(__fd, __name, __flag);
    return rv;
}

/* Set the access and modification times of FILE to those given in
   *FILE_TIMES.  If FILE_TIMES is NULL, set them to the current time.  */
int utime (const char *__file,
		  const struct utimbuf *__file_times) {
    int rv;
    rv = libc_utime(__file, __file_times);
    return rv;
}


/* Set file access and modification times relative to directory file
   descriptor.  */
int utimensat (int __fd, const char *__path,
		      const struct timespec __times[2],
		      int __flags) {
    int rv;
    rv = libc_utimensat(__fd, __path, __times, __flags);
    return rv;
}

/* Change the access time of FILE to TVP[0] and the modification time of
   FILE to TVP[1].  If TVP is a null pointer, use the current time instead.
   Returns 0 on success, -1 on errors.  */
int utimes (const char *__file, const struct timeval __tvp[2]) {
    int rv;
    rv = libc_utimes(__file, __tvp);
    return rv;
}

/* Write N bytes of BUF to FD.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t write (int __fd, const void *__buf, size_t __n) {
    ssize_t rv;
    rv = libc_write(__fd, __buf, __n);
    return rv;
}

/* Write data pointed by the buffers described by IOVEC, which
   is a vector of COUNT 'struct iovec's, to file descriptor FD.
   The data is written in the order specified.
   Operates just like 'write' (see <unistd.h>) except that the data
   are taken from IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t writev (int __fd, const struct iovec *__iovec, int __count) {
    ssize_t rv;
    rv = libc_writev(__fd, __iovec, __count);
    return rv;
}
