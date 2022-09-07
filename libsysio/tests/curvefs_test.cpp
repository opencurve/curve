#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

#include <sys/time.h>
#include <sys/eventfd.h>

#include "atomic_queue.h"
#include "curvefs_test.h"

#include "fs_curve.h"
#include "fuse_i.h"

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif

bool keepRunning = true;

struct QueueMesgResp {
    void* _req;
    void* _resp;
    stCoRoutine_t* co;
};

typedef uint64_t Element;
const unsigned CAPACITY = 512;
const Element NIL = static_cast<Element>(-1);
using MpscQueue = atomic_queue::AtomicQueueB<Element, std::allocator<Element>, NIL>;

struct Thread_Message {
    MpscQueue* _mpsc_front;
    MpscQueue* _mpsc_back;
    int _efd;
    stCoCond_t* _cond;
};

struct Thread_Message* __g_vfsgate_msg;
struct Thread_Message* __g_fuse_msg;

struct fuse_session* __curvefs_se = NULL;
struct fuse_req* __curvefs_req = NULL;

pthread_key_t __curvefs_msg_key;

void send_curvefs_request(void* msg) {
    uint64_t count = 1;
    ssize_t rv;
    __g_fuse_msg->_mpsc_front->push((Element)msg);

    co_cond_signal(__g_fuse_msg->_cond);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "send_curvefs_request\n");

#if 0
    rv = write(__g_fuse_msg->_efd, &count, sizeof(count));
    if (rv <= 0) {
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "send_curvefs_request eventfd return false rv=%d errno =%d\n", rv, errno);
    }
#endif

    return;
}

void sigHandler(int signalValue) {
    (void)signalValue;
    keepRunning = false;

    return;
}

void sleep_for(uint32_t milliseconds) {
    usleep(milliseconds * 1000U);
}

void send_gateway_request(void* msg) {
    uint64_t count = 1;
    ssize_t rv;

    __g_vfsgate_msg->_mpsc_back->push((Element) msg);
    co_cond_signal(__g_vfsgate_msg->_cond);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "send_gateway_request\n");

#if 0
    rv = write(__g_vfsgate_msg->_efd, &count, sizeof(count));
    if (rv <= 0) {
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "send_gateway_request eventfd return false rv=%d err=%d\n", rv, errno);
    }
#endif

    return;
}

void processResponseLoop() {
    for(; false == __g_vfsgate_msg->_mpsc_back->was_empty();) {
        struct Fuse_Queue_Mesg* curResp = (struct Fuse_Queue_Mesg*)__g_vfsgate_msg->_mpsc_back->pop();
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "processResponseLoop co=%p, msg=%p\n", curResp->co, curResp);
        if (curResp->co) {
            co_resume(curResp->co);
        }
    }
    return;
}

//vfs_gate_event_loop
static void* vfs_gate_loop(void* args) {
    int rv = 0;
    ssize_t res;
    uint64_t value;
    co_enable_hook_sys();

    for (; true;) {
        //CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "vfs_gate_loop have some message msg=%p cond=%p\n", __g_vfsgate_msg, __g_vfsgate_msg->_cond);

        co_cond_timedwait(__g_vfsgate_msg->_cond, 500);

        //CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "vfs_gate_loop have some message\n");

        processResponseLoop();
    }

#if 0
    struct pollfd *pf = (struct pollfd*)calloc( 1,sizeof(struct pollfd) * 1 );
    pf[0].fd = __g_vfsgate_msg->_efd;
    pf[0].events = ( POLLIN | POLLERR | POLLHUP );

    for(; ;) {
        rv = poll(pf, 1, 500);
        if (0 == rv) {
            continue;
        }
        res = read(__g_vfsgate_msg->_efd, &value, sizeof(value));
        if (res <= 0 ) {
            CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "vfs_gate_loop read event res=%d err=%d\n", res, errno);
        }
        
        processResponseLoop();

    }
#endif

    return 0;
}

static int init_buffer(char* buf, size_t size, int ckpt) {
    size_t i;
    for(i=0; i < size; i++) {
        char c = 'a' + (char)((ckpt + i) & 32);
        buf[i] = c;
    }
    return 0;
}

static void curvefs_test() {

    int err;
	int size = 4096;
    long tot_size = 32 * 4096;
	int num = tot_size/size;
    int i;

	char *data = NULL;
	char *read_data = NULL;
	int fd = 0;
    const char *fpath = "test.txt";
    struct timeval start, end;
    double time;
    struct stat statbuf;

    printf("total data size: %d, transfer size: %ld\n", size, tot_size);
	data = (char *) malloc(sizeof(char) * size);
	init_buffer(data, size, 0);

	read_data = (char *) malloc(sizeof(char) * size);
	memset(read_data, 0, size);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "before mount fd=%d\n", ((struct lo_data *) fuse_req_userdata(__curvefs_req))->root.fd);

#if 0
	err = SYSIO_INTERFACE_NAME(mount)("/", "/", "curvefs", 2, NULL);
	if (err) {
		fprintf(stderr, "mount curvefs failed\n");
		return;
	}

    fuse_log(FUSE_LOG_ERR, "mount successful");
#endif

    fd = SYSIO_INTERFACE_NAME(open)("test2.txt", O_CREAT|O_WRONLY|O_SYNC|O_TRUNC, 0644);
    SYSIO_INTERFACE_NAME(close)(fd);

    gettimeofday(&start, NULL);

	int rc;
	fd = SYSIO_INTERFACE_NAME(open)(fpath, O_CREAT|O_WRONLY|O_SYNC|O_TRUNC, 0644);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");
	int offset = 0;

#if 0
    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open  cost %lf seconds\n", fd, time);
  
    //printf("file %s has created, fd: %d\n", fpath, fd);
    
    gettimeofday(&start, NULL);
#endif

    err = SYSIO_INTERFACE_NAME(ftruncate)(fd, tot_size);
    if (err < 0) {
        fprintf(stderr, "ftrunate failed\n");
        return;
    } else {
        //fprintf(stderr, "truncated file to %u\n", size);
    }

#if 0
    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open truncate cost %lf seconds\n", fd, time);

    gettimeofday(&start, NULL);
#endif

    SYSIO_INTERFACE_NAME(close)(fd);

    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open truncate close cost %lf seconds\n", fd, time);

	fd = SYSIO_INTERFACE_NAME(open)(fpath, O_WRONLY);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");

	gettimeofday(&start, NULL);
    printf("curvefs come to pwrite\n");
	for(i = 0; i < num; i++) {
		rc = SYSIO_INTERFACE_NAME(pwrite)(fd, data, size, offset);
		offset += size;
	}

    printf("curvefs come to fsync\n");
	err = SYSIO_INTERFACE_NAME(fsync)(fd);
	gettimeofday(&end, NULL);	
    
    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs %d times write %d bytes %lf seconds\n", i, rc, time);
	
	SYSIO_INTERFACE_NAME(close)(fd);

    err = SYSIO_INTERFACE_NAME(stat)(fpath, &statbuf);
    if (err < 0) {
        fprintf(stderr, "stat %s error: %d\n", fpath, errno);
    }

    printf("stat file size: %zu\n", statbuf.st_size);

	fd = SYSIO_INTERFACE_NAME(open)(fpath, O_RDONLY);
	if (fd < 0)
		fprintf(stderr, "curvefs read open failed\n");	
	else printf("curvefs read open correct\n");

	
	offset = 0;
	gettimeofday(&start, NULL);
	for (i = 0; i < num; i++){
		err = SYSIO_INTERFACE_NAME(pread)(fd, read_data, size, offset);	
		offset += size;
	}
	gettimeofday(&end, NULL);	
	time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs %d times read %d bytes %lf seconds\n", i, err, time);

	
	SYSIO_INTERFACE_NAME(close)(fd);
    
#if 0
	SYSIO_INTERFACE_NAME(unlink)(fpath);
#endif

#if 0
	SYSIO_INTERFACE_NAME(umount)("curvefs:/b_fuse");
#endif

	if (read_data)
		free(read_data);
	if (data)
		free(data);
    
    return;

}

static void* vfs_cmd_test(void* args) {

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "vfs_cmd_test enter\n");

    curvefs_test();

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "vfs_cmd_test exit\n");
    return NULL;
}

extern void* fuse_process_request(void* arg);

void fuse_request_loop() {
    for(; false == __g_fuse_msg->_mpsc_front->was_empty();) {
        struct Fuse_Queue_Mesg* curReq = (struct Fuse_Queue_Mesg *) __g_fuse_msg->_mpsc_front->pop();

        stCoRoutine_t *co_inst = NULL;
        co_create(&co_inst, NULL, fuse_process_request, curReq);
        co_resume(co_inst);
    }
    return;
}

static void* fuse_curvefs_loop(void* args) {
    int rv = 0;
    uint64_t value;
    ssize_t res;

    co_enable_hook_sys();

    for (; true;) {
        //CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_curvefs_loop have some message msg=%p cond=%p\n", __g_fuse_msg, __g_fuse_msg->_cond);
        
        co_cond_timedwait(__g_fuse_msg->_cond, 500);

        //CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_curvefs_loop have some message\n");

        fuse_request_loop();
    }

#if 0
    struct pollfd *pf = (struct pollfd*)calloc( 1,sizeof(struct pollfd) * 1 );
    pf[0].fd = __g_fuse_msg->_efd;
    pf[0].events = ( POLLIN | POLLERR | POLLHUP );

    for(; ;) {
        rv = poll(pf, 1, 500);
        if (0 == rv) {
            continue;
        }
        res = read(__g_fuse_msg->_efd, &value, sizeof(value));
        if (res <= 0) {
            CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_curvefs_loop read event res=%d failed err=%d\n", res, errno);
        }
        
        fuse_request_loop();
    }
#endif

    return NULL;
}

static void* vfs_gate(void* args) {

    __g_vfsgate_msg = (struct Thread_Message*) malloc(sizeof(struct Thread_Message));
    __g_vfsgate_msg->_mpsc_front = NULL;
    __g_vfsgate_msg->_mpsc_back = new MpscQueue(CAPACITY);
    __g_vfsgate_msg->_efd = -1;
    __g_vfsgate_msg->_cond = co_cond_alloc();

    __g_fuse_msg = (struct Thread_Message*) malloc(sizeof(struct Thread_Message));
    __g_fuse_msg->_mpsc_front = new MpscQueue(CAPACITY);
    __g_fuse_msg->_mpsc_back = NULL;
    __g_fuse_msg->_efd = -1;
    __g_fuse_msg->_cond = co_cond_alloc();

    pthread_key_create(&__curvefs_msg_key, NULL);

    stCoRoutine_t *co_fuse_loop = NULL;
    co_create(&co_fuse_loop, NULL, fuse_curvefs_loop, NULL);
    co_resume(co_fuse_loop);

    stCoRoutine_t *co_loop = NULL;
    co_create(&co_loop, NULL, vfs_gate_loop, NULL);
    co_resume(co_loop);

    stCoRoutine_t *co_command = NULL;
    co_create(&co_command, NULL, vfs_cmd_test, NULL);
    co_resume(co_command);

    stCoEpoll_t * ev = co_get_epoll_ct(); //ct = current thread
    co_eventloop( ev, NULL, 0 );
    return 0;
}



#if 0
static inline struct fuse_session* get_curvefs_session() {
    return (struct fuse_session *) pthread_getspecific(__curvefs_se);
}

static inline struct fuse_req* get_curvefs_req() {
    return (struct fuse_req *) pthread_getspecific(__curvefs_req);
}
#endif

int curvefs_reply_err(fuse_req_t req, int err) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    curreq->_ret = -err;
    send_gateway_request(curreq);
    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs_reply_err out co=%p\n", curreq->co);
    return 0;
}

int curvefs_reply_entry(fuse_req_t req, const struct fuse_entry_param *e) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct fuse_entry_param* args_out = (struct fuse_entry_param*) curreq->_args_out;

    memcpy(args_out, e, sizeof(struct fuse_entry_param));
    curreq->_ret = 0;    

    send_gateway_request(curreq);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs_reply_entry msg=%p out=%p out ino=%ld mode=%d\n", curreq, curreq->_args_out, args_out->ino, args_out->attr.st_mode);

    return 0;
}

int curvefs_reply_open(fuse_req_t req, const struct fuse_file_info *fi) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct Args_Open_out* args_out = (struct Args_Open_out*) curreq->_args_out;
    curreq->_ret = 0;
    args_out->_fout.fh = fi->fh;
    args_out->_fout.open_flags = fi->flags;

    send_gateway_request(curreq);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs_reply_open out\n");

    return 0;
}

int curvefs_reply_attr(fuse_req_t req, const struct stat *attr, double attr_timeout) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct stat* args_out = (struct stat*) curreq->_args_out;
    curreq->_ret = 0;
    memcpy(args_out, attr, sizeof(struct stat));

    send_gateway_request(curreq);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs_reply_attr out\n");

    return 0;
}

int curvefs_reply_write(fuse_req_t req, size_t count) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct Args_Write_out* args_out = (struct Args_Write_out*) curreq->_args_out;
    curreq->_ret = 0;
    args_out->_size = count;

    send_gateway_request(curreq);

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs_reply_write out\n");

    return 0;
}

int curvefs_fill_reply_data(struct fuse_bufvec *bufv) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct Args_Read_out* args_out = (struct Args_Read_out*) curreq->_args_out;

    bufv->buf[0].mem = (void *) args_out->_buf;

    return 0;
}

int curvefs_reply_read(fuse_req_t req, size_t count) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct Args_Read_out* args_out = (struct Args_Read_out*) curreq->_args_out;
    curreq->_ret = 0;
    args_out->_size = count;

    send_gateway_request(curreq);
    return 0;
}

int curvefs_reply_data(fuse_req_t req, struct fuse_bufvec *bufv, enum fuse_buf_copy_flags flags) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) co_getspecific(__curvefs_msg_key);
    struct Args_Read_out* args_out = (struct Args_Read_out*) curreq->_args_out;
    curreq->_ret = 0;

    send_gateway_request(curreq);
    return 0;
}

int fuse_process_request_lookup(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Lookup_in* args_in = (struct Args_Lookup_in*) curreq->_args_in;
    lo_oper.lookup(__curvefs_req, args_in->_ino, args_in->_path);

    return rv;
}

int fuse_process_request_mknod(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;
    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Mknod_in* args_in = (struct Args_Mknod_in*) curreq->_args_in;
    lo_oper.mknod(__curvefs_req, args_in->_ino, args_in->_path, args_in->_mode, 0);

    return rv;
}

int fuse_process_request_open(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;
    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Open_in* args_in = (struct Args_Open_in*) curreq->_args_in;
    lo_oper.open(__curvefs_req, args_in->_ino, &args_in->_info);

    return rv;
}

int fuse_process_request_setattr(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Setattr_in* args_in = (struct Args_Setattr_in*) curreq->_args_in;
    lo_oper.setattr(__curvefs_req, args_in->_ino, args_in->_stat, args_in->_mask, args_in->_finfo);

    return rv;
}

int fuse_process_request_getattr(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Getattr_in* args_in = (struct Args_Getattr_in*) curreq->_args_in;
    lo_oper.getattr(__curvefs_req, args_in->_ino, args_in->_finfo);

    return rv;
}

int fuse_process_request_release(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Close_in* args_in = (struct Args_Close_in*) curreq->_args_in;
    lo_oper.release(__curvefs_req, args_in->_ino, args_in->_finfo);

    return rv;
}

int fuse_process_request_write(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Write_in* args_in = (struct Args_Write_in*) curreq->_args_in;
    if (lo_oper.write) {
        lo_oper.write(__curvefs_req, args_in->_ino, args_in->_buf, args_in->_size, args_in->_off, args_in->_finfo);
    } else {
        struct fuse_bufvec bufv = FUSE_BUFVEC_INIT(args_in->_size);
        bufv.buf[0].mem =(void *) args_in->_buf;
        lo_oper.write_buf(__curvefs_req, args_in->_ino, &bufv, args_in->_off, args_in->_finfo);
    }

    return rv;
}

int fuse_process_request_read(struct Fuse_Queue_Mesg* curreq) {
    int rv = 0;

    co_setspecific(__curvefs_msg_key, curreq);
    struct Args_Read_in* args_in = (struct Args_Read_in*) curreq->_args_in;
    lo_oper.read(__curvefs_req, args_in->_ino, args_in->_size, args_in->_off, args_in->_finfo);

    return rv;
}

void* fuse_process_request(void* arg) {
    struct Fuse_Queue_Mesg* curreq = (struct Fuse_Queue_Mesg*) arg;

    switch(curreq->_method) {
    case FUSE_LOWLEVEL_LOOKUP:
        fuse_process_request_lookup(curreq);
        break;
    case FUSE_LOWLEVEL_INIT:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_INIT\n");
        break;
    case FUSE_LOWLEVEL_DESTROY:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_DESTROY\n");
        break;
    case FUSE_LOWLEVEL_OPEN:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_OPEN\n");
        fuse_process_request_open(curreq);
        break;
    case FUSE_LOWLEVEL_MKNOD:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_MKNOD\n");
        fuse_process_request_mknod(curreq);
        break;
    case FUSE_LOWLEVEL_SETATTR:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_SETATTR\n");
        fuse_process_request_setattr(curreq);
        break;
    case FUSE_LOWLEVEL_GETXATTR:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_GETXATTR\n");
        fuse_process_request_getattr(curreq);
        break;
    case FUSE_LOWLEVEL_RELEASE:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_RELEASE\n");
        fuse_process_request_release(curreq);
        break;
    case FUSE_LOWLEVEL_WRITE:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_WRITE\n");
        fuse_process_request_write(curreq);
        break;
    case FUSE_LOWLEVEL_READ:
        CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request FUSE_LOWLEVEL_READ\n");
        fuse_process_request_read(curreq);
        break;
    default:
        break;
    }

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse_process_request exit\n");
    return NULL;
}

static void* fuse_curve_main(void* args) {

#if 0
    struct MountOption mOpts = {0};
    struct fuse_session *se;
    struct fuse_req* req;
#endif

    pthread_key_create(&__curvefs_msg_key, NULL);

    __g_fuse_msg = (struct Thread_Message*) malloc(sizeof(struct Thread_Message));
    __g_fuse_msg->_mpsc_front = new MpscQueue(CAPACITY);
    __g_fuse_msg->_mpsc_back = NULL;
    __g_fuse_msg->_efd = eventfd(0, EFD_NONBLOCK|EFD_CLOEXEC);

#if 0
    se = fuse_session_new(&args, &curve_ll_oper, sizeof(curve_ll_oper), &mOpts);
    pthread_key_create(&__curvefs_se, NULL);
    pthread_setspecific(__curvefs_se, se);

    req = fuse_ll_alloc_req(se);
    pthread_key_create(&__curvefs_req, NULL);
    pthread_setspecific(__curvefs_req, req);
#endif

    stCoRoutine_t *co_loop = 0;
    co_create(&co_loop, NULL, fuse_curvefs_loop, NULL);
    co_resume(co_loop);

    stCoEpoll_t * ev = co_get_epoll_ct(); //ct = current thread
    co_eventloop( ev, NULL, 0 );

    return 0;
}

static struct fuse_req *curvefs_ll_alloc_req(struct fuse_session *se) {
	struct fuse_req *req;

	req = (struct fuse_req *) calloc(1, sizeof(struct fuse_req));
	if (req == NULL) {
		CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "fuse: failed to allocate request\n");
	} else {
		req->se = se;
		req->ctr = 1;

        req->next = req;
        req->prev = req;

		pthread_mutex_init(&req->lock, NULL);
	}

	return req;
}

struct lo_data lo = { .debug = 0,
	                  .writeback = 0};

/* just for passthrough 
*/
static int curvefs_init(int argc, char *argv[]) {
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_session *se;
	struct fuse_cmdline_opts opts;
	int ret = -1;

	pthread_mutex_init(&lo.mutex, NULL);
	lo.root.next = lo.root.prev = &lo.root;
	lo.root.fd = -1;
	lo.cache = CACHE_NORMAL;

	if (fuse_parse_cmdline(&args, &opts) != 0)
		return 1;

	if (fuse_opt_parse(&args, &lo, lo_opts, NULL)== -1)
		return 1;

	lo.debug = opts.debug;
	lo.root.refcount = 2;
	if (lo.source) {
		struct stat stat;
		int res;

		res = lstat(lo.source, &stat);
		if (res == -1) {
			fuse_log(FUSE_LOG_ERR, "failed to stat source (\"%s\"): %m\n",
				 lo.source);
			exit(1);
		}
		if (!S_ISDIR(stat.st_mode)) {
			fuse_log(FUSE_LOG_ERR, "source is not a directory\n");
			exit(1);
		}

	} else {
		lo.source = "/";
	}
	if (!lo.timeout_set) {
		switch (lo.cache) {
		case CACHE_NEVER:
			lo.timeout = 0.0;
			break;

		case CACHE_NORMAL:
			lo.timeout = 1.0;
			break;

		case CACHE_ALWAYS:
			lo.timeout = 86400.0;
			break;
		}
	} else if (lo.timeout < 0) {
		fuse_log(FUSE_LOG_ERR, "timeout is negative (%lf)\n",
			 lo.timeout);
		exit(1);
	}

	lo.root.fd = open(lo.source, O_PATH);
	if (lo.root.fd == -1) {
		fuse_log(FUSE_LOG_ERR, "open(\"%s\", O_PATH): %m\n",
			 lo.source);
		exit(1);
	}

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "mount source %s fd=%d\n", lo.source, lo.root.fd);
#if 0
    char sysio_cmd[256];

    snprintf(sysio_cmd, 256, "{mnt, dev=\"curvefs:%s\", dir=%s, fl=2}", lo.source, opts.mountpoint);
#endif
    //init the sysio module
    _test_curvefs_sysio_startup();

	se = fuse_session_new(&args, &lo_oper, sizeof(lo_oper), &lo);
	if (se == NULL)
	    goto err_out1;

    __curvefs_se = se;
    __curvefs_req = curvefs_ll_alloc_req(se);

	if (curvefs_session_mount(se, opts.mountpoint) != 0)
	    goto err_out3;

    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "curvefs mount successful se=%p req=%p root fd=%d\n", __curvefs_se, __curvefs_req, ((struct lo_data *) fuse_req_userdata(__curvefs_req))->root.fd);
	return 0;

err_out3:
	fuse_remove_signal_handlers(se);
	fuse_session_destroy(se);
err_out1:
	free(opts.mountpoint);
	fuse_opt_free_args(&args);

	if (lo.root.fd >= 0)
		close(lo.root.fd);

	return ret ? 1 : 0;
}

int main(int argc, char** argv) {


    curvefs_init(argc, argv);


#if 0
    pthread_attr_t custom_attr_fifo;
    int fifo_max_prio;
    struct sched_param fifo_param;

    pthread_attr_init(&custom_attr_fifo);
    pthread_attr_setschedpolicy(&custom_attr_fifo, SCHED_FIFO);
    fifo_max_prio = sched_get_priority_max(SCHED_FIFO);
    fifo_param.sched_priority = fifo_max_prio/2;
    pthread_attr_setschedparam(&custom_attr_fifo, &fifo_param);
    printf("now set the schedule to SCHED_FIFO and the priority=%d\n", fifo_param.sched_priority);

#endif

    pthread_t vfsGate_id;    
    pthread_create(&vfsGate_id, NULL, vfs_gate, NULL);

#if 0
    pthread_t fusefs_id;
    pthread_create(&fusefs_id, NULL, fuse_curve_main, NULL);
#endif

    while (keepRunning) {
        const uint32_t SLEEP_TIME_IN_MS = 500U;
        sleep_for(SLEEP_TIME_IN_MS);
    }

    _test_curvefs_sysio_shutdown();

    return 0;
}
