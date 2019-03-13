/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include <sched.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node.h"
#include "proto/chunk.pb.h"
#include "proto/copyset.pb.h"
#include "src/chunkserver/cli.h"

DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_bool(verbose, false, "Output verbose information");
DEFINE_int32(file_size, 1024 * 1024, "File size in 4K sectors");
DEFINE_int32(chunk_size, 4 * 1024 * 1024, "Chunk size");
DEFINE_int32(copyset_num, 32, "Copyset number");
DEFINE_int32(request_size, 4096, "Size of each requst");
DEFINE_int32(iodepth, 32, "I/O count(*request_size) performed before stop");
DEFINE_int32(io_count,
             1024 * 1024,
             "I/O count(*request_size) performed before stop");
DEFINE_int32(io_time, 10, "Time(seconds) of I/O performed before stop");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(write_percentage, 100, "Percentage of fetch_add");
DEFINE_string(io_mode, "sync", "I/O Mode(sync/async)");
DEFINE_string(io_pattern, "randwrite", "I/O pattern against storage");
DEFINE_string(wait_mode, "efficiency", "I/O Mode(sync/async)");
DEFINE_string(conf, "", "Configuration file path");
DEFINE_string(raftconf,
              "172.17.0.2:8200:0,172.17.0.2:8201:0,172.17.0.2:8202:0",
              "Configuration file path");

using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::CopysetRequest;
using curve::chunkserver::CopysetResponse;
using curve::chunkserver::PeerId;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::CopysetID;
using curve::chunkserver::Configuration;
using curve::chunkserver::CHUNK_OP_TYPE;
using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::COPYSET_OP_STATUS;

enum IO_PATTERN {
    IO_PATTERN_READ = 0,
    IO_PATTERN_WRITE = 1,
    IO_PATTERN_RANDREAD = 2,
    IO_PATTERN_RANDWRITE = 3,
    IO_PATTERN_VERITY = 4,
    IO_PATTERN_MAX = 5,
};

enum WAIT_MODE {
    WAIT_MODE_EFFICIENCY = 0,
    WAIT_MODE_LOWLATENCY = 1,
    WAIT_MODE_MAX = 2,
};

struct CURVE_CACHELINE_ALIGNMENT IoContext;

struct CURVE_CACHELINE_ALIGNMENT CopysetInfo {
    LogicPoolID poolId;
    CopysetID copysetId;
    braft::Configuration conf;
    std::vector<butil::EndPoint *> ep;
    std::vector<brpc::Channel *> channel;
    std::vector<curve::chunkserver::CopysetService_Stub *> copyset_stubs;
    std::vector<curve::chunkserver::ChunkService_Stub *> chunk_stubs;
    int leader;
};

struct CURVE_CACHELINE_ALIGNMENT ThreadInfo {
    int64_t id;
    int64_t io_count;  // I/O served already
    int64_t io_time;  // nano seconds
    int64_t latency_all;
    int64_t iodepth;
    std::atomic<int64_t> ioinflight;  // I/O being served

    struct timespec start_time;
    bool async;
    enum IO_PATTERN io_pattern;
    enum WAIT_MODE wait_mode;
    pthread_mutex_t count_mutex;
    unsigned int offset_base;
    size_t req_size;
    size_t offset_range;
    size_t io_total;
    int64_t errors;  // in nano seconds
    int64_t run_duration;  // in nano seconds
};

struct CURVE_CACHELINE_ALIGNMENT IoContext {
    ThreadInfo *tinfo;
    CopysetInfo *copyset;
    ChunkRequest *req;
    ChunkResponse *resp;
    brpc::Controller *cntl;
    google::protobuf::Closure *done;
};

void thread_sleep_wait(ThreadInfo *tinfo);
void thread_busy_wait(ThreadInfo *tinfo);
void thread_sleep_notify(ThreadInfo *tinfo);
void thread_no_notify(ThreadInfo *tinfo);
void prepare_write_request(ThreadInfo *tinfo,
                           CHUNK_OP_TYPE *op,
                           size_t *offset,
                           size_t *len);
void prepare_randwrite_request(ThreadInfo *tinfo,
                               CHUNK_OP_TYPE *op,
                               size_t *offset,
                               size_t *len);

bool toStop = false;

int64_t file_size = 0;  // I/O address space by bytes
int64_t chunk_size = 0;
int64_t nr_chunks = 0;
int64_t nr_copysets = 0;
int64_t chunks_per_copyset = 0;
ThreadInfo thread_infos[8] = {0};

LogicPoolID poolId = 10000;
CopysetID copysetIdBase = 100;

braft::Configuration conf;
std::vector<braft::PeerId> peers;
std::vector<bthread_t> io_threads;
std::vector<butil::EndPoint *> server_addrs;
std::vector<brpc::Channel *> server_channels;
std::vector<curve::chunkserver::CopysetService_Stub *> copyset_stubs;
std::vector<curve::chunkserver::ChunkService_Stub *> chunk_stubs;
std::vector<CopysetInfo *> copysets;

bvar::LatencyRecorder g_latency_recorder("chunk_client");

void (*request_generator[IO_PATTERN_MAX])(ThreadInfo *,
                                          CHUNK_OP_TYPE *,
                                          size_t *,
                                          size_t *) = {
    NULL,
    prepare_write_request,
    NULL,
    prepare_randwrite_request,
};

void (*thread_wait[WAIT_MODE_MAX])(ThreadInfo *) = {
    thread_sleep_wait,
    thread_busy_wait,
};

void (*thread_notify[WAIT_MODE_MAX])(ThreadInfo *) = {
    thread_sleep_notify,
    thread_no_notify,
};

inline int64_t time_diff(const struct timespec &t0, const struct timespec &t1) {
    return (int64_t) ((t1).tv_sec - (t0).tv_sec) * 1000000000 + (t1).tv_nsec
        - (t0).tv_nsec;
}

void update_stats(ThreadInfo *tinfo, IoContext *ioCxt) {
    struct timespec now;

    clock_gettime(CLOCK_REALTIME, &now);
    tinfo->io_time = time_diff(tinfo->start_time, now);
    tinfo->io_count++;
    tinfo->latency_all += ioCxt->cntl->latency_us();
}

void destroy_io_context(IoContext *ioCxt) {
    delete ioCxt->req;
    delete ioCxt->resp;
    delete ioCxt->cntl;
    delete ioCxt;
}

int bind_cpu(int64_t cpu) {
    cpu_set_t cpumask;

    CPU_ZERO(&cpumask);
    CPU_SET(cpu, &cpumask);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpumask) != 0) {
        return -1;
    }

    return 0;
}

enum WAIT_MODE parse_wait_mode(std::string mode) {
    if (mode == "efficiency") {
        return WAIT_MODE_EFFICIENCY;
    } else if (mode == "lowlatency") {
        return WAIT_MODE_LOWLATENCY;
    } else {
        return WAIT_MODE_MAX;
    }
}

enum IO_PATTERN parse_io_pattern(std::string pattern) {
    if (pattern == "read") {
        return IO_PATTERN_READ;
    } else if (pattern == "write") {
        return IO_PATTERN_WRITE;
    } else if (pattern == "randread") {
        return IO_PATTERN_RANDREAD;
    } else if (pattern == "randwrite") {
        return IO_PATTERN_RANDWRITE;
    } else {
        return IO_PATTERN_MAX;
    }
}

void thread_sleep_wait(ThreadInfo *tinfo) {
    while (tinfo->ioinflight.load(std::memory_order_acquire)
        >= tinfo->iodepth) {
        // FIXME(wenyu): elimate hardcode
        // bthread_usleep(1);
        // pthread_yield();
    }
    ++tinfo->ioinflight;
    LOG_IF(INFO, FLAGS_verbose) << "Inflight I/O increased to: "
                                << tinfo->ioinflight.load(std::memory_order_acquire);   //NOLINT
}

void thread_busy_wait(ThreadInfo *tinfo) {
    while (tinfo->ioinflight.load(std::memory_order_acquire)
        >= tinfo->iodepth) {
        // FIXME(wenyu): elimate hardcode
        {}
    }
}

void thread_sleep_notify(ThreadInfo *tinfo) {
    --tinfo->ioinflight;
    LOG_IF(INFO, FLAGS_verbose) << "Inflight I/O decreased to: "
                                << tinfo->ioinflight.load(std::memory_order_acquire);   //NOLINT
}

void thread_no_notify(ThreadInfo *tinfo) {
    // Do nothing
}

void notify_for_queue(ThreadInfo *tinfo) {
    thread_notify[tinfo->wait_mode](tinfo);
}

void wait_for_queue(ThreadInfo *tinfo) {
    thread_wait[tinfo->wait_mode](tinfo);
}

bool status_retryable(CHUNK_OP_STATUS status) {
    return status == CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED;
}

void prepare_write_request(ThreadInfo *tinfo,
                           CHUNK_OP_TYPE *op,
                           size_t *offset,
                           size_t *len) {
    *op = CHUNK_OP_TYPE::CHUNK_OP_WRITE;
    *offset = tinfo->offset_base;
    *len = tinfo->req_size;

    tinfo->offset_base += tinfo->req_size % file_size;
}

void prepare_randwrite_request(ThreadInfo *tinfo,
                               CHUNK_OP_TYPE *op,
                               size_t *offset,
                               size_t *len) {
    *op = CHUNK_OP_TYPE::CHUNK_OP_WRITE;
    *offset =
        rand_r(&tinfo->offset_base) % tinfo->offset_range * tinfo->req_size;
    *len = tinfo->req_size;
}

int update_leader(CopysetInfo *copyset) {
    braft::PeerId peerId;
    butil::Status status = curve::chunkserver::GetLeader(copyset->poolId,
                                                         copyset->copysetId,
                                                         copyset->conf,
                                                         &peerId);
    if (status.ok()) {
        if (*copyset->ep[copyset->leader] == peerId.addr) {
            return copyset->leader;
        }
        copyset->leader = -1;
        for (unsigned int j = 0; j < copyset->conf.size(); j++) {
            if (*copyset->ep[j] == peerId.addr) {
                copyset->leader = j;
                break;
            }
        }
        if (copyset->leader == -1) {
            LOG(ERROR) << "Failed to enumerate leader of copyset<"
                       << copyset->poolId << ", " << copyset->copysetId << ">";
            return -1;
        }
    }

    return copyset->leader;
}

void chunk_io_complete(IoContext *ioCxt) {
    ThreadInfo *tinfo = ioCxt->tinfo;
    ChunkResponse *resp = ioCxt->resp;
    brpc::Controller *cntl = ioCxt->cntl;
    CopysetInfo *copyset = ioCxt->copyset;

    // std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    // std::unique_ptr<ChunkResponse> response_guard(response);

    LOG_IF(INFO, FLAGS_verbose) << "Response from <"
                                << copyset->poolId << ", " << copyset->copysetId
                                << ">:"
                                << copyset->leader << ":"
                                << ioCxt->req->chunkid() << ":"
                                << ioCxt->req->offset() << ":"
                                << ioCxt->req->size()
                                << ", infligth I/Os: "
                                << tinfo->ioinflight.load(std::memory_order_acquire)    //NOLINT
                                << ", resp data size: "
                                << cntl->response_attachment().to_string().size()   //NOLINT
                                << " cntl failed: " << cntl->Failed()
                                << " resp status: " << resp->status();

    if (cntl->Failed()) {
        LOG_IF(WARNING, FLAGS_verbose)
        << "Thread " << tinfo->id << " failed to complete "
        << tinfo->io_count << "th I/O request: " << cntl->ErrorText();
        tinfo->errors++;
        // TODO(wenyu): add backoff/retry logic
        bthread_usleep(FLAGS_timeout_ms * 1000L);
    } else if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS != resp->status()) {
        tinfo->errors++;
        LOG_IF(ERROR, FLAGS_verbose || !status_retryable(resp->status()))
        << "Failed I/O response from <"
        << copyset->poolId << ", " << copyset->copysetId << ">:"
        << copyset->leader << ":"
        << ioCxt->req->chunkid() << ":"
        << ioCxt->req->offset() << ":"
        << ioCxt->req->size()
        << ", status: " << resp->status()
        << ", infligth I/Os: "
        << tinfo->ioinflight.load(std::memory_order_acquire)
        << ", data size: " << cntl->response_attachment().to_string().size();
        if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == resp->status()) {
            // TODO(wenyu): add backoff/retry logic
            LOG_IF(ERROR, FLAGS_verbose)
            << "Leader redirected to: " << resp->redirect();
            if (update_leader(copyset) >= 0) {
            } else {
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
        }
    } else {
        // do nothing for success request
        update_stats(tinfo, ioCxt);
    }

    destroy_io_context(ioCxt);

    notify_for_queue(tinfo);
}

int init_thread_info(ThreadInfo *tinfo) {
    tinfo->errors = 0;
    tinfo->io_count = 0;
    tinfo->iodepth = FLAGS_iodepth;
    tinfo->ioinflight = 0;
    tinfo->io_time = 0;
    tinfo->latency_all = 0;

    tinfo->io_pattern = parse_io_pattern(FLAGS_io_pattern);
    tinfo->wait_mode = parse_wait_mode(FLAGS_wait_mode);
    if (!request_generator[tinfo->io_pattern]) {
        return -1;
    }
    if (FLAGS_io_mode == "sync") {
        tinfo->async = false;
    } else {
        tinfo->async = true;
    }
    if (FLAGS_io_pattern == "write") {
        tinfo->offset_base = 0;
    } else if (FLAGS_io_pattern == "randwrite") {
        tinfo->offset_base =
            time(NULL) ^ getpid() ^ pthread_self() ^ (tinfo->id < 8);
    }
    clock_gettime(CLOCK_REALTIME, &tinfo->start_time);
    tinfo->req_size = FLAGS_request_size;
    tinfo->offset_range = file_size / FLAGS_request_size;
    tinfo->io_total = FLAGS_io_count;
    tinfo->run_duration = 1000000000l * FLAGS_io_time;

    if (FLAGS_verbose) {
        LOG(INFO) << "Thread " << tinfo->id << " informations:"
                  << "wait mode: " << tinfo->wait_mode << ", "
                  << "I/O mode: " << tinfo->async << ", "
                  << "I/O pattern: " << tinfo->io_pattern << ", "
                  << "I/O iodepth: " << tinfo->iodepth << ", "
                  << "Request size: " << tinfo->req_size;
    }

    return 0;
}

int prepare_io_context(IoContext *ioCxt) {
    CopysetInfo *copyset;
    ThreadInfo *tinfo = ioCxt->tinfo;
    ChunkRequest *req = new ChunkRequest();
    CHUNK_OP_TYPE op;
    size_t offset, len;

    request_generator[tinfo->io_pattern](tinfo, &op, &offset, &len);
    copyset = copysets[offset / chunk_size % nr_copysets];

    ioCxt->req = req;
    ioCxt->resp = new ChunkResponse();
    ioCxt->cntl = new brpc::Controller();
    if ((!req) || (!ioCxt->resp) || (!ioCxt->cntl)) {
        return -1;
    }
    if (tinfo->async) {
        ioCxt->done = brpc::NewCallback(chunk_io_complete, ioCxt);
    } else {
        ioCxt->done = NULL;
    }
    ioCxt->cntl->set_timeout_ms(FLAGS_timeout_ms);
    // TODO(wenyu): to support more data pattern
    ioCxt->cntl->request_attachment().resize(tinfo->req_size, 'a');
    ioCxt->copyset = copyset;

    req->set_optype(op);
    req->set_offset(offset % chunk_size);
    req->set_size(tinfo->req_size);
    req->set_logicpoolid(copyset->poolId);
    req->set_copysetid(copyset->copysetId);
    req->set_chunkid(offset / chunk_size);

    return 0;
}

static void *async_io_client(void *arg) {
    ThreadInfo *tinfo = reinterpret_cast<ThreadInfo *>(arg);

    if (init_thread_info(tinfo) != 0) {
        LOG(ERROR)
        << "Failed to initialize thread infomation for thread " << tinfo->id;
        toStop = true;
    }

    if (bind_cpu(tinfo->id) != 0) {
        LOG(ERROR) << "Failed to bind thread to cpu " << tinfo->id;
        toStop = true;
    }

    while (!toStop) {
        IoContext *ioCxt = new IoContext();
        ioCxt->tinfo = tinfo;
        if (prepare_io_context(ioCxt) != 0) {
            LOG(ERROR) << "Failed to prepare I/O context";
            toStop = true;
            break;
        }

        wait_for_queue(tinfo);

        curve::chunkserver::ChunkService_Stub
            *stub = ioCxt->copyset->chunk_stubs[ioCxt->copyset->leader];
        // TODO(wenyu): to support more data pattern;
        stub->WriteChunk(ioCxt->cntl, ioCxt->req, ioCxt->resp, ioCxt->done);

        if (!tinfo->async) {
            chunk_io_complete(ioCxt);
        }
        if (tinfo->io_time >= tinfo->run_duration) {
            LOG_IF(INFO, FLAGS_verbose)
            << "Thread " << tinfo->id << "consumed time(us): "
            << tinfo->io_time / 1000;
            toStop = true;
        } else if (tinfo->io_count >= FLAGS_io_count) {
            toStop = true;
        } else if (brpc::IsAskedToQuit()) {
            toStop = true;
        }
    }

    LOG_IF(INFO, FLAGS_verbose) << "IO client thread " << tinfo->id << " exits";
    return NULL;
}

int create_io_threads(int nr_threads) {
    void *(*ioroutine)(void *) = async_io_client;

    io_threads.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread) {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            thread_infos[i].id = i;

            if (pthread_create(&io_threads[i],
                               NULL,
                               ioroutine,
                               &thread_infos[i]) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(&io_threads[i],
                                         NULL,
                                         ioroutine,
                                         &thread_infos[i]) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    return 0;
}

void destroy_io_threads(int thread_num) {
    for (int i = 0; i < thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(io_threads[i], NULL);
        } else {
            bthread_join(io_threads[i], NULL);
        }
    }
}

void threads_stats(int thread_num, ThreadInfo *total_info) {
    for (int i = 0; i < thread_num; ++i) {
        total_info->errors += thread_infos[i].errors;
        total_info->io_count += thread_infos[i].io_count;
        total_info->latency_all += thread_infos[i].latency_all;

        LOG(INFO) << "Thread " << thread_infos[i].id << " I/O stats: "
                  << "time(us): " << thread_infos[i].io_time / 1000 << ", "
                  << "count: " << thread_infos[i].io_count << ", "
                  << "depth: " << thread_infos[i].iodepth << ", "
                  << "iops: " << thread_infos[i].io_count * 1000000000
                      / thread_infos[i].io_time << ", "
                  << "bandwidth(KB/s): "
                  << thread_infos[i].io_count * FLAGS_request_size / 1024
                      * 1000000000 / thread_infos[i].io_time << ", "
                  << "avarage latency(us): "
                  << thread_infos[i].latency_all / thread_infos[i].io_count
                  << ", ";
    }
}

int init_channels() {
    if (conf.parse_from(FLAGS_raftconf) != 0) {
        LOG(ERROR) << "Fail to parse configuration";
        return -1;
    }

    conf.list_peers(&peers);
    LOG_IF(INFO, FLAGS_verbose) << "Server list: ";
    for (unsigned int i = 0; i < peers.size(); i++) {
        server_addrs.push_back(&peers[i].addr);
        LOG_IF(INFO, FLAGS_verbose) << peers[i].addr;

        brpc::Channel *channel = new brpc::Channel();
        if (channel->Init(peers[i].addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << peers[i].addr;
        }
        server_channels.push_back(channel);

        curve::chunkserver::CopysetService_Stub *copyset_stub =
            new curve::chunkserver::CopysetService_Stub(channel);
        if (!copyset_stub) {
            LOG(ERROR)
            << "Fail to init copyset service stub to " << peers[i].addr;
        }
        copyset_stubs.push_back(copyset_stub);

        curve::chunkserver::ChunkService_Stub
            *chunk_stub = new curve::chunkserver::ChunkService_Stub(channel);
        if (!chunk_stub) {
            LOG(ERROR)
            << "Fail to init chunk service stub to " << peers[i].addr;
        }
        chunk_stubs.push_back(chunk_stub);
    }

    return 0;
}

int create_copyset(CopysetInfo *info) {
    for (unsigned int i = 0; i < info->conf.size(); i++) {
        brpc::Controller cntl;
        CopysetRequest request;
        CopysetResponse response;

        cntl.set_timeout_ms(10);
        request.set_logicpoolid(info->poolId);
        request.set_copysetid(info->copysetId);

        /**
         * The 'conf' paramter of CreateCopysetNode() actually means 'peer',
         * so parse conf to peer before invoke
         * servicerequest.set_conf(FLAGS_raftconf);
         */
        braft::Configuration conf;
        if (conf.parse_from(FLAGS_raftconf) != 0) {
            LOG(ERROR)
            << "Failed to parse raft configuration from " << FLAGS_raftconf;
        }

        std::vector<braft::PeerId> peers;
        conf.list_peers(&peers);
        std::vector<braft::PeerId>::iterator it;
        for (it = peers.begin(); it != peers.end(); it++) {
            request.add_peerid(it->to_string());
        }

        info->copyset_stubs[i]->CreateCopysetNode(&cntl,
                                                  &request,
                                                  &response,
                                                  nullptr);
        if (COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS != response.status() &&
            COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST != response.status()) {
            LOG(ERROR) << "Failed to create copyset <"
                       << info->poolId << ", " << info->copysetId
                       << "> on peer " << i
                       << ", status: " << response.status();
            return -1;
        }
    }

    return 0;
}

int init_copysets() {
    file_size = (int64_t) FLAGS_file_size * 4096;
    chunk_size = FLAGS_chunk_size;
    nr_copysets = FLAGS_copyset_num;

    nr_chunks = file_size / chunk_size;
    chunks_per_copyset = nr_chunks / nr_copysets;

    if (init_channels() != 0) {
        LOG(ERROR) << "Fail to initialize channels";
        return -1;
    }

    for (int i = 0; i < nr_copysets; i++) {
        CopysetInfo *info = new CopysetInfo();
        info->poolId = poolId;
        info->copysetId = i + copysetIdBase;
        info->conf = conf;
        for (unsigned int j = 0; j < conf.size(); j++) {
            info->ep.push_back(server_addrs[j]);
            info->channel.push_back(server_channels[j]);
            info->copyset_stubs.push_back(copyset_stubs[j]);
            info->chunk_stubs.push_back(chunk_stubs[j]);
        }
        if (create_copyset(info) == 0) {
            // TODO(zhouwenyu)
        } else {
            LOG(ERROR) << "Failed to create copyset " << i;
            return -1;
        }

        copysets.push_back(info);
    }

    int retry;
    for (int i = 0; i < nr_copysets; i++) {
        retry = 20;

        getLeaderRetry:
        if (update_leader(copysets[i]) >= 0) {
            continue;
        } else if (retry > 0) {
            retry--;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            goto getLeaderRetry;
        } else {
            LOG(ERROR) << "Failed to get leader of copyset<"
                       << copysets[i]->poolId << ", " << copysets[i]->copysetId
                       << ">";
            return -1;
        }
    }

    return 0;
}

int fini_channels() {
    for (unsigned int i = 0; i < conf.size(); i++) {
        delete chunk_stubs[i];
        delete copyset_stubs[i];
        delete server_channels[i];
    }

    return 0;
}

int fini_copysets() {
    for (int i = 0; i < nr_copysets; i++) {
        delete copysets[i];
    }

    if (fini_channels() != 0) {
        LOG(ERROR) << "Fail to initialize channels";
    }

    return 0;
}

int check_arguments() {
    if (FLAGS_wait_mode != "efficiency" && FLAGS_wait_mode != "lowlatency") {
        LOG(ERROR) << "Fail to check argument wait_mode";
        return -1;
    }
    if (FLAGS_io_mode != "sync" && FLAGS_io_mode != "async") {
        LOG(ERROR) << "Fail to check argument io_mode";
        return -1;
    }
    if (FLAGS_io_pattern != "read" && FLAGS_io_pattern != "write" &&
        FLAGS_io_pattern != "randread" && FLAGS_io_pattern != "randwrite") {
        LOG(ERROR) << "Fail to check argument io_pattern";
        return -1;
    }
    if (FLAGS_chunk_size < FLAGS_request_size) {
        LOG(ERROR) << "Fail to check argument request_size";
        return -1;
    }
    if (FLAGS_thread_num > 8) {
        LOG(ERROR) << "Fail to check argument thread_num";
        return -1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (check_arguments() != 0) {
        LOG(ERROR) << "Fail to check arguments";
        return -1;
    }

    if (init_copysets() != 0) {
        LOG(ERROR) << "Fail to initialize copysets";
        return -1;
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_REALTIME, &t0);

    if (create_io_threads(FLAGS_thread_num) != 0) {
        LOG(ERROR) << "Fail to create I/O threads";
        return -1;
    }

    destroy_io_threads(FLAGS_thread_num);

    clock_gettime(CLOCK_REALTIME, &t1);

    ThreadInfo total_info = {0};
    total_info.io_time = time_diff(t0, t1);
    total_info.iodepth = FLAGS_iodepth;
    threads_stats(FLAGS_thread_num, &total_info);

    LOG(INFO) << "Summary " << " I/O stats: "
              << "time(us): " << total_info.io_time / 1000 << ", "
              << "count: " << total_info.io_count << ", "
              << "io mode: " << FLAGS_io_mode << ", "
              << "io pattern: " << FLAGS_io_pattern << ", "
              << "block size: " << FLAGS_request_size << ", "
              << "jobs: " << FLAGS_thread_num << ", "
              << "iodepth: " << total_info.iodepth << ", "
              << "iops: "
              << total_info.io_count * 1000000000 / total_info.io_time << ", "
              << "bandwidth(KB/s): "
              << total_info.io_count * FLAGS_request_size / 1024 * 1000000000
                  / total_info.io_time << ", "
              << "avarage latency(us): "
              << total_info.latency_all / total_info.io_count << ", "
              << "error count: " << total_info.errors;

    LOG_IF(INFO, FLAGS_verbose)
    << "Multiple copyset I/O test client is going to quit";

    fini_copysets();

    return 0;
}
