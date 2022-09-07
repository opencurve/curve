#include <stdio.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include "curvefs_dbg.h"

extern "C" {
#include "iceoryx_binding_c/listener.h"
#include "iceoryx_binding_c/client.h"
#include "iceoryx_binding_c/request_header.h"
#include "iceoryx_binding_c/response_header.h"
#include "iceoryx_binding_c/runtime.h"
#include "iceoryx_binding_c/wait_set.h"
};

bool keepRunning = true;

DEFINE_string(app_name, "curvefs_client", "the appname");

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

static void hexdump (void* psrc, int len) {
    unsigned char* line;
    int i, thisline, offset;

    line = (unsigned char*) psrc;
    offset = 0;

    while (offset < len) {
        LDBG1 (0, "%04x ", offset);
        thisline = len - offset;

        if (thisline > 16) {
            thisline = 16;
        }

        for (i=0; i < thisline; i++) {
            LDBG1 (0, "%02x ", line[i]);
        }

        for (; i < 16; i++) {
            LDBG1 (0, " ");
        }

        for (i=0; i < thisline; i++) {
            LDBG1 (0, "%c", (line[i] >= 0x20 && line[i] < 0x7f)? line[i]: '.');
        }

        LDBG1 (0, "\n");
        offset += thisline;
        line += thisline;
    }

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

#define OBJ_HEADER_SIZE 4
#define OBJ_FUNC_SIZE 1
#define OBJ_INT_SIZE 4
#define OBJ_STR_HEADER_SIZE 1

enum func_no {
    FUNC_NO_NONE,
    FUNC_NO_ACCESS,
};

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
    pos += slen;
    *req = pos;

    return spos;
}

int getArgs_int(char** req) {
    char* pos = *req;
    int* pint = (int* )pos;
    pos += OBJ_INT_SIZE;

    return *pint;
}

int getFuncArgs(enum func_no fno, char** req) {
    int rv = 0;

    switch (fno) {
    case FUNC_NO_ACCESS:
    {
        char* __name = getArgs_str(req);
        int __type = getArgs_int(req);
        LDBG(0, "the args is __name %s __type %d\n", __name, __type);

        break;
    }
    default:
        break;
    }

    return rv;
}

void parseRequest(char* req) {
    int reqsize = getReqSize(&req);
    enum func_no fno = getFuncNo(&req);

    LDBG(0, "the reqsize is %d the func_no is %d\n", reqsize, fno);

    getFuncArgs(fno, &req);

    return;
}

void onRequestReceived(iox_server_t server) {
    char* request = NULL;

    while (ServerRequestResult_SUCCESS == iox_server_take_request(server, (const void**)&request)) {
        LDBG(0, "%s Got Request\n", FLAGS_app_name.c_str());
        hexdump(request, 20);
        
        parseRequest(request);
        
        int* response = NULL;
        enum iox_AllocationResult loanResult = iox_server_loan_response(server, request, (void**)&response, sizeof(int));
        if (loanResult == AllocationResult_SUCCESS) {
            *response = 0;
            LDBG(0, "%s Send Response: %d\n", FLAGS_app_name.c_str(), *response);
            enum iox_ServerSendResult sendResult = iox_server_send(server, response);
            if (sendResult != ServerSendResult_SUCCESS) {
                LDBG(0, "Error sending Response! Error code: %d\n", sendResult);
            }
        } else {
            LDBG(0, "Could not allocate Response! Error code: %d\n", loanResult);
        }

        iox_server_release_request(server, request);
    }

    return;
}

int main(int argc, char** argv) {

    google::SetVersionString(get_version());
    google::SetUsageMessage(get_help());
    google::ParseCommandLineFlags(&argc, (char ***) &argv, true);

    iox_runtime_init(FLAGS_app_name.c_str());

    iox_server_storage_t serverStorage;
    iox_server_t server = iox_server_init(&serverStorage, "CurveFS", "curvefs_client", "request", NULL);

    iox_listener_storage_t listenerStorage;
    iox_listener_t listener = iox_listener_init(&listenerStorage);
    if (iox_listener_attach_server_event(listener, server, ServerEvent_REQUEST_RECEIVED, onRequestReceived) != ListenerResult_SUCCESS) {
        LDBG(0, "unable to attach server\n");
        _exit(-1);
    }

    while (keepRunning) {
        const uint32_t SLEEP_TIME_IN_MS = 500U;
        sleep_for(SLEEP_TIME_IN_MS);
    }

    iox_listener_detach_server_event(listener, server, ServerEvent_REQUEST_RECEIVED);
    iox_listener_deinit(listener);
    iox_server_deinit(server);

    return 0;
}
