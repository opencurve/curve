#include <stdio.h>
#include <gflags/gflags.h>

DEFINE_int32(max_client_works, 16, "the max client works supported");
DEFINE_string(app_name, "curvefs_client", "the appname");
DEFINE_int32(debug, 0, "LDP_ENV_DEBUG");
DEFINE_int32(sh_sid_bit, 10, "session handle LDP_ENV_SID_BIT");

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

int main(int argc1, char** argv1) {
    //char* argv[8];
    char ** argv = (char **) malloc(8 * sizeof(char *));
    int argc = 0;
    char strfile[256];
    char cuf[256];
    //const char* filename = NULL;
    const char* filename = "sample";
    snprintf(cuf, 256, "%s", "./curvefs");
    argv[0] = cuf;

    //char **fake_argv = new char*[argc+1]{arg0};

    if (NULL == filename) {
        argc = 1;
        argv[1] = NULL;
    } else {
        snprintf(strfile, 256, "--flagfile=%s", filename);
        argv[1] = strfile;
        argc = 2;
        argv[2] = NULL;
    }

    printf("the argc %d the argv %s\n", argc, argv[0]);
    google::SetVersionString(get_version());
    google::SetUsageMessage(get_help());
    google::ParseCommandLineFlags(&argc, (char ***) &argv, true);

    printf("the argc1 %d the argv1 %s\n", argc, argv[1]);
    printf("the max_client_works is %d \n", FLAGS_max_client_works);
    
    free(argv);

    return 0;
}
