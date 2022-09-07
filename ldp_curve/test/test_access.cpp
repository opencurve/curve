#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/time.h>


///home/dxiang/linuxtest/test
extern int errno;

int main() {

    struct timeval start, end;
    double time;

    int fd1 = access("sample1.txt", F_OK);

    gettimeofday(&start, NULL);
    int fd = access("sample.txt", F_OK);
    gettimeofday(&end, NULL);
    if (-1 == fd) {
        printf("Error Number: %d\n", errno);
        perror("Error Description:");
    } else {
        printf("No error\n");
    }

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
    printf("curvefs fd=%d access cost %lf seconds\n", fd, time);

    return 0;
}