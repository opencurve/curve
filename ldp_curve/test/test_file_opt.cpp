#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/time.h>
#include <sys/stat.h>


#ifdef CURVEFS_TEST_DEBUG

#define CURVEFS_FUSE_PRINTF(level, fmt, ...) 	\
	do {									\
		fuse_log (level, " file=%s func=%s line=%d ", __FILE__, __func__, __LINE__); 	\
		fuse_log (level, fmt, ##__VA_ARGS__);	\
	} while (0)

#else

#define CURVEFS_FUSE_PRINTF(level, fmt, ...) 	\
	do { } while (0)

#endif

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

#if 0
    CURVEFS_FUSE_PRINTF(FUSE_LOG_ERR, "before mount fd=%d\n", ((struct lo_data *) fuse_req_userdata(__curvefs_req))->root.fd);
#endif

    fd = open("test2.txt", O_CREAT|O_WRONLY|O_SYNC|O_TRUNC, 0644);
    close(fd);

    gettimeofday(&start, NULL);

	int rc;
	fd = open(fpath, O_CREAT|O_WRONLY|O_SYNC|O_TRUNC, 0644);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");
	int offset = 0;

#if 0
    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open  cost %lf seconds\n", fd, time);
      
    gettimeofday(&start, NULL);
#endif

    err = ftruncate(fd, tot_size);
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

    close(fd);

    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open truncate close cost %lf seconds\n", fd, time);


	fd = open(fpath, O_WRONLY);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");

	gettimeofday(&start, NULL);
	for(i = 0; i < num; i++) {
		rc = pwrite(fd, data, size, offset);
		offset += size;
	}

	err = fsync(fd);
	gettimeofday(&end, NULL);	
    
    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs %d times write %d bytes %lf seconds\n", i, rc, time);
	
	close(fd);


    err = stat(fpath, &statbuf);
    if (err < 0) {
        fprintf(stderr, "stat %s error: %d\n", fpath, errno);
    }

    printf("stat file size: %zu\n", statbuf.st_size);

	fd = open(fpath, O_RDONLY);
	if (fd < 0)
		fprintf(stderr, "curvefs read open failed\n");	
	else printf("curvefs read open correct\n");

	
	offset = 0;
	gettimeofday(&start, NULL);
	for (i = 0; i < num; i++){
		err = pread(fd, read_data, size, offset);	
		offset += size;
	}
	gettimeofday(&end, NULL);	
	time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs %d times read %d bytes %lf seconds\n", i, err, time);

	
	close(fd);

#if 0
	unlink(fpath);
#endif

	if (read_data)
		free(read_data);
	if (data)
		free(data);

    return;
}


int main() {

    curvefs_test();

    return 0;
}