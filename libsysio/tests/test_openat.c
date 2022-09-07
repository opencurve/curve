#define _GNU_SOURCE 

#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/time.h>
#include <sys/stat.h>

static int init_buffer(char* buf, size_t size, int ckpt) {
    size_t i;
    for(i=0; i < size; i++) {
        char c = 'a' + (char)((ckpt + i) & 32);
        buf[i] = c;
    }
    return 0;
}

int main() {
    int err;
	int size = 4096;
    long tot_size = 32 * 4096;
	int num = tot_size/size;
    int i;

	char *data = NULL;
	char *read_data = NULL;
	int fd = 0;
    const char *fpath = "/home/dxiang/curvefs_mount/test1.txt";
    struct timeval start, end;
    double time;
    struct stat statbuf;

    printf("total data size: %d, transfer size: %ld\n", size, tot_size);
	data = (char *) malloc(sizeof(char) * size);
	init_buffer(data, size, 0);

	read_data = (char *) malloc(sizeof(char) * size);
	memset(read_data, 0, size);

    gettimeofday(&start, NULL);

	int rc;
	fd = open(fpath, O_CREAT|O_WRONLY|O_SYNC|O_TRUNC, 0644);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");
	int offset = 0;
  
    //printf("file %s has created, fd: %d\n", fpath, fd);

    err = ftruncate(fd, tot_size);
    if (err < 0) {
        fprintf(stderr, "ftrunate failed\n");
        return -1;
    } else {
        //fprintf(stderr, "truncated file to %u\n", size);
    }

    close(fd);

    gettimeofday(&end, NULL);

    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	printf("curvefs fd=%d open truncate close cost %lf seconds\n", fd, time);

	fd = open(fpath, O_WRONLY);
	if (fd < 0)
		fprintf(stderr, "curvefs open failed\n");

	gettimeofday(&start, NULL);
    printf("curvefs come to pwrite\n");
	for(i = 0; i < num; i++) {
		rc = pwrite(fd, data, size, offset);
		offset += size;
	}

    printf("curvefs come to fsync\n");
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
	else 
        printf("curvefs read open correct\n");

	
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
	SYSIO_INTERFACE_NAME(unlink)(fpath);
#endif

#if 0
	SYSIO_INTERFACE_NAME(umount)("curvefs:/b_fuse");
#endif

	if (read_data)
		free(read_data);
	if (data)
		free(data);
    
    return 0;
}