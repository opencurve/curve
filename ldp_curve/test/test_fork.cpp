
// C program to demonstrate use of fork() and pipe()
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

/*
    test failed:
    because that if  iox_runtime_init run before fork, the child process can not 
    init a new runtime of iox_runtime, it still use the parent runtime, so failed, 
    need to fixed the iox_runtime_init. add a new api like iox_runtime_destroy / iox_runtime_new ?
    
*/
int main()
{
    // We use two pipes
    // First pipe to send input string from parent
    // Second pipe to send concatenated string from child
 
    int fd1[2]; // Used to store two ends of first pipe
    int fd2[2]; // Used to store two ends of second pipe
    int rv;
 
    char fixed_str[] = "forgeeks.org";
    char input_str[100];
    pid_t p;
 
    if (pipe(fd1) == -1) {
        fprintf(stderr, "Pipe Failed");
        return 1;
    }
    if (pipe(fd2) == -1) {
        fprintf(stderr, "Pipe Failed");
        return 1;
    }
 
    rv = scanf("%s", input_str);
    rv = truncate("sample1.txt", 4096);
    printf("after the truncate\n");
    p = fork();
 
    if (p < 0) {
        fprintf(stderr, "fork Failed");
        return 1;
    }
 
    // Parent process
    else if (p > 0) {
        printf("in parent process after the truncate\n");
        int fd3 = open("sample1.txt", O_RDONLY);
        close(fd3);

        char concat_str[100];
 
        close(fd1[0]); // Close reading end of first pipe
 
        // Write input string and close writing end of first
        // pipe.
        rv = write(fd1[1], input_str, strlen(input_str) + 1);
        close(fd1[1]);
 
        // Wait for child to send a string
        wait(NULL);
 
        close(fd2[1]); // Close writing end of second pipe
 
        // Read string from child, print it and close
        // reading end.
        rv = read(fd2[0], concat_str, 100);
        printf("Concatenated string %s\n", concat_str);
        close(fd2[0]);
    }
 
    // child process
    else {
        printf("in child process after the truncate\n");

        int fd4 = open("sample1.txt", O_RDONLY);
        close(fd4);

        close(fd1[1]); // Close writing end of first pipe
 
        // Read a string using first pipe
        char concat_str[100];
        rv = read(fd1[0], concat_str, 100);
 
        // Concatenate a fixed string with it
        int k = strlen(concat_str);
        int i;
        for (i = 0; i < strlen(fixed_str); i++)
            concat_str[k++] = fixed_str[i];
 
        concat_str[k] = '\0'; // string ends with '\0'
 
        // Close both reading ends
        close(fd1[0]);
        close(fd2[0]);
 
        // Write concatenated string and close writing end
        rv = write(fd2[1], concat_str, strlen(concat_str) + 1);
        close(fd2[1]);
 
        exit(0);
    }
}