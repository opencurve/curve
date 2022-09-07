#include <stdio.h>

int main() {
    int alloc_size = 0;
    int obj_size = 1;
    int tmp = obj_size - 1;

    tmp |= tmp >> 1;
    tmp |= tmp >> 2;
    tmp |= tmp >> 4;
    tmp |= tmp >> 8;
    tmp |= tmp >> 16;
    alloc_size = tmp + 1;

    printf("the objsize %d allocsize  %d \n", obj_size, alloc_size);
    return 0;
}
