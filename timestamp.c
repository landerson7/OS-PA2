// timestamp.c
#include <stdint.h>
#include <sys/time.h>
#include <stddef.h> // Include this header for NULL

uint64_t get_timestamp() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t timestamp = tv.tv_sec * (uint64_t)1000000 + tv.tv_usec;
    return timestamp;
}
