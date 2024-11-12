// jenkins_hash.c
#include <string.h>
#include "jenkins_hash.h"

uint32_t jenkins_one_at_a_time_hash(const char *key) {
    uint32_t hash = 0;
    size_t i = 0;
    while (i != strlen(key)) {
        hash += key[i++];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}
