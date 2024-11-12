// jenkins_hash.h
#ifndef JENKINS_HASH_H
#define JENKINS_HASH_H

#include <stdint.h>

uint32_t jenkins_one_at_a_time_hash(const char *key);

#endif
