// common.h
#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>
#include <pthread.h>
#include "rwlock.h" // Include to get rwlock_t

typedef struct {
    char command[10];
    char name[50];
    uint32_t salary;
} Command;

#define MAX_COMMANDS 1000

extern Command commands[MAX_COMMANDS];
extern int num_commands;
extern int num_threads;

extern pthread_mutex_t output_mutex;

extern int num_lock_acquisitions;
extern int num_lock_releases;
extern pthread_mutex_t lock_counter_mutex;

extern rwlock_t rwlock;

#endif // COMMON_H
