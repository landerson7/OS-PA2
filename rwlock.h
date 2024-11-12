// rwlock.h
#ifndef RWLOCK_H
#define RWLOCK_H

#include <pthread.h>
#include <semaphore.h>

typedef struct __rwlock_t {
    sem_t lock;          // binary semaphore (basic lock)
    sem_t writelock;     // allow one writer or many readers
    int readers;         // number of readers
} rwlock_t;

void rwlock_init(rwlock_t *rw);
void rwlock_acquire_readlock(rwlock_t *rw);
void rwlock_release_readlock(rwlock_t *rw);
void rwlock_acquire_writelock(rwlock_t *rw);
void rwlock_release_writelock(rwlock_t *rw);

#endif // RWLOCK_H
