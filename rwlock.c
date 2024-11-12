// rwlock.c
#include "rwlock.h"
#include "common.h"

pthread_mutex_t lock_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
int num_lock_acquisitions = 0;
int num_lock_releases = 0;

void rwlock_init(rwlock_t *rw) {
    rw->readers = 0;
    sem_init(&rw->lock, 0, 1);
    sem_init(&rw->writelock, 0, 1);
}

void rwlock_acquire_readlock(rwlock_t *rw) {
    sem_wait(&rw->lock);
    rw->readers++;
    if (rw->readers == 1) {
        sem_wait(&rw->writelock);

        // Increment lock acquisitions when first reader acquires write lock
        pthread_mutex_lock(&lock_counter_mutex);
        num_lock_acquisitions++;
        pthread_mutex_unlock(&lock_counter_mutex);
    }
    sem_post(&rw->lock);
}

void rwlock_release_readlock(rwlock_t *rw) {
    sem_wait(&rw->lock);
    rw->readers--;
    if (rw->readers == 0) {
        sem_post(&rw->writelock);

        // Increment lock releases when last reader releases write lock
        pthread_mutex_lock(&lock_counter_mutex);
        num_lock_releases++;
        pthread_mutex_unlock(&lock_counter_mutex);
    }
    sem_post(&rw->lock);
}

void rwlock_acquire_writelock(rwlock_t *rw) {
    sem_wait(&rw->writelock);

    // Increment lock acquisitions
    pthread_mutex_lock(&lock_counter_mutex);
    num_lock_acquisitions++;
    pthread_mutex_unlock(&lock_counter_mutex);
}

void rwlock_release_writelock(rwlock_t *rw) {
    sem_post(&rw->writelock);

    // Increment lock releases
    pthread_mutex_lock(&lock_counter_mutex);
    num_lock_releases++;
    pthread_mutex_unlock(&lock_counter_mutex);
}
