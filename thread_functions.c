// thread_functions.c
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include "hash_table.h"
#include "rwlock.h"
#include "timestamp.h"
#include "output.h"
#include "common.h"

// Condition variables and mutex
pthread_cond_t insert_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t delete_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t cv_mutex = PTHREAD_MUTEX_INITIALIZER;

// State counters for tracking active inserts and deletes
int active_inserts = 0;
int active_deletes = 0;

void *insert_thread(void *arg) {
    int thread_id = *(int*)arg;
    Command *cmd = &commands[thread_id];
    uint64_t timestamp;

    // Wait if there are other active insert threads
    pthread_mutex_lock(&cv_mutex);
    while (active_inserts > 0) {
        // Log waiting on insert condition variable
        timestamp = get_timestamp();
        write_condition_event_to_output(timestamp, "WAITING ON INSERTS");
        pthread_cond_wait(&insert_cv, &cv_mutex);
    }
    active_inserts++;
    pthread_mutex_unlock(&cv_mutex);

    // Log after being awakened
    timestamp = get_timestamp();
    write_condition_event_to_output(timestamp, "INSERT AWAKENED");

    // Acquire write lock
    rwlock_acquire_writelock(&rwlock);

    // Log lock acquisition
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "WRITE LOCK ACQUIRED");

    // Perform insert operation
    hash_table_insert(cmd->name, cmd->salary);

    // Release write lock
    rwlock_release_writelock(&rwlock);

    // Log lock release
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "WRITE LOCK RELEASED");

    // Update and signal other waiting insert threads
    pthread_mutex_lock(&cv_mutex);
    active_inserts--;
    pthread_cond_signal(&insert_cv);
    pthread_mutex_unlock(&cv_mutex);

    // Log signaling insert
    timestamp = get_timestamp();
    // write_condition_event_to_output(timestamp, "INSERT SIGNAL SENT");

    return NULL;
}

void *delete_thread(void *arg) {
    int thread_id = *(int*)arg;
    Command *cmd = &commands[thread_id];
    uint64_t timestamp;

    // Wait if there are other active delete threads
    pthread_mutex_lock(&cv_mutex);
    while (active_deletes > 0) {
        // Log waiting on delete condition variable
        timestamp = get_timestamp();
        write_condition_event_to_output(timestamp, "WAITING ON DELETES");
        pthread_cond_wait(&delete_cv, &cv_mutex);
    }
    active_deletes++;
    pthread_mutex_unlock(&cv_mutex);

    // Log after being awakened
    timestamp = get_timestamp();
    write_condition_event_to_output(timestamp, "DELETE AWAKENED");

    // Acquire write lock
    rwlock_acquire_writelock(&rwlock);

    // Log lock acquisition
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "WRITE LOCK ACQUIRED");

    // Perform delete operation
    hash_table_delete(cmd->name);

    // Release write lock
    rwlock_release_writelock(&rwlock);

    // Log lock release
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "WRITE LOCK RELEASED");

    // Update and signal other waiting delete threads
    pthread_mutex_lock(&cv_mutex);
    active_deletes--;
    pthread_cond_signal(&delete_cv);
    pthread_mutex_unlock(&cv_mutex);

    // Log signaling delete
    timestamp = get_timestamp();
    // write_condition_event_to_output(timestamp, "DELETE SIGNAL SENT");

    return NULL;
}

void *search_thread(void *arg) {
    int thread_id = *(int*)arg;
    Command *cmd = &commands[thread_id];
    uint64_t timestamp = get_timestamp();

    // Write command to output.txt
    write_command_to_output(timestamp, "SEARCH", cmd->name, 0);

    // Acquire read lock
    rwlock_acquire_readlock(&rwlock);

    // Log lock acquisition
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK ACQUIRED");

    // Perform search operation
    hashRecord *record = hash_table_search(cmd->name);

    // Release read lock
    rwlock_release_readlock(&rwlock);

    // Log lock release
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK RELEASED");

    // Write search result
    if (record != NULL) {
        write_search_result(timestamp, record->name, record->salary);
    } else {
        write_no_record_found(timestamp);
    }

    return NULL;
}

void *print_thread(void *arg) {
    uint64_t timestamp = get_timestamp();

    // Write command to output.txt
    write_command_to_output(timestamp, "PRINT", "", 0);

    // Acquire read lock
    rwlock_acquire_readlock(&rwlock);

    // Log lock acquisition
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK ACQUIRED");

    // Perform print operation
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        hash_table_print(fp);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);

    // Release read lock
    rwlock_release_readlock(&rwlock);

    // Log lock release
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK RELEASED");

    return NULL;
}
