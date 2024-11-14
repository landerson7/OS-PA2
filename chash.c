// chash.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "hash_table.h"
#include "rwlock.h"
#include "timestamp.h"
#include "output.h"
#include "common.h"

// Define the variables declared as extern in common.h
Command commands[MAX_COMMANDS];
int num_commands = 0;
int num_threads = 0;

pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;

rwlock_t rwlock;

void *insert_thread(void *arg);
void *delete_thread(void *arg);
void *search_thread(void *arg);
void *print_thread(void *arg);

int main() {
    // Initialize the hash table and locks
    hash_table_init();
    rwlock_init(&rwlock);

    // Open commands.txt
    FILE *fp = fopen("commands.txt", "r");
    if (!fp) {
        fprintf(stderr, "Error opening commands.txt\n");
        exit(1);
    }

    // Read the first line to get the number of threads
    char line[256];
    if (fgets(line, sizeof(line), fp) != NULL) {
        char *token = strtok(line, ",");
        if (strcmp(token, "threads") != 0) {
            fprintf(stderr, "First line should be threads,<number>,0\n");
            exit(1);
        }
        token = strtok(NULL, ",");
        num_threads = atoi(token);
    } else {
        fprintf(stderr, "Error reading commands.txt\n");
        exit(1);
    }
    /*fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "Running %d threads\n", num_threads);
        
        fclose(fp);
    }*/
    // Read the rest of the commands
    while (fgets(line, sizeof(line), fp) != NULL) {
        Command cmd;
        char *token = strtok(line, ",");
        strcpy(cmd.command, token);
        token = strtok(NULL, ",");
        if (token) {
            strcpy(cmd.name, token);
            // Remove newline if present
            size_t len = strlen(cmd.name);
            if (len > 0 && cmd.name[len - 1] == '\n') {
                cmd.name[len - 1] = '\0';
            }
        } else {
            cmd.name[0] = '\0';
        }
        token = strtok(NULL, ",");
        if (token) {
            cmd.salary = atoi(token);
        } else {
            cmd.salary = 0;
        }
        commands[num_commands++] = cmd;
    }
    fclose(fp);

    // Clear output.txt
    pthread_mutex_lock(&output_mutex);
    fp = fopen("output.txt", "w");
    if (fp) fclose(fp);
    pthread_mutex_unlock(&output_mutex);

    pthread_t threads[num_commands];
    int thread_ids[num_commands];

    // Log waiting on insert condition variable
    uint64_t timestamp = get_timestamp();
    write_condition_event_to_output(timestamp, "WAITING ON INSERTS");

    // Create and start INSERT threads
    for (int i = 0; i < num_commands; i++) {
        Command *cmd = &commands[i];
        if (strcmp(cmd->command, "insert") == 0) {
            thread_ids[i] = i;
            pthread_create(&threads[i], NULL, insert_thread, (void*)&thread_ids[i]);
        } else {
            threads[i] = 0; // Initialize thread handles for non-insert commands
        }
    }

    // Wait for INSERT threads to finish
    for (int i = 0; i < num_commands; i++) {
        Command *cmd = &commands[i];
        if (strcmp(cmd->command, "insert") == 0) {
            pthread_join(threads[i], NULL);
        }
    }

    // Now create and start DELETE, SEARCH, and PRINT threads
    for (int i = 0; i < num_commands; i++) {
        Command *cmd = &commands[i];
        if (strcmp(cmd->command, "delete") == 0 || strcmp(cmd->command, "search") == 0 || strcmp(cmd->command, "print") == 0) {
            thread_ids[i] = i;
            if (strcmp(cmd->command, "delete") == 0) {
                pthread_create(&threads[i], NULL, delete_thread, (void*)&thread_ids[i]);
            } else if (strcmp(cmd->command, "search") == 0) {
                pthread_create(&threads[i], NULL, search_thread, (void*)&thread_ids[i]);
            } else if (strcmp(cmd->command, "print") == 0) {
                pthread_create(&threads[i], NULL, print_thread, (void*)&thread_ids[i]);
            }
        }
    }

    // Wait for DELETE, SEARCH, and PRINT threads to finish
    for (int i = 0; i < num_commands; i++) {
        Command *cmd = &commands[i];
        if (strcmp(cmd->command, "delete") == 0 || strcmp(cmd->command, "search") == 0 || strcmp(cmd->command, "print") == 0) {
            pthread_join(threads[i], NULL);
        }
    }

    // Acquire read lock to print the contents
    timestamp = get_timestamp();
    write_condition_event_to_output(timestamp, "READ LOCK ACQUIRED");
    rwlock_acquire_readlock(&rwlock);

    // Print the contents of the table
    pthread_mutex_lock(&output_mutex);
    fp = fopen("output.txt", "a");
    if (fp) {
        //hash_table_print(fp);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);

    // Release read lock
    rwlock_release_readlock(&rwlock);
    timestamp = get_timestamp();
    write_condition_event_to_output(timestamp, "READ LOCK RELEASED");

    // After releasing the read lock, print the final summary
    pthread_mutex_lock(&output_mutex);
    fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "Finished all threads.\n");
        fprintf(fp, "Number of lock acquisitions:  %d\n", num_lock_acquisitions);
        fprintf(fp, "Number of lock releases:  %d\n", num_lock_releases);
        
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
    // Acquire read lock to print the contents
    rwlock_acquire_readlock(&rwlock);
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK ACQUIRED");
    // Print the contents of the table
    pthread_mutex_lock(&output_mutex);
    fp = fopen("output.txt", "a");
    if (fp) {
        hash_table_print(fp);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);

    // Release read lock
    rwlock_release_readlock(&rwlock);
    timestamp = get_timestamp();
    write_lock_event_to_output(timestamp, "READ LOCK RELEASED");
    return 0;
}
