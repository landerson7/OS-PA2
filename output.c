// output.c
#include <stdio.h>
#include <inttypes.h> // For PRIu64
#include "output.h"
#include "common.h"

void write_command_to_output(uint64_t timestamp, const char *command, const char *name, uint32_t salary) {
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "%" PRIu64 ",%s,%s,%u\n", timestamp, command, name, salary);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
}

void write_lock_event_to_output(uint64_t timestamp, const char *event) {
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "%" PRIu64 ",%s\n", timestamp, event);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
}

void write_condition_event_to_output(uint64_t timestamp, const char *event) {
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "%" PRIu64 ": %s\n", timestamp, event);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
}

void write_search_result(uint64_t timestamp, const char *name, uint32_t salary) {
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "%" PRIu64 ",FOUND,%s,%u\n", timestamp, name, salary);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
}

void write_no_record_found(uint64_t timestamp) {
    pthread_mutex_lock(&output_mutex);
    FILE *fp = fopen("output.txt", "a");
    if (fp) {
        fprintf(fp, "%" PRIu64 ",No Record Found\n", timestamp);
        fclose(fp);
    }
    pthread_mutex_unlock(&output_mutex);
}
