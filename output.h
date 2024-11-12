// output.h
#ifndef OUTPUT_H
#define OUTPUT_H

#include <stdint.h>

void write_command_to_output(uint64_t timestamp, const char *command, const char *name, uint32_t salary);
void write_lock_event_to_output(uint64_t timestamp, const char *event);
void write_condition_event_to_output(uint64_t timestamp, const char *event);
void write_search_result(uint64_t timestamp, const char *name, uint32_t salary);
void write_no_record_found(uint64_t timestamp);

#endif
