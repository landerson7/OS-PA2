// hash_table.h
#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stdint.h>
#include <stdio.h>

typedef struct hash_struct
{
  uint32_t hash;
  char name[50];
  uint32_t salary;
  struct hash_struct *next;
} hashRecord;

void hash_table_init();
void hash_table_insert(const char *name, uint32_t salary);
void hash_table_delete(const char *name);
hashRecord *hash_table_search(const char *name);
void hash_table_print(FILE *fp);

#endif
