// hash_table.c
#include <stdlib.h>
#include <string.h>
#include "hash_table.h"
#include "jenkins_hash.h"

hashRecord *hash_table = NULL; // head of the linked list

void hash_table_init() {
    hash_table = NULL;
}

void hash_table_insert(const char *name, uint32_t salary) {
    uint32_t hash = jenkins_one_at_a_time_hash(name);

    // Search for existing node
    hashRecord *current = hash_table;
    hashRecord *prev = NULL;
    while (current != NULL) {
        if (current->hash == hash) {
            // Update value
            current->salary = salary;
            strcpy(current->name, name);
            return;
        }
        prev = current;
        current = current->next;
    }

    // Not found, insert new node
    hashRecord *new_node = (hashRecord *)malloc(sizeof(hashRecord));
    new_node->hash = hash;
    strcpy(new_node->name, name);
    new_node->salary = salary;
    new_node->next = NULL;

    if (prev == NULL) {
        // List is empty
        hash_table = new_node;
    } else {
        prev->next = new_node;
    }
}

void hash_table_delete(const char *name) {
    uint32_t hash = jenkins_one_at_a_time_hash(name);

    hashRecord *current = hash_table;
    hashRecord *prev = NULL;
    while (current != NULL) {
        if (current->hash == hash) {
            // Found, delete node
            if (prev == NULL) {
                // Deleting head
                hash_table = current->next;
            } else {
                prev->next = current->next;
            }
            free(current);
            return;
        }
        prev = current;
        current = current->next;
    }
    // Not found, do nothing
}

hashRecord *hash_table_search(const char *name) {
    uint32_t hash = jenkins_one_at_a_time_hash(name);

    hashRecord *current = hash_table;
    while (current != NULL) {
        if (current->hash == hash) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}

int compare_hash_records(const void *a, const void *b) {
    hashRecord *rec1 = *(hashRecord**)a;
    hashRecord *rec2 = *(hashRecord**)b;
    if (rec1->hash < rec2->hash)
        return -1;
    else if (rec1->hash > rec2->hash)
        return 1;
    else
        return 0;
}

void hash_table_print(FILE *fp) {
    // Collect the records into an array, then sort by hash value
    int count = 0;
    hashRecord *current = hash_table;
    while (current != NULL) {
        count++;
        current = current->next;
    }

    if (count == 0) return;

    hashRecord **records = malloc(count * sizeof(hashRecord*));
    current = hash_table;
    int index = 0;
    while (current != NULL) {
        records[index++] = current;
        current = current->next;
    }

    // Sort the array by hash value
    qsort(records, count, sizeof(hashRecord*), compare_hash_records);

    // Print the records
    for (int i = 0; i < count; i++) {
        hashRecord *rec = records[i];
        fprintf(fp, "%u,%s,%u\n", rec->hash, rec->name, rec->salary);
    }

    free(records);
}
