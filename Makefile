# Makefile
CC = gcc
CFLAGS = -pthread -Wall -g

OBJS = chash.o hash_table.o jenkins_hash.o rwlock.o timestamp.o output.o thread_functions.o

all: chash

chash: $(OBJS)
	$(CC) $(CFLAGS) -o chash $(OBJS)

chash.o: chash.c hash_table.h rwlock.h output.h timestamp.h common.h
	$(CC) $(CFLAGS) -c chash.c

hash_table.o: hash_table.c hash_table.h jenkins_hash.h
	$(CC) $(CFLAGS) -c hash_table.c

jenkins_hash.o: jenkins_hash.c jenkins_hash.h
	$(CC) $(CFLAGS) -c jenkins_hash.c

rwlock.o: rwlock.c rwlock.h common.h
	$(CC) $(CFLAGS) -c rwlock.c

timestamp.o: timestamp.c timestamp.h
	$(CC) $(CFLAGS) -c timestamp.c

output.o: output.c output.h common.h
	$(CC) $(CFLAGS) -c output.c

thread_functions.o: thread_functions.c common.h hash_table.h rwlock.h output.h timestamp.h
	$(CC) $(CFLAGS) -c thread_functions.c

clean:
	rm -f *.o chash
