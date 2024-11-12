README.txt

This is a concurrent hash table implementation in C, designed to handle multiple threads performing
insert, delete, search, and print operations. The program reads commands from a file called
"commands.txt" and outputs results and logs to "output.txt".

To compile and run the program:

1. Unzip the provided files into a directory.
2. Run `make` to compile the program.
3. Ensure that a valid "commands.txt" file is in the same directory.
4. Run the executable with `./chash`.
5. The output will be written to "output.txt".

Files:
- common.h: Common definitions and extern declarations.
- chash.c: Main program.
- hash_table.c/h: Hash table implementation.
- jenkins_hash.c/h: Jenkins's one-at-a-time hash function.
- rwlock.c/h: Reader-writer lock implementation.
- timestamp.c/h: Timestamp utility functions.
- output.c/h: Output handling functions.
- thread_functions.c: Thread function implementations.
- Makefile: Build script.

Note:
- The program ensures that DELETE operations wait until all INSERT operations are completed by starting DELETE threads only after INSERT threads finish.
- All synchronization primitives are implemented using pthreads and semaphores.
- The code uses standard POSIX threads and should compile on most UNIX-like systems.

AI Use:
This code was generated with the assistance of OpenAI's GPT-4 language model, which provided guidance
on structuring the program according to the specifications. The code was carefully reviewed and
adapted to meet the assignment requirements.

Contact:
For any questions or issues, please contact [Your Name] at [Your Email].