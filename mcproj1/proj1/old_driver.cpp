/*
 * main.cpp
 *
 * Serial version
 *
 * Compile with -O3
 */

#include <limits.h> 
#include <stdbool.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <iostream> 
#include <queue>
#include "skiplist2.h"

int main(int argc, char* argv[])
{
    int count = 0;
    struct timespec start, stop;
    bool printFlag = false;  // -p option: whether to print or not

    skiplist<int, int> list(0, INT_MAX);

    int opt;
    extern char* optarg;
    while ((opt = getopt(argc, argv, "p")) != -1) {
        switch (opt) {
            case 'p':
                printFlag = true;
                break;
            default:
                fprintf(stderr, "Usage: %s [-p] <infile>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "Usage: %s [-p] <infile>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    FILE* fin = fopen(argv[optind], "r");
    if (!fin) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    // count the number of lines and buffer the input file in the page cache.       
    int totalLines = 0;
    char tmp[256];
    while (fgets(tmp, sizeof(tmp), fin)) totalLines++;
    rewind(fin);

    clock_gettime(CLOCK_REALTIME, &start);

    char action;
    long num;
    int lineNo = 0;
    while (fscanf(fin, "%c %ld\n", &action, &num) > 0) {
        lineNo++;

        if (action == 'i') {
            list.insert(num, num);
        } else if (action == 'q') {
            int val;
            if (!list.find(num, val))
                cout << "ERROR: Not Found: " << num << endl;
        } else if (action == 'd') {
            list.erase(num);
        } else {
            printf("ERROR: Unrecognized action: '%c'\n", action);
            exit(EXIT_FAILURE);
        }

        count++;

        // ANSI progress rate     
        if (printFlag && (lineNo % (totalLines / 100) == 0 || lineNo == totalLines)) {
            int percent = (lineNo * 100) / totalLines;
            printf("\r\033[KLine %d / %d, Progress: %d%%", lineNo, totalLines, percent);
            fflush(stdout);
        }
    }
    fclose(fin);
    printf("\n");  

    clock_gettime(CLOCK_REALTIME, &stop);

    cout << "Final skiplist keys: " << list.printList() << endl;

    double elapsed_time = (stop.tv_sec - start.tv_sec) +
                          ((double)(stop.tv_nsec - start.tv_nsec)) / BILLION;

    cout << "Elapsed time: " << elapsed_time << " sec" << endl;
    cout << "Throughput: " << (double) count / elapsed_time << " ops/sec" << endl;

    return EXIT_SUCCESS;
}

