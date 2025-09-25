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
#include "skiplist.h"

skiplist<int, int> list(0, INT_MAX);
vector<vector<long>> not_found;

int thread_sz = 1;

void *thread_work(void* arg)
{
    int worker_id = *static_cast<int*>(arg);
    int num;
    char action;

    struct Work curr_work;
    while(!WorkQueue[worker_id].empty())
    {
	curr_work = WorkQueue[worker_id].front();
	WorkQueue[worker_id].pop();
	action = curr_work.action;
	num = curr_work.key;

	if ( action == 'i' ) {
	    //printf("i%d\n",num);
	    list.insert(num, num);
	} else if ( action == 'q' ) {
	    int val;
	    //printf("q%d\n",num);
	    if (!list.find(num, val))
		not_found[worker_id].push_back(num);
	} else if ( action == 'd' ) {
	    //printf("d%d\n",num);
	    list.erase(num);
	}
    }

    pthread_exit(NULL);
}

int main(int argc, char* argv[])
{
    int count = 0;
    struct timespec start, stop;
    bool printFlag = false;  // -p option: whether to print or not

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

    if (optind+1 >= argc) {
        fprintf(stderr, "Usage: %s [-p] <infile>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    FILE* fin = fopen(argv[optind], "r");
    thread_sz = atoi(argv[optind+1]);
    if (!fin) {
        perror("fopen");
        exit(EXIT_FAILURE);
    } else if (thread_sz <= 0){
	fprintf(stderr, "num_threads must be > 0 \n");
	exit(EXIT_FAILURE);
    }

    // make the work queue & trash queue that each thread will use
    WorkQueue.resize(thread_sz);
    list.TrashSet();
    not_found.resize(thread_sz);

    // count the number of lines and buffer the input file in the page cache.       
    int totalLines = 0;
    char tmp[256];
    while (fgets(tmp, sizeof(tmp), fin)) totalLines++;
    rewind(fin);

    clock_gettime(CLOCK_REALTIME, &start);

    char action;
    long num;
    int lineNo = 0;
    //1-phase : Distribute the query to each worker queue
    while (fscanf(fin, "%c %ld\n", &action, &num) > 0) {
        lineNo++;
	//hashing the number & push into queue
	long h = num % thread_sz;
	if (h < 0) h += thread_sz;
        if (action == 'i' || action == 'q' || action == 'd') {
            WorkQueue[h].push({num,action});
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

    //2-phase : Create pthread & Process the queries
    pthread_t* threads = new pthread_t[thread_sz];
    int* tids = new int[thread_sz];

    for (int i = 0; i < thread_sz; i++){
	tids[i] = i;
	if ( pthread_create(&threads[i], nullptr, thread_work, (void*)&tids[i]) != 0 ){
	    perror("pthread_create");
	    exit(EXIT_FAILURE);
	}
    }

    for (int i = 0; i < thread_sz; i++){
	if ( pthread_join(threads[i], nullptr) != 0 ){
	    perror("pthread_join");
	    exit(EXIT_FAILURE);
	}
    }

    list.TrashEmpty();

    delete[] threads;
    delete[] tids;

    for ( int i = 0; i < thread_sz; i++ ){
	for ( long k : not_found[i] ){
	    printf("ERROR: Not Found: %ld\n", k);
	}
    }

    printf("\n");  

    clock_gettime(CLOCK_REALTIME, &stop);

    cout << "Final skiplist keys: " << list.printList() << endl;

    double elapsed_time = (stop.tv_sec - start.tv_sec) +
                          ((double)(stop.tv_nsec - start.tv_nsec)) / BILLION;

    cout << "Elapsed time: " << elapsed_time << " sec" << endl;
    cout << "Throughput: " << (double) count / elapsed_time << " ops/sec" << endl;

    return EXIT_SUCCESS;
}

