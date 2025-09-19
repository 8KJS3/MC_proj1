#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#define SPARSENESS 5

int main(int argc, char** argv)
{
    int i, r;
    int opt;
    int opt_cnt = 0;
    int nqueries = 10;
    int ins_proportion = 40;    // insert proportion
    int del_proportion = 20;    // delete proportion
    int search_proportion = 40; // will adjust
    int hit_rate = 50;          // search hit 비율
    extern char* optarg;

    while ((opt = getopt(argc, argv, "n:h:i:d:")) != -1) {
        switch (opt) {
            case 'n': nqueries = atoi(optarg); break;
            case 'h': hit_rate = atoi(optarg); break;
            case 'i': ins_proportion = atoi(optarg); break;
            case 'd': del_proportion = atoi(optarg); break;
            default:
                fprintf(stderr,
                    "Usage: %s -n {queries} -h {hit rate} -i {insert %%} -d {delete %%}\n"
                    "Search proportion = 100 - insert - delete\n", argv[0]);
                exit(0);
        }
        opt_cnt++;
    }

    if (opt_cnt < 2 || nqueries < 10) {
        fprintf(stderr,
            "Usage: %s -n {queries (>10)} -h {hit rate} -i {insert %%} -d {delete %%}\n", argv[0]);
        exit(0);
    }

    if (ins_proportion + del_proportion > 100) {
        fprintf(stderr, "Error: insert%% + delete%% must be <= 100\n");
        exit(1);
    }

    search_proportion = 100 - ins_proportion - del_proportion;

    srand(time(NULL));

    // load phase: 짝수 key 미리 insert
    int preload_size = nqueries;  // 원하는 preload 양 조정 가능
    for (i = 0; i < preload_size; i++) {
        int evenKey = (i * 2);
        printf("i %d\n", evenKey);
    }

    // workload phase
    for (i = 0; i < nqueries; i++) {
        int choice = rand() % 100;

        if (choice < ins_proportion) {
            // insert: 홀수 key만
            r = (rand() % (nqueries * SPARSENESS)) * 2 + 1; // 홀수 생성
            printf("i %d\n", r);
        }
        else if (choice < ins_proportion + del_proportion) {
            // delete: 짝수 key만
            r = (rand() % (nqueries * SPARSENESS)) * 2; // 짝수 생성
            printf("d %d\n", r);
        }
        else {
            // search: 짝수 key만
            int doHit = (rand() % 100 < hit_rate);
            if (doHit) {
                r = (rand() % (preload_size)) * 2; // 이미 load된 짝수 key
                printf("q %d\n", r);
            } else {
                // miss search: 아직 없는 짝수 key
                r = ((rand() % (nqueries * SPARSENESS)) * 2) + 2 * preload_size;
                printf("q %d\n", r);
            }
        }
    }

    return 0;
}

