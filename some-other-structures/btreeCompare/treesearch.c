#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "treesearch.h"
// #include "b+tree.h"
#include "k_ary_tree.h"
#include "binarySearch.h"

void clock_timediff(struct timespec start, struct timespec end)
{
	struct timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	printf("%ld.%ld\n", temp.tv_sec, temp.tv_nsec);
}

int main(int argc, char const *argv[]) {
	srand((unsigned)time(NULL));
	int search[100000];
	int arr[TESTNUMBER];
	// entry ev[TESTNUMBER];
	int pivot = 0;
	for (uint i = 0; i < TESTNUMBER; i++) {
		int k = rand()  % INTERVAL + 1;
		pivot += k;
		arr[i] = pivot;
		// ev[i].key = pivot;
		// ev[i].value = i;
		// ev[i].ptr.v = NULL;
		// ev[i].type = INT;
	}
	for (uint i = 0; i < 100000; i++) {
		search[i] = rand() % pivot + 1;
	}
	uint x;
	struct timespec tic, toc;
	long sum = 0;
	int c = (argc == 2)? (*argv[1] - '0'): 10;
	switch(c) {
	case 0: {
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);
		BTree *root = bTreeCreate(arr, TESTNUMBER);
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
		clock_timediff(tic, toc);
		bt_node_search(root, 123);
		break;
		}
	/* =====================================================================================
	 * Test of my btreee
	 * ===================================================================================== */
	case 1: {
		sum = 0;
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);

		BTree *root = bTreeCreate(arr, TESTNUMBER);
		for (uint i = 0; i < 1000; i++)
		for (uint q = 0; q < 100000; q++) {
			x = bt_node_search(root, search[q]);
			// uint y = binary_search_pivots(arr, TESTNUMBER, search[q]);
			// if ( x != y) printf("wrong %u and %u when %d\n", x, y, search[q]);
			sum += x;
		}

		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
		clock_timediff(tic, toc);
		fprintf(stderr, "sum %ld\n", sum);
		// int count = 0;
		// printf("tree height: %d\n", root->height);
		// for (int i = 1; i <= root->height; i++){
		// 	printf("level node count: %d\n", root->levelNodeCount[i]);
		// 	for (int j = 0; j < root->levelNodeCount[i] - root->levelNodeCount[i - 1]; j++) {
		// 		printf("internal %d\t", root->internalNode[count]);
		// 		count ++;
		// 	}
		// 	printf("\n");
		// }

		break;
		}
		
	/* =====================================================================================
	 * Test of binary search
	 * ===================================================================================== */
	case 3: {
		sum = 0;
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);
		
		for (uint i = 0; i < 1000; i++)
		for (uint q = 0; q < 100000; q++) {
			x = binary_search_pivots(arr, TESTNUMBER, search[q]);
			sum += x;
		}
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
		clock_timediff(tic, toc);
		fprintf(stderr, "sum %ld\n", sum);
		break;
		}

	/* =====================================================================================
	 * Test of Mike's btree
	 * ===================================================================================== */
//	case 4: {
//		sum = 0;
//		uint y = 0;
//		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);
//		
//		bt_index_node *root = bt_populate_load(ev, TESTNUMBER);
//		for (uint i = 0; i < 1000; i++)
//		for (uint q = 0; q < 100000; q++) {
//			bt_get_gte(root, search[q], &y, 1); // want only one result
//			sum += y;
//		}
//		
//		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
//		clock_timediff(tic, toc);
//		fprintf(stderr, "sum %ld\n", sum);
//		break;
//		}
	default : {
		break;
		}
	}
	return 0;
}
