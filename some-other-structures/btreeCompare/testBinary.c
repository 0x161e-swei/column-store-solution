#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "binarySearch.h"

typedef unsigned int uint;

int main(int argc, char const *argv[]) {
	srand((unsigned)time(NULL));
	int search[100000];
	int arr[TESTNUMBER];
	int pivot = 0;
	for (uint i = 0; i < TESTNUMBER; i++) {
		int k = rand()  % INTERVAL + 1;
		pivot += k;
		arr[i] = pivot;
	}
	for (uint i = 0; i < 100000; i++) {
		search[i] = rand() % pivot + 1;
	}
	uint x;
	clock_t beg = 0, end = 0;
	unsigned long sum = 0;
	beg = clock();
	for (uint i = 0; i < 1000; i++)
	for (uint q = 0; q < 100000; q++) {
		x = binary_search_pivots(arr, TESTNUMBER, search[q]);
		//sum += x;
	}
	end = clock();
	printf("%lf", (double)(end - beg) / CLOCKS_PER_SEC);
	fprintf(stderr, "sum %ld\n", sum);
	return 0;
}
