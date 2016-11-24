#include "custom_qsort.h"
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define TESTNUMBER 100000000

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

int cmpfunc (const void * a, const void * b)
{
	return ( *(int*)a - *(int*)b );
}

int main(int argc, char const *argv[])
{
	srand((unsigned)15);
	int *arr1 = malloc(TESTNUMBER * sizeof(int));
	int *arr2 = malloc(TESTNUMBER * sizeof(int));
	for (int i = 0; i < TESTNUMBER; i++) {
		int k = rand();
		arr1[i] = k;
		arr2[i] = k;
	}
	
	struct timespec tic, toc;
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);
	_c_qsort(arr1, TESTNUMBER);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
	clock_timediff(tic, toc);

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tic);
	qsort(arr2, TESTNUMBER, sizeof(int), cmpfunc);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &toc);
	clock_timediff(tic, toc);

	free(arr1);
	free(arr2);
	// for (int i = 0; i < TESTNUMBER; i++) {
	// 	printf("%d %d\n", arr1[i], arr2[i]);
	// }
	return 0;
}
