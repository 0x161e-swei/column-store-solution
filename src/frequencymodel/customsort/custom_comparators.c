/*
 * custom_comparators.c
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */
#include "custom_comparators.h"
#include "../data_structures.h"

int cmpfunc_int (const void * a, const void * b)
{
	return ( *(int*)a - *(int*)b );
}

int cmpfunc_aof (const void * a, const void * b)
{
	return ((partition_inst_aos *)a)->pivot - ((partition_inst_aos *)b)->pivot;
}

int cmpfunc_ps(const void *a, const void *b)
{

	const partition_struct* first = (partition_struct*)a;
	const partition_struct* second = (partition_struct*)b;
	if(first->score < second->score) {
		return 1;
	} else if(second->score < first->score) {
		return -1;
	} else {
		return 0;
	}
	return 0;
}

