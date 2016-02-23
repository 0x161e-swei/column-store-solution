/*
 * data_structures.h
 *
 *  Created on: Jan 21, 2016
 *      Author: kenneth
 */

#ifndef DATA_STRUCTURES_H_
#define DATA_STRUCTURES_H_
#include <stdlib.h>

typedef struct data {
	int* array;
	size_t size;
} data;

typedef struct partition_struct partition_struct;

typedef struct forward_backward{
	int* forward;
	int* backward;

} forward_backward;

struct partition_struct {
	int min_block;
	int max_block;
	int max_val;
	double partition_static_cost;
	double score;
	partition_struct* prev_neighbor;
	partition_struct* next_neighbor;
	partition_struct* next_queue;
	partition_struct* prev_queue;
};

typedef struct workload_api {
	size_t size;
	int* type;
	int* first;
	int* second;
} workload_api;

typedef struct _partition_inst{
	// pivot count
	int p_count;
	int *pivots;
	#ifdef GHOST_VALUE
	int *ghost_count;
	#endif
} Partition_inst;

typedef struct frequency_model {
	int block_size;
	int histogram_size;
	int* rs;
	int* re;
	int* sc;
	int* pq;
	int* de;
	int* in;
	int* ud;
	int* uf;
	int* ub;
} frequency_model;

typedef struct prefix {
	int histogram_size;
	int* de;
	int* in;
} prefix;


#endif /* DATA_STRUCTURES_H_ */
