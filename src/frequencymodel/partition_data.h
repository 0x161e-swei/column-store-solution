/*
 * partition_data.h
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */

#ifndef FREQUENCYMODEL_PARTITION_DATA_H_
#define FREQUENCYMODEL_PARTITION_DATA_H_
#include "data_structures.h"
#include <float.h>
#if __cplusplus
extern "C" {
#endif

static inline void compute_score(partition_struct *ps, const forward_backward *fb, const double sr) {
	if(ps->next_neighbor){
		partition_struct *next = ps->next_neighbor;
		int additional_blocks = (next->max_block-ps->min_block);
		double score = 0.0;
		for(int i = 0, index = ps->min_block; i <= additional_blocks; i++, index++) {
			//compute the cost of removing the partition boundary in terms of additional read penalty
			score += fb->backward[index]*i*sr;
			score += fb->forward[index]*(additional_blocks-i)*sr;
		}
		//the score is the difference between the data movement cost of having the partition boundary,
		//and the read penalty of not having the partition boundary
		ps->score = ps->partition_static_cost-score;
	} else {
		ps->score = -DBL_MAX;
	}
}

double compute_costs(forward_backward* fb, partition_struct* partition_cost, const frequency_model *fm,
		const prefix *prefix_sum, const double rr, const double rw, const double sr);

static inline partition_struct* insert_queue(partition_struct* head, partition_struct* el){
	partition_struct* pos = head;
	if(pos->score < el->score) {
		//el is first in line
		el->prev_queue = NULL;
		el->next_queue = pos;
		pos->prev_queue = el;
		return el;
	} else {
		while(pos->next_queue && pos->score >= el->score) {
			//printf("pos score: %f\n",pos->score);
			partition_struct* temp = pos->next_queue;
			pos = temp;
		}
		if(pos->score < el->score){
			partition_struct* temp = pos->prev_queue;
			pos = temp;
		}
		//printf("pos final: %f\t el score:%f\n",pos->score,el->score);
		//el have a larger score than pos, so we insert it immediately before pos.
		el->prev_queue = pos;
		if(pos->next_queue){
			el->next_queue = pos->next_queue;
			el->next_queue->prev_queue = el;
		} else {
			el->next_queue = NULL;
		}
		pos->next_queue = el;
	}
	return head;
}

static inline void delete_queue(partition_struct* next){
	if(next->next_queue) {
		if(next->prev_queue) {
			next->prev_queue->next_queue = next->next_queue;
			next->next_queue->prev_queue = next->prev_queue;
		} else {
			//next is first in the queue
			next->next_queue->prev_queue = NULL;
		}
	} else if(next->prev_queue) {
		//next is last in the queue
		next->prev_queue->next_queue = NULL;
	}
}


static inline void prefix_sum(const int* input, int* output,const int size){
	output[0] = input[0];
	for(int i = 1; i < size; i++) {
		output[i] = input[i]+output[i-1];
	}
}

static inline void initialize_prefix_sum(prefix *prefix_sum,size_t size) {
	prefix_sum->de = (int *) calloc(size, sizeof(int));
	prefix_sum->in = (int *) calloc(size, sizeof(int));
	prefix_sum->histogram_size = size;
}


static inline void compute_prefix_sum(const frequency_model *fm, prefix *prefix_sum_model) {
	//prefix sum for all histograms
	//TODO: Once all algorithms are in place, check if some of these are not needed and if so remove them
	initialize_prefix_sum(prefix_sum_model,fm->histogram_size);
	prefix_sum(fm->de,prefix_sum_model->de,fm->histogram_size);
	prefix_sum(fm->in,prefix_sum_model->in,fm->histogram_size);

}

static inline void setup_partition_structure(const int block_counter,const int max_val,const int histogram_size, partition_struct *ps){

	ps[block_counter].max_val = max_val;
	ps[block_counter].max_block = block_counter;
	ps[block_counter].min_block = block_counter;
	ps[block_counter].score = 0.0;
	ps[block_counter].partition_static_cost = 0.0;
#ifdef GHOST_VALUE
	ps[block_counter].next_partitions = 0.0;
	ps[block_counter].ghost_values = 0;
	ps[block_counter].inserts = 0;

#endif

	if(block_counter+1 < histogram_size){
		ps[block_counter].next_neighbor = &ps[block_counter+1];
	} else {
		ps[block_counter].next_neighbor = NULL;
	}

	if(block_counter != 0) {
		ps[block_counter].prev_neighbor = &ps[block_counter-1];
	} else {
		ps[block_counter].prev_neighbor = NULL;
	}

}

#ifdef GHOST_VALUE
void partition_data_gv(frequency_model *fm, const int data_size, const int algo, const int ghost_values, Partition_inst *out);
#endif
void partition_data(frequency_model* fm,const int algo, Partition_inst *out, size_t data_size);
#if __cplusplus

void setup_partitions(frequency_model *fm, partition_struct *ps);


void free_prefix_sum(prefix *prefix_sum);

partition_struct* compute_partitioning_bottom_up(const forward_backward *fb, partition_struct* partition_cost,
		const frequency_model *fm, const double sr, size_t data_size);
partition_struct* compute_partitioning_bottom_up_gv(const forward_backward *fb, partition_struct* partition_cost,
		const frequency_model *fm, const double sr, const double rr, const double rw, const int gv, const int data_size);

}
#endif
#endif /* FREQUENCYMODEL_PARTITION_DATA_H_ */
